"""生命周期台账的本地 SQLite 持久化实现。

设计要点：
- 只依赖标准库 ``sqlite3``，旧环境（Python 3.11/3.12）无需新增构建依赖；
- 生命周期策略以“策略编号”为业务主键，表名沿用策略编号口径，不另起炉灶；
- 迁移在 ``BEGIN IMMEDIATE`` 单事务内完成，重复执行按唯一键跳过，不产生副本；
- 运行期动作与存量回填并发时，回填用 ``ON CONFLICT DO NOTHING``，动作更新用
  ``revision`` 乐观锁，先成功落库的一方作为最终结论；
- 迁移前用 SQLite 在线备份 API 生成快照，失败时事务回滚并返回快照路径。
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

from app.persistence.config import (
    AUDIT_STATUS_ORDER,
    LIFECYCLE_TABLE,
    RECYCLE_STATUS_ORDER,
    RECYCLE_TABLE,
    AUDIT_TABLE,
    STATUS_ORDER,
    PersistenceSettings,
)

SCHEMA_VERSION = 1

LIFECYCLE_FIELDS = [
    "策略编号",
    "表名称",
    "创建日期",
    "保留天数",
    "归档路径",
    "清理方式",
    "最后操作",
    "策略状态",
]
RECYCLE_FIELDS = [
    "记录编号",
    "表名称",
    "删除时间",
    "回收原因",
    "数据大小",
    "可恢复期",
    "恢复状态",
    "清理时间",
]
AUDIT_FIELDS = [
    "记录编号",
    "变更对象",
    "变更类型",
    "变更前值",
    "变更后值",
    "操作人",
    "操作时间",
    "复核状态",
]

# 迁移时允许补齐的可选字段。已有值绝不覆盖，缺失值登记到 backfilled_fields。
OPTIONAL_LIFECYCLE_FIELDS = ("保留天数", "归档路径", "清理方式", "最后操作")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _split_optional(values: dict[str, Any], required: Sequence[str], optional: Sequence[str]) -> tuple[dict[str, Any], list[str]]:
    """把旧记录拆成“已有字段”和“缺失可选字段”，供回填与审计使用。"""
    present: dict[str, Any] = {}
    backed: list[str] = []
    for name in required:
        present[name] = values.get(name)
    for name in optional:
        value = values.get(name)
        if value is None or (isinstance(value, str) and not value.strip()):
            backed.append(name)
            present[name] = None
        else:
            present[name] = value
    return present, backed


def _json_dump(values: Any) -> str:
    return json.dumps(values, ensure_ascii=False, sort_keys=True, default=str)


def _json_load(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return fallback


@dataclass
class MigrationReport:
    run_id: str
    backup_path: str | None
    db_path: str
    inserted: dict[str, int] = field(default_factory=dict)
    skipped: dict[str, int] = field(default_factory=dict)
    audit_diffs: int = 0
    recycle_snapshot: str | None = None
    policy_numbers: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "run_id": self.run_id,
            "db_path": self.db_path,
            "backup_path": self.backup_path,
            "inserted": dict(self.inserted),
            "skipped": dict(self.skipped),
            "audit_diffs": self.audit_diffs,
            "recycle_snapshot": self.recycle_snapshot,
            "policy_numbers": list(self.policy_numbers),
        }


class MigrationError(RuntimeError):
    """迁移失败：保留原库并给出可恢复位置。"""

    def __init__(self, message: str, *, db_path: str, backup_path: str | None) -> None:
        self.db_path = db_path
        self.backup_path = backup_path
        self.recoverable_location = backup_path or db_path
        super().__init__(
            f"{message}；原数据未被覆盖，可从 {self.recoverable_location} 恢复"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "message": str(self),
            "db_path": self.db_path,
            "backup_path": self.backup_path,
            "recoverable_location": self.recoverable_location,
        }


class _DuplicateRecord(ValueError):
    pass


class LifecycleDatabase:
    """生命周期策略、回收记录与变更审计的 SQLite 数据访问层。"""

    def __init__(
        self,
        db_path: str | os.PathLike[str] | None = None,
        backup_dir: str | os.PathLike[str] | None = None,
        *,
        legacy_seed_path: str | os.PathLike[str] | None = None,
        settings: PersistenceSettings | None = None,
    ) -> None:
        cfg = settings or PersistenceSettings.from_env()
        self.db_path = str(db_path or cfg.db_path)
        self.backup_dir = str(backup_dir or cfg.backup_dir)
        self.legacy_seed_path = str(legacy_seed_path or cfg.legacy_seed_path or "")
        self._schema_ready = False
        self._schema_lock = threading.Lock()

    # ------------------------------------------------------------------ 连接
    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=FULL")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_tx(self) -> Iterator[sqlite3.Connection]:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with self._schema_lock:
            if self._schema_ready:
                return
            with self._connect() as conn:
                conn.executescript(
                    f"""
                    CREATE TABLE IF NOT EXISTS life_schema_migrations (
                        version INTEGER PRIMARY KEY,
                        applied_at TEXT NOT NULL,
                        note TEXT NOT NULL DEFAULT ''
                    );

                    CREATE TABLE IF NOT EXISTS {LIFECYCLE_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        policy_no TEXT NOT NULL,
                        table_name TEXT,
                        created_on TEXT,
                        retention_days TEXT,
                        archive_path TEXT,
                        cleanup_method TEXT,
                        last_action TEXT,
                        policy_status TEXT,
                        status TEXT NOT NULL,
                        pending INTEGER NOT NULL DEFAULT 1,
                        abnormal INTEGER NOT NULL DEFAULT 0,
                        source TEXT NOT NULL DEFAULT 'seed',
                        backfilled_fields TEXT NOT NULL DEFAULT '[]',
                        extra_json TEXT NOT NULL DEFAULT '{{}}',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_lifecycle_policy_no
                        ON {LIFECYCLE_TABLE}(policy_no);

                    CREATE TABLE IF NOT EXISTS {RECYCLE_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        record_no TEXT NOT NULL,
                        table_name TEXT,
                        deleted_at TEXT,
                        reason TEXT,
                        data_size TEXT,
                        recoverable_until TEXT,
                        restore_status TEXT,
                        cleaned_at TEXT,
                        status TEXT NOT NULL,
                        pending INTEGER NOT NULL DEFAULT 1,
                        abnormal INTEGER NOT NULL DEFAULT 0,
                        source TEXT NOT NULL DEFAULT 'seed',
                        backup_path TEXT,
                        linked_policy_no TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_recycle_record_no
                        ON {RECYCLE_TABLE}(record_no);

                    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        record_no TEXT NOT NULL,
                        target TEXT,
                        change_type TEXT,
                        before_value TEXT,
                        after_value TEXT,
                        operator TEXT,
                        changed_at TEXT,
                        review_status TEXT,
                        status TEXT NOT NULL,
                        pending INTEGER NOT NULL DEFAULT 1,
                        abnormal INTEGER NOT NULL DEFAULT 0,
                        source TEXT NOT NULL DEFAULT 'seed',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_audit_record_no
                        ON {AUDIT_TABLE}(record_no);

                    CREATE TABLE IF NOT EXISTS life_migration_run (
                        run_id TEXT PRIMARY KEY,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        status TEXT NOT NULL,
                        backup_path TEXT,
                        detail TEXT NOT NULL DEFAULT '{{}}'
                    );
                    """
                )
                conn.execute(
                    "INSERT OR IGNORE INTO life_schema_migrations(version, applied_at, note) "
                    "VALUES (?, ?, ?)",
                    (SCHEMA_VERSION, utc_now(), "生命周期台账持久化表结构"),
                )
                # WAL 只在建表阶段切换一次；每条连接重复切库会在并发写时抢锁失败。
                conn.execute("PRAGMA journal_mode=WAL")
            self._schema_ready = True

    # -------------------------------------------------------------- 行转换
    def _lifecycle_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "status": row["status"],
            "pending": bool(row["pending"]),
            "abnormal": bool(row["abnormal"]),
            "策略编号": row["policy_no"],
            "表名称": row["table_name"],
            "创建日期": row["created_on"],
            "保留天数": row["retention_days"],
            "归档路径": row["archive_path"],
            "清理方式": row["cleanup_method"],
            "最后操作": row["last_action"],
            "策略状态": row["policy_status"],
            "_revision": int(row["revision"]),
        }

    def _recycle_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "status": row["status"],
            "pending": bool(row["pending"]),
            "abnormal": bool(row["abnormal"]),
            "记录编号": row["record_no"],
            "表名称": row["table_name"],
            "删除时间": row["deleted_at"],
            "回收原因": row["reason"],
            "数据大小": row["data_size"],
            "可恢复期": row["recoverable_until"],
            "恢复状态": row["restore_status"],
            "清理时间": row["cleaned_at"],
            "_revision": int(row["revision"]),
            "备份位置": row["backup_path"],
        }

    def _audit_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "status": row["status"],
            "pending": bool(row["pending"]),
            "abnormal": bool(row["abnormal"]),
            "记录编号": row["record_no"],
            "变更对象": row["target"],
            "变更类型": row["change_type"],
            "变更前值": row["before_value"],
            "变更后值": row["after_value"],
            "操作人": row["operator"],
            "操作时间": row["changed_at"],
            "复核状态": row["review_status"],
            "_revision": int(row["revision"]),
        }

    # -------------------------------------------------------------- 查询接口
    def list_lifecycle(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        self.ensure_schema()
        clauses: list[str] = []
        params: list[Any] = []
        if keyword:
            clauses.append("policy_no LIKE ?")
            params.append(f"%{keyword}%")
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            total = int(conn.execute(f"SELECT COUNT(*) FROM {LIFECYCLE_TABLE} {where}", params).fetchone()[0])
            start = max(page - 1, 0) * size
            rows = conn.execute(
                f"SELECT * FROM {LIFECYCLE_TABLE} {where} ORDER BY id LIMIT ? OFFSET ?",
                [*params, size, start],
            ).fetchall()
        return [self._lifecycle_row(row) for row in rows], total

    def get_lifecycle(self, entry_id: int) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {LIFECYCLE_TABLE} WHERE id = ?", (entry_id,)
            ).fetchone()
        return self._lifecycle_row(row) if row else None

    def get_lifecycle_by_policy_no(self, policy_no: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {LIFECYCLE_TABLE} WHERE policy_no = ?", (policy_no,)
            ).fetchone()
        return self._lifecycle_row(row) if row else None

    def get_recycle_by_record_no(self, record_no: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {RECYCLE_TABLE} WHERE record_no = ?", (record_no,)
            ).fetchone()
        return self._recycle_row(row) if row else None

    def get_audit_by_record_no(self, record_no: str) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT * FROM {AUDIT_TABLE} WHERE record_no = ?", (record_no,)
            ).fetchone()
        return self._audit_row(row) if row else None

    def list_recycle(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        return self._list_records(
            RECYCLE_TABLE, "record_no", self._recycle_row, keyword=keyword, status=status, page=page, size=size
        )

    def get_recycle(self, entry_id: int) -> dict[str, Any] | None:
        return self._get_record(RECYCLE_TABLE, entry_id, self._recycle_row)

    def list_audit(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        return self._list_records(
            AUDIT_TABLE, "record_no", self._audit_row, keyword=keyword, status=status, page=page, size=size
        )

    def get_audit(self, entry_id: int) -> dict[str, Any] | None:
        return self._get_record(AUDIT_TABLE, entry_id, self._audit_row)

    def _list_records(
        self,
        table: str,
        keyword_column: str,
        builder,
        *,
        keyword: str | None,
        status: str | None,
        page: int,
        size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        self.ensure_schema()
        clauses: list[str] = []
        params: list[Any] = []
        if keyword:
            clauses.append(f"{keyword_column} LIKE ?")
            params.append(f"%{keyword}%")
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as conn:
            total = int(conn.execute(f"SELECT COUNT(*) FROM {table} {where}", params).fetchone()[0])
            start = max(page - 1, 0) * size
            rows = conn.execute(
                f"SELECT * FROM {table} {where} ORDER BY id LIMIT ? OFFSET ?",
                [*params, size, start],
            ).fetchall()
        return [builder(row) for row in rows], total

    def _get_record(self, table: str, entry_id: int, builder) -> dict[str, Any] | None:
        self.ensure_schema()
        with self._connect() as conn:
            row = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (entry_id,)).fetchone()
        return builder(row) if row else None

    def counts(self) -> dict[str, dict[str, int]]:
        """给运营概览使用：各台账的记录数、待处理量、异常量。"""
        self.ensure_schema()
        result: dict[str, dict[str, int]] = {}
        with self._connect() as conn:
            for module, table in (
                ("lifecycle", LIFECYCLE_TABLE),
                ("recycle", RECYCLE_TABLE),
                ("audit", AUDIT_TABLE),
            ):
                row = conn.execute(
                    f"SELECT COUNT(*) AS created, "
                    f"COALESCE(SUM(CASE WHEN pending THEN 1 ELSE 0 END), 0) AS pending, "
                    f"COALESCE(SUM(CASE WHEN abnormal THEN 1 ELSE 0 END), 0) AS abnormal "
                    f"FROM {table}"
                ).fetchone()
                result[module] = {
                    "created": int(row["created"]),
                    "pending": int(row["pending"]),
                    "abnormal": int(row["abnormal"]),
                }
        return result

    # ---------------------------------------------------------- 运行期写接口
    def create_lifecycle(self, values: dict[str, Any]) -> dict[str, Any]:
        self.ensure_schema()
        policy_no = str(values.get("策略编号") or "").strip()
        now = utc_now()
        with self._write_tx() as conn:
            existing = conn.execute(
                f"SELECT id FROM {LIFECYCLE_TABLE} WHERE policy_no = ?", (policy_no,)
            ).fetchone()
            if existing:
                raise _DuplicateRecord(f"策略编号 {policy_no} 已存在，以首次落库记录为准")
            row = conn.execute(
                f"INSERT INTO {LIFECYCLE_TABLE}("
                "policy_no, table_name, created_on, retention_days, archive_path, "
                "cleanup_method, last_action, policy_status, status, pending, abnormal, "
                "source, backfilled_fields, extra_json, created_at, updated_at, revision"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    policy_no,
                    values.get("表名称"),
                    values.get("创建日期"),
                    values.get("保留天数"),
                    values.get("归档路径"),
                    values.get("清理方式"),
                    values.get("最后操作"),
                    values.get("策略状态"),
                    STATUS_ORDER[0],
                    1,
                    0,
                    "manual",
                    "[]",
                    "{}",
                    now,
                    now,
                    0,
                ),
            )
            inserted_id = int(row.lastrowid)
            fresh = conn.execute(
                f"SELECT * FROM {LIFECYCLE_TABLE} WHERE id = ?", (inserted_id,)
            ).fetchone()
        return self._lifecycle_row(fresh)

    def apply_lifecycle_action(
        self,
        entry_id: int,
        expected_revision: int,
        *,
        action: str,
        target_status: str,
        pending: bool,
        abnormal: bool,
    ) -> tuple[dict[str, Any], bool]:
        """按 revision 乐观锁写入动作；返回（当前记录, 本次是否落库）。

        并发时若另一个写入已经成功提交，本次更新不会覆盖它，调用方拿到的是
        首次落库的结论。
        """
        self.ensure_schema()
        now = utc_now()
        with self._write_tx() as conn:
            current = conn.execute(
                f"SELECT * FROM {LIFECYCLE_TABLE} WHERE id = ? AND revision = ?",
                (entry_id, expected_revision),
            ).fetchone()
            if current is None:
                latest = conn.execute(
                    f"SELECT * FROM {LIFECYCLE_TABLE} WHERE id = ?", (entry_id,)
                ).fetchone()
                if latest is None:
                    raise LookupError(f"生命周期策略 {entry_id} 不存在")
                return self._lifecycle_row(latest), False

            before = self._lifecycle_row(current)
            conn.execute(
                f"UPDATE {LIFECYCLE_TABLE} SET status=?, pending=?, abnormal=?, "
                "last_action=?, updated_at=?, revision=revision+1 WHERE id=? AND revision=?",
                (
                    target_status,
                    1 if pending else 0,
                    1 if abnormal else 0,
                    action,
                    now,
                    entry_id,
                    expected_revision,
                ),
            )
            updated = conn.execute(
                f"SELECT * FROM {LIFECYCLE_TABLE} WHERE id = ?", (entry_id,)
            ).fetchone()
            after = self._lifecycle_row(updated)
            # 动作流水写进变更审计，让上一轮清理/归档能够被解释。
            self._insert_audit_if_absent(
                conn,
                record_no=f"AUDI-{before['策略编号']}-R{expected_revision + 1}",
                target=f"生命周期策略 {before['策略编号']}",
                change_type=f"动作：{action}",
                before_value=_json_dump(self._public_snapshot(before)),
                after_value=_json_dump(self._public_snapshot(after)),
                operator="system",
                changed_at=now,
                review_status="已记录",
                status="已记录",
                pending=True,
                abnormal=False,
                source="action",
            )
        return after, True

    def create_recycle(self, values: dict[str, Any]) -> dict[str, Any]:
        return self._create_record(
            RECYCLE_TABLE,
            record_column="record_no",
            record_no=str(values.get("记录编号") or "").strip(),
            field_values={
                "table_name": values.get("表名称"),
                "deleted_at": values.get("删除时间"),
                "reason": values.get("回收原因"),
                "data_size": values.get("数据大小"),
                "recoverable_until": values.get("可恢复期"),
                "restore_status": values.get("恢复状态"),
                "cleaned_at": values.get("清理时间"),
            },
            status_order=RECYCLE_STATUS_ORDER,
            builder=self._recycle_row,
        )

    def apply_recycle_action(
        self, entry_id: int, expected_revision: int, *, target_status: str, pending: bool, abnormal: bool
    ) -> tuple[dict[str, Any], bool]:
        return self._apply_action(
            RECYCLE_TABLE,
            entry_id,
            expected_revision,
            target_status=target_status,
            pending=pending,
            abnormal=abnormal,
            builder=self._recycle_row,
        )

    def create_audit(self, values: dict[str, Any]) -> dict[str, Any]:
        return self._create_record(
            AUDIT_TABLE,
            record_column="record_no",
            record_no=str(values.get("记录编号") or "").strip(),
            field_values={
                "target": values.get("变更对象"),
                "change_type": values.get("变更类型"),
                "before_value": values.get("变更前值"),
                "after_value": values.get("变更后值"),
                "operator": values.get("操作人"),
                "changed_at": values.get("操作时间"),
                "review_status": values.get("复核状态"),
            },
            status_order=AUDIT_STATUS_ORDER,
            builder=self._audit_row,
        )

    def apply_audit_action(
        self, entry_id: int, expected_revision: int, *, target_status: str, pending: bool, abnormal: bool
    ) -> tuple[dict[str, Any], bool]:
        return self._apply_action(
            AUDIT_TABLE,
            entry_id,
            expected_revision,
            target_status=target_status,
            pending=pending,
            abnormal=abnormal,
            builder=self._audit_row,
        )

    def _create_record(
        self,
        table: str,
        *,
        record_column: str,
        record_no: str,
        field_values: dict[str, Any],
        status_order: Sequence[str],
        builder,
    ) -> dict[str, Any]:
        self.ensure_schema()
        now = utc_now()
        columns = [record_column, *field_values.keys(), "status", "pending", "abnormal", "source", "created_at", "updated_at", "revision"]
        values = [record_no, *field_values.values(), status_order[0], 1, 0, "manual", now, now, 0]
        placeholders = ",".join("?" for _ in columns)
        with self._write_tx() as conn:
            existing = conn.execute(
                f"SELECT id FROM {table} WHERE {record_column} = ?", (record_no,)
            ).fetchone()
            if existing:
                raise _DuplicateRecord(f"记录编号 {record_no} 已存在，以首次落库记录为准")
            cursor = conn.execute(
                f"INSERT INTO {table}({','.join(columns)}) VALUES ({placeholders})", values
            )
            fresh = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (cursor.lastrowid,)).fetchone()
        return builder(fresh)

    def _apply_action(
        self,
        table: str,
        entry_id: int,
        expected_revision: int,
        *,
        target_status: str,
        pending: bool,
        abnormal: bool,
        builder,
    ) -> tuple[dict[str, Any], bool]:
        self.ensure_schema()
        now = utc_now()
        with self._write_tx() as conn:
            current = conn.execute(
                f"SELECT * FROM {table} WHERE id = ? AND revision = ?",
                (entry_id, expected_revision),
            ).fetchone()
            if current is None:
                latest = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (entry_id,)).fetchone()
                if latest is None:
                    raise LookupError(f"记录 {entry_id} 不存在")
                return builder(latest), False
            conn.execute(
                f"UPDATE {table} SET status=?, pending=?, abnormal=?, updated_at=?, "
                "revision=revision+1 WHERE id=? AND revision=?",
                (target_status, 1 if pending else 0, 1 if abnormal else 0, now, entry_id, expected_revision),
            )
            fresh = conn.execute(f"SELECT * FROM {table} WHERE id = ?", (entry_id,)).fetchone()
        return builder(fresh), True

    @staticmethod
    def _public_snapshot(row: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in row.items() if not key.startswith("_") and key != "备份位置"}

    # ------------------------------------------------------------ 迁移与回填
    def _backup(self, reason: str) -> str | None:
        path = Path(self.db_path)
        if not path.exists() or path.stat().st_size == 0:
            return None
        Path(self.backup_dir).mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        target = Path(self.backup_dir) / f"lifecycle-{stamp}-{os.getpid()}-{reason}.sqlite3"
        src = sqlite3.connect(self.db_path, timeout=30)
        dst = sqlite3.connect(str(target))
        try:
            src.backup(dst)
        finally:
            dst.close()
            src.close()
        return str(target)

    def _insert_legacy_lifecycle(
        self, conn: sqlite3.Connection, values: dict[str, Any], *, source: str, now: str
    ) -> bool:
        policy_no = str(values.get("策略编号") or "").strip()
        if not policy_no:
            raise ValueError("旧生命周期记录缺少策略编号，无法按策略编号保留")
        if conn.execute(
            f"SELECT 1 FROM {LIFECYCLE_TABLE} WHERE policy_no = ?", (policy_no,)
        ).fetchone():
            return False

        present, backfilled = _split_optional(
            values, ("策略编号", "表名称", "创建日期", "策略状态"), OPTIONAL_LIFECYCLE_FIELDS
        )
        status = str(values.get("status") or "").strip()
        if status not in STATUS_ORDER:
            candidate = str(present.get("策略状态") or "").strip()
            status = candidate if candidate in STATUS_ORDER else STATUS_ORDER[0]
        pending = values.get("pending")
        if pending is None:
            pending = status != STATUS_ORDER[-1]
        abnormal = values.get("abnormal", False)

        explicit_id = values.get("id")
        use_explicit = isinstance(explicit_id, int) or (
            isinstance(explicit_id, str) and explicit_id.isdigit()
        )
        if use_explicit:
            candidate_id = int(explicit_id)
            taken = conn.execute(
                f"SELECT policy_no FROM {LIFECYCLE_TABLE} WHERE id = ?", (candidate_id,)
            ).fetchone()
            if taken is not None:
                use_explicit = False

        known = {
            "策略编号",
            "表名称",
            "创建日期",
            "保留天数",
            "归档路径",
            "清理方式",
            "最后操作",
            "策略状态",
            "status",
            "pending",
            "abnormal",
            "id",
            "_migration_source",
        }
        extra = {key: value for key, value in values.items() if key not in known}
        params = [
            present["策略编号"],
            present["表名称"],
            present["创建日期"],
            present["保留天数"],
            present["归档路径"],
            present["清理方式"],
            present["最后操作"],
            present["策略状态"],
            status,
            1 if pending else 0,
            1 if abnormal else 0,
            source,
            _json_dump(backfilled),
            _json_dump(extra),
            now,
            now,
            0,
        ]
        columns = (
            "policy_no, table_name, created_on, retention_days, archive_path, cleanup_method, "
            "last_action, policy_status, status, pending, abnormal, source, backfilled_fields, "
            "extra_json, created_at, updated_at, revision"
        )
        if use_explicit:
            conn.execute(
                f"INSERT INTO {LIFECYCLE_TABLE}(id, {columns}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [int(explicit_id), *params],
            )
        else:
            conn.execute(
                f"INSERT INTO {LIFECYCLE_TABLE}({columns}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params,
            )
        return True

    def _insert_legacy_recycle(self, conn: sqlite3.Connection, values: dict[str, Any], *, source: str, now: str) -> bool:
        record_no = str(values.get("记录编号") or "").strip()
        if not record_no:
            raise ValueError("旧回收记录缺少记录编号，无法按记录编号保留")
        if conn.execute(f"SELECT 1 FROM {RECYCLE_TABLE} WHERE record_no = ?", (record_no,)).fetchone():
            return False
        status = str(values.get("status") or "").strip()
        if status not in RECYCLE_STATUS_ORDER:
            status = RECYCLE_STATUS_ORDER[0]
        pending = values.get("pending")
        if pending is None:
            pending = status != RECYCLE_STATUS_ORDER[-1]
        id_clause, id_params = self._free_id_clause(conn, RECYCLE_TABLE, values.get("id"))
        columns = (
            "record_no, table_name, deleted_at, reason, data_size, recoverable_until, "
            "restore_status, cleaned_at, status, pending, abnormal, source, backup_path, "
            "linked_policy_no, created_at, updated_at, revision"
        )
        params = [
            record_no,
            values.get("表名称"),
            values.get("删除时间"),
            values.get("回收原因"),
            values.get("数据大小"),
            values.get("可恢复期"),
            values.get("恢复状态"),
            values.get("清理时间"),
            status,
            1 if pending else 0,
            1 if bool(values.get("abnormal")) else 0,
            source,
            values.get("备份位置"),
            values.get("关联策略编号"),
            now,
            now,
            0,
        ]
        conn.execute(
            f"INSERT INTO {RECYCLE_TABLE}({'id, ' if id_clause else ''}{columns}) "
            f"VALUES ({'?,' if id_clause else ''}{','.join('?' for _ in params)})",
            [*id_params, *params],
        )
        return True

    def _insert_legacy_audit(self, conn: sqlite3.Connection, values: dict[str, Any], *, source: str, now: str) -> bool:
        record_no = str(values.get("记录编号") or "").strip()
        if not record_no:
            raise ValueError("旧变更记录缺少记录编号，无法按记录编号保留")
        if conn.execute(f"SELECT 1 FROM {AUDIT_TABLE} WHERE record_no = ?", (record_no,)).fetchone():
            return False
        review_status = str(values.get("复核状态") or "").strip() or "已记录"
        status = str(values.get("status") or "").strip()
        if status not in AUDIT_STATUS_ORDER:
            status = review_status if review_status in AUDIT_STATUS_ORDER else AUDIT_STATUS_ORDER[0]
        pending = values.get("pending")
        if pending is None:
            pending = status != AUDIT_STATUS_ORDER[-1]
        id_clause, id_params = self._free_id_clause(conn, AUDIT_TABLE, values.get("id"))
        columns = (
            "record_no, target, change_type, before_value, after_value, operator, changed_at, "
            "review_status, status, pending, abnormal, source, created_at, updated_at, revision"
        )
        params = [
            record_no,
            values.get("变更对象"),
            values.get("变更类型"),
            values.get("变更前值"),
            values.get("变更后值"),
            values.get("操作人"),
            values.get("操作时间"),
            review_status,
            status,
            1 if pending else 0,
            1 if bool(values.get("abnormal")) else 0,
            source,
            now,
            now,
            0,
        ]
        conn.execute(
            f"INSERT INTO {AUDIT_TABLE}({'id, ' if id_clause else ''}{columns}) "
            f"VALUES ({'?,' if id_clause else ''}{','.join('?' for _ in params)})",
            [*id_params, *params],
        )
        return True

    @staticmethod
    def _free_id_clause(conn: sqlite3.Connection, table: str, explicit_id: Any) -> tuple[bool, list[Any]]:
        if isinstance(explicit_id, int) or (isinstance(explicit_id, str) and str(explicit_id).isdigit()):
            candidate = int(explicit_id)
            taken = conn.execute(f"SELECT 1 FROM {table} WHERE id = ?", (candidate,)).fetchone()
            if taken is None:
                return True, [candidate]
        return False, []

    @staticmethod
    def _insert_audit_if_absent(
        conn: sqlite3.Connection,
        *,
        record_no: str,
        target: str,
        change_type: str,
        before_value: str,
        after_value: str,
        operator: str,
        changed_at: str,
        review_status: str,
        status: str,
        pending: bool,
        abnormal: bool,
        source: str,
        backup_path: str | None = None,
        linked_policy_no: str | None = None,
    ) -> bool:
        exists = conn.execute(
            f"SELECT 1 FROM {AUDIT_TABLE} WHERE record_no = ?", (record_no,)
        ).fetchone()
        if exists:
            return False
        conn.execute(
            f"INSERT INTO {AUDIT_TABLE}(record_no, target, change_type, before_value, after_value, "
            "operator, changed_at, review_status, status, pending, abnormal, source, created_at, "
            "updated_at, revision) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                record_no,
                target,
                change_type,
                before_value,
                after_value,
                operator,
                changed_at,
                review_status,
                status,
                1 if pending else 0,
                1 if abnormal else 0,
                source,
                changed_at,
                changed_at,
                0,
            ),
        )
        return True

    def migrate(
        self,
        *,
        lifecycle_rows: Iterable[dict[str, Any]],
        recycle_rows: Iterable[dict[str, Any]] = (),
        audit_rows: Iterable[dict[str, Any]] = (),
        source: str = "legacy",
        reason: str = "migrate",
    ) -> MigrationReport:
        """把旧记录回填到本地库。

        数据顺序由调用方按部署顺序给出（历史记录在前、本地开发样例在后）；
        同一策略编号/记录编号已存在时直接跳过，因此重复执行不会产生副本。
        每条记录允许用保留键 ``_migration_source`` 覆盖来源标记。
        """
        self.ensure_schema()
        report = MigrationReport(
            run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
            backup_path=None,
            db_path=self.db_path,
            inserted={"lifecycle": 0, "recycle": 0, "audit": 0},
            skipped={"lifecycle": 0, "recycle": 0, "audit": 0},
        )
        try:
            report.backup_path = self._backup(reason)
        except Exception as exc:  # 备份失败不再继续写，避免失去恢复位置
            raise MigrationError(
                f"迁移前备份失败：{exc}", db_path=self.db_path, backup_path=None
            ) from exc

        now = utc_now()
        accepted_lifecycle: list[dict[str, Any]] = []
        try:
            with self._write_tx() as conn:
                conn.execute(
                    "INSERT INTO life_migration_run(run_id, started_at, status, backup_path, detail) "
                    "VALUES (?,?,?,?,?)",
                    (report.run_id, now, "running", report.backup_path, "{}"),
                )
                for values in lifecycle_rows:
                    row = dict(values)
                    row_source = str(row.pop("_migration_source", source))
                    if self._insert_legacy_lifecycle(conn, row, source=row_source, now=now):
                        report.inserted["lifecycle"] += 1
                        accepted_lifecycle.append(row)
                    else:
                        report.skipped["lifecycle"] += 1
                for values in recycle_rows:
                    row = dict(values)
                    row_source = str(row.pop("_migration_source", source))
                    if self._insert_legacy_recycle(conn, row, source=row_source, now=now):
                        report.inserted["recycle"] += 1
                    else:
                        report.skipped["recycle"] += 1
                for values in audit_rows:
                    row = dict(values)
                    row_source = str(row.pop("_migration_source", source))
                    if self._insert_legacy_audit(conn, row, source=row_source, now=now):
                        report.inserted["audit"] += 1
                    else:
                        report.skipped["audit"] += 1

                policies = [
                    self._lifecycle_row(row)
                    for row in conn.execute(
                        f"SELECT * FROM {LIFECYCLE_TABLE} ORDER BY id"
                    ).fetchall()
                ]
                report.policy_numbers = [row["策略编号"] for row in policies]

                for input_row in accepted_lifecycle:
                    policy_no = str(input_row.get("策略编号") or "").strip()
                    stored = conn.execute(
                        f"SELECT * FROM {LIFECYCLE_TABLE} WHERE policy_no = ?", (policy_no,)
                    ).fetchone()
                    if stored is None:
                        continue
                    if self._migration_diff_audit(
                        conn,
                        before_input=input_row,
                        stored=self._lifecycle_row(stored),
                        backfilled=_json_load(stored["backfilled_fields"], []),
                        extra=_json_load(stored["extra_json"], {}),
                        now=now,
                    ):
                        report.audit_diffs += 1
                        report.inserted["audit"] += 1

                report.recycle_snapshot = self._migration_recycle_snapshot(
                    conn, policies=policies, now=now, backup_path=report.backup_path
                )
                if report.recycle_snapshot:
                    report.inserted["recycle"] += 1

                conn.execute(
                    "UPDATE life_migration_run SET finished_at=?, status=?, detail=? WHERE run_id=?",
                    (
                        utc_now(),
                        "succeeded",
                        _json_dump(report.as_dict()),
                        report.run_id,
                    ),
                )
        except Exception as exc:
            self._record_failed_run(report, exc)
            raise MigrationError(
                f"迁移事务失败：{exc}", db_path=self.db_path, backup_path=report.backup_path
            ) from exc
        return report

    def _record_failed_run(self, report: MigrationReport, exc: BaseException) -> None:
        """事务回滚后单独记一条失败流水；此操作失败也不影响恢复位置。"""
        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO life_migration_run(run_id, started_at, finished_at, "
                    "status, backup_path, detail) VALUES (?,?,?,?,?,?)",
                    (
                        report.run_id,
                        utc_now(),
                        utc_now(),
                        "failed",
                        report.backup_path,
                        _json_dump({"error": str(exc), "backup_path": report.backup_path}),
                    ),
                )
        except Exception:  # 流水只是旁证，不掩盖原始迁移错误
            pass

    def _migration_diff_audit(
        self,
        conn: sqlite3.Connection,
        *,
        before_input: dict[str, Any],
        stored: dict[str, Any],
        backfilled: list[str],
        extra: dict[str, Any],
        now: str,
    ) -> bool:
        """写一条迁移前后差异的审计记录；只保留旧记录本身有语义的字段。"""
        policy_no = stored["策略编号"]
        before = {
            key: before_input.get(key)
            for key in LIFECYCLE_FIELDS
            if key in before_input
        }
        before["status"] = before_input.get("status", stored["status"])
        before["pending"] = before_input.get("pending", stored["pending"])
        before["abnormal"] = before_input.get("abnormal", stored["abnormal"])

        after = {
            key: stored.get(key)
            for key in LIFECYCLE_FIELDS
        }
        after.update(
            {
                "status": stored["status"],
                "pending": stored["pending"],
                "abnormal": stored["abnormal"],
                "补齐字段": backfilled,
                "额外字段": extra,
                "入库时间": now,
            }
        )
        record_no = f"AUDI-MIG-{self._safe_record_suffix(policy_no)}"
        return self._insert_audit_if_absent(
            conn,
            record_no=record_no,
            target=f"生命周期策略 {policy_no}",
            change_type="数据迁移",
            before_value=_json_dump(before),
            after_value=_json_dump(after),
            operator="migration",
            changed_at=now,
            review_status="已记录",
            status="已记录",
            pending=True,
            abnormal=False,
            source="migration",
        )

    @staticmethod
    def _safe_record_suffix(policy_no: str) -> str:
        return re.sub(r"[^0-9A-Za-z-]+", "_", policy_no).strip("_") or "UNKNOWN"

    def _migration_recycle_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        policies: list[dict[str, Any]],
        now: str,
        backup_path: str | None,
    ) -> str | None:
        existing = conn.execute(
            f"SELECT 1 FROM {RECYCLE_TABLE} WHERE record_no = ?", ("RECY-MIG-0001",)
        ).fetchone()
        if existing:
            return None
        summary = " / ".join(f"{row['策略编号']}={row['status']}" for row in policies) or "无策略"
        conn.execute(
            f"INSERT INTO {RECYCLE_TABLE}(record_no, table_name, deleted_at, reason, data_size, "
            "recoverable_until, restore_status, cleaned_at, status, pending, abnormal, source, "
            "backup_path, linked_policy_no, created_at, updated_at, revision) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "RECY-MIG-0001",
                LIFECYCLE_TABLE,
                now,
                f"生命周期数据迁移快照：{summary}",
                f"{len(policies)} 条策略",
                "迁移前备份可恢复",
                "待恢复",
                None,
                RECYCLE_STATUS_ORDER[0],
                1,
                0,
                "migration",
                backup_path,
                None,
                now,
                now,
                0,
            ),
        )
        return "RECY-MIG-0001"

    # -------------------------------------------------------------- 兼容入口
    def bootstrap_from_seed(self) -> MigrationReport:
        """启动时按部署顺序回填：先历史记录，再本地开发样例。"""
        from app.seed import SEED_ROWS

        history_lifecycle: list[dict[str, Any]] = []
        history_recycle: list[dict[str, Any]] = []
        history_audit: list[dict[str, Any]] = []
        if self.legacy_seed_path:
            history_lifecycle, history_recycle, history_audit = load_legacy_bundle(
                self.legacy_seed_path
            )
        lifecycle_rows = [
            {**row, "_migration_source": "history"} for row in history_lifecycle
        ] + [{**row, "_migration_source": "seed"} for row in SEED_ROWS["lifecycle"]]
        recycle_rows = [
            {**row, "_migration_source": "history"} for row in history_recycle
        ] + [{**row, "_migration_source": "seed"} for row in SEED_ROWS["recycle"]]
        audit_rows = [
            {**row, "_migration_source": "history"} for row in history_audit
        ] + [{**row, "_migration_source": "seed"} for row in SEED_ROWS["audit"]]
        return self.migrate(
            lifecycle_rows=lifecycle_rows,
            recycle_rows=recycle_rows,
            audit_rows=audit_rows,
            source="history" if history_lifecycle else "seed",
            reason="bootstrap",
        )


def _load_list(data: Any, key: str, path: str) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        data = data.get(key, [])
    if not isinstance(data, list):
        raise ValueError(f"{path} 不是有效的 {key} 记录列表")
    return [dict(item) for item in data]


def load_legacy_bundle(
    path: str | os.PathLike[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """读取历史快照，返回（生命周期、回收记录、变更记录）三组数据。

    支持顶层 ``[...]``（仅生命周期）或 ``{"lifecycle": [...], "recycle": [...],
    "audit": [...]}`` 两种结构，旧环境导出的单表文件也能直接兼容。
    """
    raw = Path(path).read_text(encoding="utf-8")
    data = json.loads(raw)
    if isinstance(data, list):
        return _load_list(data, "lifecycle", str(path)), [], []
    return (
        _load_list(data, "lifecycle", str(path)),
        _load_list(data, "recycle", str(path)),
        _load_list(data, "audit", str(path)),
    )


def load_legacy_lifecycle_file(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """只读历史生命周期快照，保留给旧脚本调用。"""
    return load_legacy_bundle(path)[0]


database = LifecycleDatabase()
