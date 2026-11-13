"""生命周期台账运维工具：迁移回填、完整性核对与备份。

用法（在 backend 目录下）：
    python -m app.persistence_cli migrate --legacy ../data/legacy/lifecycle-history.json
    python -m app.persistence_cli migrate --with-dev-seed
    python -m app.persistence_cli verify --expect LIFE-0001,LIFE-0002,LIFE-0003
    python -m app.persistence_cli snapshot --label pre-cutover

``migrate`` 先回填历史快照，再回填本地开发样例；同一策略编号/记录编号只会保留
第一条成功落库的记录，重复执行是幂等的。失败时退出码非 0，并输出可恢复位置。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from app.persistence import database
from app.persistence.db import (
    AUDIT_TABLE,
    LIFECYCLE_TABLE,
    RECYCLE_TABLE,
    MigrationError,
    load_legacy_bundle,
)


def _default_legacy_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "legacy" / "lifecycle-history.json"


def _parse_json_file(path: str, key: str) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get(key, [])
    if not isinstance(data, list):
        raise SystemExit(f"{path} 不是有效的 {key} 记录列表")
    return [dict(item) for item in data]


def _bundle(path: str | None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if not path:
        return [], [], []
    return load_legacy_bundle(path)


def _parse_json_file(path: str, key: str) -> list[dict[str, Any]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get(key, [])
    if not isinstance(data, list):
        raise SystemExit(f"{path} 不是有效的 {key} 记录列表")
    return [dict(item) for item in data]


def command_migrate(args: argparse.Namespace) -> int:
    """按部署顺序回填：先历史记录（--legacy / --history-*），再本地开发样例（--with-dev-seed）。"""
    from app.seed import SEED_ROWS

    history_lifecycle: list[dict[str, Any]] = []
    history_recycle: list[dict[str, Any]] = []
    history_audit: list[dict[str, Any]] = []
    if args.legacy:
        history_lifecycle, bundle_recycle, bundle_audit = _bundle(args.legacy)
        history_recycle = bundle_recycle
        history_audit = bundle_audit
    if args.history_recycle:
        history_recycle = _parse_json_file(args.history_recycle, "recycle")
    if args.history_audit:
        history_audit = _parse_json_file(args.history_audit, "audit")

    lifecycle_rows = [
        {**row, "_migration_source": "history"} for row in history_lifecycle
    ]
    recycle_rows = [{**row, "_migration_source": "history"} for row in history_recycle]
    audit_rows = [{**row, "_migration_source": "history"} for row in history_audit]
    if args.with_dev_seed:
        lifecycle_rows += [{**row, "_migration_source": "seed"} for row in SEED_ROWS["lifecycle"]]
        recycle_rows += [{**row, "_migration_source": "seed"} for row in SEED_ROWS["recycle"]]
        audit_rows += [{**row, "_migration_source": "seed"} for row in SEED_ROWS["audit"]]

    database.ensure_schema()
    try:
        report = database.migrate(
            lifecycle_rows=lifecycle_rows,
            recycle_rows=recycle_rows,
            audit_rows=audit_rows,
            source="history" if history_lifecycle else "seed",
            reason=args.reason,
        )
    except MigrationError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False, indent=2))
        return 2

    payload = report.as_dict()
    payload["db_path"] = database.db_path
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def command_verify(args: argparse.Namespace) -> int:
    database.ensure_schema()
    expected = [item.strip() for item in (args.expect or "").split(",") if item.strip()]
    with database._connect() as conn:  # noqa: SLF001 - 运维核对需要直读台账
        policies = conn.execute(
            f"SELECT policy_no, status, pending, abnormal, retention_days, archive_path, "
            f"cleanup_method, created_on, backfilled_fields FROM {LIFECYCLE_TABLE} "
            f"ORDER BY policy_no"
        ).fetchall()
        recycle = conn.execute(
            f"SELECT record_no, status, pending FROM {RECYCLE_TABLE} ORDER BY record_no"
        ).fetchall()
        audits = conn.execute(
            f"SELECT record_no, status, review_status, change_type FROM {AUDIT_TABLE} "
            f"ORDER BY record_no"
        ).fetchall()
        counts = {
            "lifecycle": conn.execute(f"SELECT COUNT(*) FROM {LIFECYCLE_TABLE}").fetchone()[0],
            "recycle": conn.execute(f"SELECT COUNT(*) FROM {RECYCLE_TABLE}").fetchone()[0],
            "audit": conn.execute(f"SELECT COUNT(*) FROM {AUDIT_TABLE}").fetchone()[0],
        }

    present = {row["policy_no"] for row in policies}
    missing = [item for item in expected if item not in present]
    duplicates = {
        "lifecycle": counts["lifecycle"] - len(present),
        "recycle": counts["recycle"] - len({row["record_no"] for row in recycle}),
        "audit": counts["audit"] - len({row["record_no"] for row in audits}),
    }
    result = {
        "ok": not missing and not any(duplicates.values()),
        "db_path": database.db_path,
        "counts": counts,
        "duplicates": duplicates,
        "missing_expected": missing,
        "lifecycle": [dict(row) for row in policies],
        "recycle": [dict(row) for row in recycle],
        "audit": [dict(row) for row in audits],
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


def command_snapshot(args: argparse.Namespace) -> int:
    database.ensure_schema()
    path = database._backup(args.label)  # noqa: SLF001 - 运维快照复用同一备份实现
    print(json.dumps({"ok": True, "backup_path": path, "db_path": database.db_path}, ensure_ascii=False))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生命周期台账迁移与核对")
    parser.add_argument(
        "--db",
        default=os.getenv("LIFECYCLE_DB_PATH"),
        help="SQLite 文件路径，默认取 LIFECYCLE_DB_PATH 或 backend/data/lifecycle.sqlite3",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    migrate = sub.add_parser("migrate", help="回填历史记录与本地开发样例（幂等）")
    migrate.add_argument("--legacy", help="历史快照 JSON（可同时包含 lifecycle/recycle/audit）")
    migrate.add_argument("--history-recycle", help="历史回收记录 JSON（记录编号唯一）")
    migrate.add_argument("--history-audit", help="历史变更记录 JSON（记录编号唯一）")
    migrate.add_argument("--with-dev-seed", action="store_true", help="同时回填内存样例里的本地开发数据")
    migrate.add_argument("--reason", default="manual", help="备份文件后缀，标记本次迁移目的")
    migrate.set_defaults(func=command_migrate)

    verify = sub.add_parser("verify", help="核对迁移结果的完整性与唯一性")
    verify.add_argument("--expect", default="LIFE-0001,LIFE-0002,LIFE-0003", help="必须存在的策略编号")
    verify.set_defaults(func=command_verify)

    snapshot = sub.add_parser("snapshot", help="生成迁移/切换前的数据库快照")
    snapshot.add_argument("--label", default="snapshot", help="快照标签")
    snapshot.set_defaults(func=command_snapshot)

    args = parser.parse_args(argv)
    if getattr(args, "db", None):
        database.db_path = args.db
        database._schema_ready = False  # noqa: SLF001 - 切换库文件后重新建表
    if args.command == "migrate" and not args.legacy and not args.with_dev_seed \
            and not args.history_recycle and not args.history_audit:
        legacy = _default_legacy_path()
        if legacy.exists():
            args.legacy = str(legacy)
        args.with_dev_seed = True
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
