"""生命周期持久化回归测试：重启恢复、幂等迁移、并发结论与失败可恢复。

运行：backend/.venv/bin/python -m unittest discover -s backend/tests -v
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_ROOT))

from app.persistence.db import (  # noqa: E402
    AUDIT_TABLE,
    LIFECYCLE_TABLE,
    RECYCLE_TABLE,
    LifecycleDatabase,
    load_legacy_bundle,
)
from app.services.audit import AuditService  # noqa: E402
from app.services.lifecycle import LifecycleService  # noqa: E402
from app.services.recycle import RecycleService  # noqa: E402

LEGACY_PATH = BACKEND_ROOT / "data" / "legacy" / "lifecycle-history.json"


def _db(tmp: str) -> LifecycleDatabase:
    return LifecycleDatabase(
        db_path=os.path.join(tmp, "life.sqlite3"),
        backup_dir=os.path.join(tmp, "backups"),
        legacy_seed_path=str(LEGACY_PATH),
    )


class PersistenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _bootstrap(self) -> LifecycleDatabase:
        db = _db(self.tmp)
        db.bootstrap_from_seed()
        return db

    def test_restart_keeps_status_and_action_history(self) -> None:
        db = self._bootstrap()
        service = LifecycleService(db)
        entry = service.get_entry(3)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "草稿")
        self.assertTrue(entry["pending"])
        _, message = service.run_action(3, "删除表")
        self.assertIn("删除表", message)

        # 模拟重启：重新打开同一个库文件，状态、待处理标记与最后动作都必须还在。
        reopened = _db(self.tmp)
        entry = LifecycleService(reopened).get_entry(3)
        self.assertEqual(entry["status"], "已到期")
        self.assertTrue(entry["pending"])  # 未到“已归档”终态，仍算待处理
        self.assertEqual(entry["最后操作"], "删除表")

        recycle = RecycleService(reopened).get_entry(1)
        self.assertEqual(recycle["status"], "待恢复")
        self.assertTrue(recycle["pending"])

        audit = AuditService(reopened).get_entry(1)
        self.assertEqual(audit["status"], "已记录")
        self.assertEqual(audit["复核状态"], "已记录")

        # 动作流水进了变更审计，上一轮动作可被解释。
        diffs = [row for row in reopened.list_audit(size=200)[0] if row["变更类型"].startswith("动作：")]
        self.assertEqual(len(diffs), 1)
        self.assertIn("删除表", diffs[0]["变更类型"])

    def test_migration_is_idempotent_without_copies(self) -> None:
        db = self._bootstrap()
        first = db.list_lifecycle(size=200)
        report = db.bootstrap_from_seed()
        self.assertEqual(report.inserted["lifecycle"], 0)
        self.assertEqual(report.inserted["recycle"], 0)
        second = db.list_lifecycle(size=200)
        self.assertEqual(first[1], second[1])
        self.assertEqual([row["策略编号"] for row in first[0]], [row["策略编号"] for row in second[0]])
        with db._connect() as conn:  # noqa: SLF001 - 断言唯一键
            for table, column in (
                (LIFECYCLE_TABLE, "policy_no"),
                (RECYCLE_TABLE, "record_no"),
                (AUDIT_TABLE, "record_no"),
            ):
                duplicates = conn.execute(
                    f"SELECT {column}, COUNT(*) c FROM {table} GROUP BY {column} HAVING c > 1"
                ).fetchall()
                self.assertEqual(duplicates, [], f"{table} 出现副本")

    def test_migrated_rows_keep_required_semantics_and_backfill_optional(self) -> None:
        db = self._bootstrap()
        rows = {row["策略编号"]: row for row in db.list_lifecycle(size=200)[0]}
        self.assertEqual(rows["LIFE-0001"]["表名称"], "ods_order_detail")
        self.assertEqual(rows["LIFE-0001"]["创建日期"], "2025-11-02")
        self.assertEqual(rows["LIFE-0001"]["保留天数"], "365")
        self.assertEqual(rows["LIFE-0002"]["表名称"], "dwd_trade_order")
        self.assertEqual(rows["LIFE-0002"]["status"], "生效中")
        # LIFE-0003 缺最后操作，按补齐规则留空并记录补齐字段，不改已有语义。
        self.assertIsNone(rows["LIFE-0003"]["最后操作"])
        self.assertEqual(rows["LIFE-0003"]["归档路径"], "oss://dw-archive/dws/user_profile/")
        with db._connect() as conn:
            backfilled = json.loads(
                conn.execute(
                    f"SELECT backfilled_fields FROM {LIFECYCLE_TABLE} WHERE policy_no='LIFE-0003'"
                ).fetchone()[0]
            )
        self.assertIn("最后操作", backfilled)

    def test_concurrent_backfill_uses_first_successful_conclusion(self) -> None:
        db = _db(self.tmp)
        db.ensure_schema()
        service = LifecycleService(db)
        legacy, _, _ = load_legacy_bundle(LEGACY_PATH)
        barrier = threading.Barrier(2)
        outcomes: dict[str, str] = {}

        def migrate() -> None:
            barrier.wait()
            db.migrate(lifecycle_rows=legacy, reason="race")

        def act() -> None:
            barrier.wait()
            entry, message = service.run_action(1, "归档表")
            outcomes["message"] = message

        threads = [threading.Thread(target=migrate), threading.Thread(target=act)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertFalse(threads[0].is_alive() or threads[1].is_alive())

        entry = service.get_entry(1)
        self.assertIsNotNone(entry)
        # 谁先成功落库谁定稿，另一方不得覆盖，也不得产生副本。
        self.assertIn(entry["status"], {"已到期", "生效中"})
        self.assertTrue(outcomes.get("message"))
        with db._connect() as conn:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {LIFECYCLE_TABLE} WHERE policy_no='LIFE-0001'"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_stale_action_loses_to_first_committed_conclusion(self) -> None:
        db = self._bootstrap()
        service = LifecycleService(db)
        stale = db.get_lifecycle(1)  # 直接读库拿到 revision=0
        service.run_action(1, "归档表")  # 先落库：已到期 -> 生效中
        revision = stale["_revision"]
        updated, applied = db.apply_lifecycle_action(
            1,
            revision,
            action="删除表",
            target_status="已到期",
            pending=True,
            abnormal=False,
        )
        self.assertFalse(applied)
        self.assertEqual(updated["status"], "生效中")
        self.assertEqual(service.get_entry(1)["status"], "生效中")

    def test_failed_migration_keeps_original_and_reports_location(self) -> None:
        db = self._bootstrap()
        before = db.list_lifecycle(size=200)[1]
        bad_rows = [{"策略编号": ""}]
        with self.assertRaises(Exception) as ctx:
            db.migrate(lifecycle_rows=bad_rows, reason="broken")
        payload = ctx.exception.as_dict()
        self.assertFalse(payload["ok"])
        self.assertTrue(Path(payload["recoverable_location"]).exists())
        # 快照本身可独立打开，且保存的是失败前的完整台账。
        with sqlite3.connect(payload["recoverable_location"]) as restored:
            self.assertEqual(
                restored.execute(f"SELECT COUNT(*) FROM {LIFECYCLE_TABLE}").fetchone()[0], 3
            )
        self.assertEqual(db.list_lifecycle(size=200)[1], before)
        policies = {row["策略编号"]: row["status"] for row in db.list_lifecycle(size=200)[0]}
        self.assertEqual(policies["LIFE-0001"], "已到期")

    def test_audit_explains_migration_before_and_after(self) -> None:
        db = self._bootstrap()
        audits = {
            row["记录编号"]: row
            for row in db.list_audit(size=200)[0]
            if row["变更类型"] == "数据迁移"
        }
        diff = audits["AUDI-MIG-LIFE-0002"]
        before = json.loads(diff["变更前值"])
        after = json.loads(diff["变更后值"])
        self.assertNotIn("归档路径", before)
        self.assertIn("归档路径", after)
        self.assertIn("归档路径", after["补齐字段"])
        self.assertEqual(after["status"], "生效中")
        self.assertEqual(audits["AUDI-MIG-LIFE-0001"]["复核状态"], "已记录")

    def test_recycle_and_audit_actions_survive_restart(self) -> None:
        db = self._bootstrap()
        RecycleService(db).run_action(1, "延期保留")
        AuditService(db).run_action(1, "归档记录")
        reopened = _db(self.tmp)
        self.assertEqual(RecycleService(reopened).get_entry(1)["status"], "已过期")
        self.assertEqual(AuditService(reopened).get_entry(1)["status"], "已复核")

    def test_duplicate_policy_number_is_ignored(self) -> None:
        db = self._bootstrap()
        _, missing, conflict = LifecycleService(db).create_entry(
            {"策略编号": "LIFE-0001", "表名称": "重复登记", "创建日期": "2026-01-01"}
        )
        self.assertEqual(missing, [])
        self.assertIsNotNone(conflict)
        self.assertEqual(db.list_lifecycle(size=200)[1], 3)

    def test_duplicate_recycle_record_returns_first_entry(self) -> None:
        db = self._bootstrap()
        entry, missing, conflict = RecycleService(db).create_entry(
            {"记录编号": "RECY-0001", "表名称": "重复登记", "删除时间": "2026-01-01"}
        )
        self.assertEqual(missing, [])
        self.assertIsNotNone(conflict)
        self.assertEqual(entry["表名称"], "dwd_trade_order_202509")
        self.assertEqual(db.list_recycle(size=200)[1], 4)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
