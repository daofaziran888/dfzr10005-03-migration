"""数据回收站业务规则：状态流转、字段校验与筛选口径都收在这里。

回收记录存放在本地 SQLite 台账，迁移快照与动作结论都能在重启后继续解释。
"""
from __future__ import annotations

from typing import Any

from app.persistence import database

MODULE = "recycle"
REQUIRED_FIELDS = ["记录编号", "表名称", "删除时间"]
STATUS_ORDER = ["待恢复", "已恢复", "已清理", "已过期"]
ACTION_RULES = {"恢复数据": "待恢复", "永久清理": "已恢复", "延期保留": "已过期"}
NEGATIVE_ACTIONS: list[str] = []


def _public(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if entry is None:
        return None
    entry.pop("_revision", None)
    return entry


class RecycleService:
    def __init__(self, db=None) -> None:
        self.db = db or database

    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows, total = self.db.list_recycle(keyword=keyword, status=status, page=page, size=size)
        return [_public(row) for row in rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return _public(self.db.get_recycle(entry_id))

    def create_entry(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, list[str], str | None]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing, None
        try:
            entry = self.db.create_recycle(values)
        except ValueError as exc:
            return _public(self.db.get_recycle_by_record_no(str(values.get("记录编号")).strip())), [], str(exc)
        return _public(entry), [], None

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.db.get_recycle(entry_id)
        if entry is None:
            return None, f"回收记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据回收站可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        revision = int(entry.get("_revision", 0))
        try:
            updated, applied = self.db.apply_recycle_action(
                entry_id,
                revision,
                target_status=target,
                pending=target != STATUS_ORDER[-1],
                abnormal=action in NEGATIVE_ACTIONS,
            )
        except LookupError:
            return None, f"回收记录 {entry_id} 不存在或已归档"
        if not applied:
            return _public(updated), (
                f"动作「{action}」与已落库结论冲突，以首次成功落库的状态「{updated['status']}」为准"
            )
        return _public(updated), f"回收记录已{action}"
