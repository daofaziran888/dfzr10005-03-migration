"""表生命周期管理业务规则：状态流转、字段校验与筛选口径都收在这里。

策略状态保存在本地 SQLite 台账里，服务重启后仍能读到上一次动作的结论；
动作写入用 revision 乐观锁，并发回填或并发动作以首次成功落库的结论为准。
"""
from __future__ import annotations

from typing import Any

from app.persistence import database

MODULE = "lifecycle"
REQUIRED_FIELDS = ["策略编号", "表名称", "创建日期"]
STATUS_ORDER = ["草稿", "生效中", "已到期", "已归档"]
ACTION_RULES = {"启用策略": "草稿", "归档表": "生效中", "删除表": "已到期"}
NEGATIVE_ACTIONS: list[str] = []


def _public(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if entry is None:
        return None
    entry.pop("_revision", None)
    return entry


class LifecycleService:
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
        rows, total = self.db.list_lifecycle(
            keyword=keyword, status=status, page=page, size=size
        )
        return [_public(row) for row in rows], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return _public(self.db.get_lifecycle(entry_id))

    def create_entry(
        self, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, list[str], str | None]:
        """登记策略。返回（记录, 缺失字段, 冲突说明）；冲突时以首次落库为准，不覆盖。"""
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing, None
        try:
            entry = self.db.create_lifecycle(values)
        except ValueError as exc:
            existing = self.db.get_lifecycle_by_policy_no(str(values.get("策略编号")).strip())
            return _public(existing), [], str(exc)
        return _public(entry), [], None

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.db.get_lifecycle(entry_id)
        if entry is None:
            return None, f"生命周期策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于表生命周期管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        revision = int(entry.get("_revision", 0))
        try:
            updated, applied = self.db.apply_lifecycle_action(
                entry_id,
                revision,
                action=action,
                target_status=target,
                pending=target != STATUS_ORDER[-1],
                abnormal=action in NEGATIVE_ACTIONS,
            )
        except LookupError:
            return None, f"生命周期策略 {entry_id} 不存在或已归档"
        if not applied:
            return _public(updated), (
                f"动作「{action}」与已落库结论冲突，以首次成功落库的状态「{updated['status']}」为准"
            )
        return _public(updated), f"生命周期策略已{action}"
