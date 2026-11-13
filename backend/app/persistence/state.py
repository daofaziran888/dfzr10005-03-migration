"""运行期迁移状态：由服务启动流程写入，接口层只读展示。"""
from __future__ import annotations

from typing import Any

MIGRATION_STATE: dict[str, Any] = {"status": "pending"}
