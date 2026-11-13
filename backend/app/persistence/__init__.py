"""本地持久化层：SQLite 文件库、迁移与核对工具。"""
from __future__ import annotations

from app.persistence.db import LifecycleDatabase, database

__all__ = ["LifecycleDatabase", "database"]
