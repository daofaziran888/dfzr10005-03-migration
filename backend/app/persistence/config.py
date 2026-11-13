"""生命周期持久化配置：路径、表名、旧样例来源与业务口径。

路径全部可以用环境变量覆盖，方便容器化部署、演练和迁移校验各自使用独立的库文件，
避免演练过程误写正式数据。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# 生命周期策略按“策略编号”作为业务主键；表名沿用策略编号的命名口径，
# 与旧内存样例里的“策略编号”字段一一对应，避免迁移时重新造表。
LIFECYCLE_TABLE = "life_lifecycle_policy"
RECYCLE_TABLE = "life_recycle_record"
AUDIT_TABLE = "life_change_audit"

STATUS_ORDER = ("草稿", "生效中", "已到期", "已归档")
RECYCLE_STATUS_ORDER = ("待恢复", "已恢复", "已清理", "已过期")
AUDIT_STATUS_ORDER = ("已记录", "已复核", "已归档")


@dataclass(frozen=True)
class PersistenceSettings:
    db_path: str
    backup_dir: str
    legacy_seed_path: str | None
    auto_migrate: bool

    @classmethod
    def from_env(cls) -> "PersistenceSettings":
        default_db = Path(__file__).resolve().parents[2] / "data" / "lifecycle.sqlite3"
        backup_dir = Path(
            os.getenv("LIFECYCLE_BACKUP_DIR", str(default_db.parent / "backups"))
        )
        default_legacy = default_db.parent / "legacy" / "lifecycle-history.json"
        env_legacy = os.getenv("LIFECYCLE_LEGACY_SEED")
        if env_legacy is None:
            # 旧环境导出快照就在仓库里，默认参与回填；显式设为空串可关闭。
            legacy_seed = str(default_legacy) if default_legacy.exists() else None
        else:
            legacy_seed = env_legacy or None
        return cls(
            db_path=os.getenv("LIFECYCLE_DB_PATH", str(default_db)),
            backup_dir=str(backup_dir),
            legacy_seed_path=legacy_seed,
            auto_migrate=os.getenv("LIFECYCLE_AUTO_MIGRATE", "1") not in {"0", "false", "False"},
        )


settings = PersistenceSettings.from_env()
