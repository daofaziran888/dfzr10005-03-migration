# 生命周期本地数据目录

| 路径 | 用途 | 是否入库 |
| --- | --- | --- |
| `legacy/lifecycle-history.json` | 旧环境导出的历史策略/回收/审计快照，启动时优先回填 | 是 |
| `lifecycle.sqlite3` | 运行期台账库（策略、回收记录、变更审计） | 否（.gitignore） |
| `backups/` | 迁移与切换前的 SQLite 快照，失败时作为可恢复位置 | 否（.gitignore） |

回填顺序固定为“历史快照 → 本地开发样例”，同一策略编号/记录编号只保留首次成功落库的
记录，重复执行不会产生副本。切换前先跑 `make preflight`，上线前再跑 `make snapshot`。
