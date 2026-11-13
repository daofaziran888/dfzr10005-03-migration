# 数仓治理与运维平台

面向数据仓库团队的治理与运维一体化平台。覆盖表生命周期管理、存储成本优化、数据热度分析、任务依赖编排、SLA告警、资源弹性调度与数仓健康度评估的完整治理链路。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   ├── app/persistence/      生命周期台账的本地 SQLite 持久化与迁移
│   ├── app/store.py          其余模块的内存数据仓库与示例数据
│   ├── data/legacy/          旧环境导出的生命周期快照（迁移输入）
│   └── tests/                生命周期持久化回归测试
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 表生命周期管理 | `lifecycle` | 生命周期策略 | 策略编号、表名称、创建日期 |
| 存储成本优化 | `cost` | 成本分析报告 | 报告编号、分析范围、存储总量 |
| 数据热度分析 | `heatmap` | 热度记录 | 记录编号、表名称、查询次数 |
| 任务依赖编排 | `dag` | 依赖图节点 | 节点编号、节点名称、上游节点 |
| SLA告警管理 | `sla` | SLA规则 | 规则编号、监控对象、产出时间 |
| 资源弹性调度 | `resource` | 资源池配置 | 配置编号、资源类型、最小配额 |
| 数仓健康度评估 | `score` | 健康度报告 | 报告编号、评估范围、存储健康分 |
| 数据漂移检测 | `drift` | 漂移记录 | 记录编号、表名称、漂移字段 |
| 数据资产盘点 | `catalog` | 资产记录 | 资产编号、表名称、资产分类 |
| 数据保留策略 | `retention` | 保留规则 | 规则编号、数据集范围、保留期限 |
| 数据去重管理 | `dedup` | 去重任务 | 任务编号、目标表名、去重字段 |
| 数据分层规划 | `tier` | 分层定义 | 分层编号、分层名称、层级别名 |
| 基线监控管理 | `baseline` | 基线配置 | 基线编号、基线名称、承诺时间 |
| 数据回收站 | `recycle` | 回收记录 | 记录编号、表名称、删除时间 |
| 变更审计追踪 | `audit` | 变更记录 | 记录编号、变更对象、变更类型 |
| 查询性能分析 | `benchmark` | 性能报告 | 报告编号、查询语句、执行耗时 |

## 生命周期台账的持久化

表生命周期管理、数据回收站、变更审计追踪三个模块的状态不再只放在进程内存里，
统一落到本地 SQLite 文件（默认 `backend/data/lifecycle.sqlite3`），服务重启后
策略状态、待处理标记、最后动作以及回收记录/审计流水都能恢复。

存储与迁移口径：

- 三张表按业务编号唯一：`life_lifecycle_policy.policy_no`（策略编号）、
  `life_recycle_record.record_no`、`life_change_audit.record_no`。
  策略编号即表名口径（例如 `LIFE-0001`），迁移时原样保留。
- 迁移在单个 `BEGIN IMMEDIATE` 事务内完成；重复执行按唯一键跳过，不产生副本。
- 旧记录缺失的可选字段（保留天数、归档路径、清理方式、最后操作）按空值补齐，
  补齐项记入 `backfilled_fields`；已有值一律不覆盖。
- 运行期动作与存量回填并发时，动作更新带 `revision` 乐观锁、回填用唯一键去重，
  以首次成功落库的结论为准，后到的一方拿到的是已落库结果而非覆盖写入。
- 迁移前用 SQLite 在线备份生成快照；事务失败会回滚并在错误里给出
  `recoverable_location`（快照路径或原库路径），`/api/health` 同时暴露该状态。
- 成功迁移后追加两条可解释的数据：一条 `RECY-MIG-0001` 回收记录保存当前状态快照，
  每条策略一条 `AUDI-MIG-<策略编号>` 审计记录保存迁移前后差异。
- 启动顺序固定为“先历史快照、再本地开发样例”，由 `LIFECYCLE_LEGACY_SEED` 指定历史文件。

相关环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `LIFECYCLE_DB_PATH` | `backend/data/lifecycle.sqlite3` | 台账库文件 |
| `LIFECYCLE_BACKUP_DIR` | `backend/data/backups` | 迁移/切换前快照目录 |
| `LIFECYCLE_LEGACY_SEED` | `backend/data/legacy/lifecycle-history.json` | 旧环境历史快照 |
| `LIFECYCLE_AUTO_MIGRATE` | `1` | 启动时是否自动回填，设为 `0` 可关闭 |

运维命令（在仓库根目录执行）：

```bash
make migrate     # 按部署顺序回填历史记录与本地开发样例（幂等）
make verify      # 核对 LIFE-0001..0003、RECY-0001、AUDI-0001 与唯一性
make snapshot    # 切换前生成数据库快照
make test        # 生命周期持久化回归测试
make preflight   # 切换前演练：依赖锁定、镜像构建、部署演练、样例与历史核对
```

`make preflight` 会先检查依赖锁文件、尝试构建镜像（没有容器运行时时如实记为
`skipped` 而不是假成功）、在临时库里连续迁移两次、核对三类记录，再用两个进程
验证动作结论跨重启保留。演练只写临时目录，不会触碰正式台账。

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
