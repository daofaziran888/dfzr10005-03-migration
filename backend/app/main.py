"""数仓治理与运维平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health

生命周期相关的三张台账（策略、回收记录、变更审计）使用本地 SQLite 持久化，
启动时会幂等回填本地开发样例；迁移失败不会覆盖原库，错误详情从 /api/health 可见。
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.config import settings
from app.persistence import database
from app.persistence.config import PersistenceSettings
from app.persistence.db import MigrationError, MigrationReport
from app.persistence.state import MIGRATION_STATE
from app.routers import ROUTERS
from app.store import store

logger = logging.getLogger("app.main")


def _run_bootstrap() -> None:
    cfg = PersistenceSettings.from_env()
    try:
        database.ensure_schema()
        if not cfg.auto_migrate:
            MIGRATION_STATE.update(status="skipped", db_path=database.db_path)
            return
        report: MigrationReport = database.bootstrap_from_seed()
        MIGRATION_STATE.update(status="succeeded", **report.as_dict())
        logger.info(
            "生命周期台账迁移完成：inserted=%s skipped=%s backup=%s",
            report.inserted,
            report.skipped,
            report.backup_path,
        )
    except MigrationError as exc:
        MIGRATION_STATE.update(status="failed", **exc.as_dict())
        logger.error("生命周期台账迁移失败：%s", exc)
    except Exception as exc:  # 未预期错误同样保留原库并记录可恢复位置
        MIGRATION_STATE.update(
            status="failed",
            ok=False,
            message=f"生命周期台账迁移失败：{exc}",
            db_path=database.db_path,
            recoverable_location=database.db_path,
        )
        logger.exception("生命周期台账迁移失败")


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    _run_bootstrap()
    yield


app = FastAPI(title="数仓治理与运维平台", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

for module in ROUTERS:
    app.include_router(module.router)


@app.get("/api/health")
def health():
    """健康检查：确认服务已经监听、示例数据已经就绪、迁移结果可追溯。

    迁移失败时返回 503，让编排/探针在切换前拦下这一次发布；原库与快照仍然保留。
    """
    payload: dict[str, object] = {
        "ok": True,
        "app": settings.app_name,
        "modules": len(store.module_names()),
        "storage": {
            "engine": "sqlite",
            "db_path": database.db_path,
            "migration": MIGRATION_STATE.get("status"),
            "recoverable_location": MIGRATION_STATE.get("recoverable_location"),
        },
    }
    if MIGRATION_STATE.get("status") == "failed":
        payload["ok"] = False
        payload["message"] = MIGRATION_STATE.get("message")
        return JSONResponse(status_code=503, content=payload)
    return payload


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。

    生命周期三张台账的统计以持久化库为准，其余模块仍读取内存样例。
    """
    try:
        persisted = database.counts()
    except Exception:  # 库不可读时不阻断看板
        persisted = {}
    return store.overview(overrides=persisted)
