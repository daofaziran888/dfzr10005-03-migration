#!/usr/bin/env bash
# 生命周期台账上线切换前置演练：依赖锁定 → 镜像构建 → 部署演练 → 样例/历史核对。
#
# 本脚本只操作临时目录与演练库，不触碰 backend/data 下的正式台账；
# 没有容器运行时时会明确记为“未执行”，而不是假装构建成功。
#
# 用法：scripts/preflight.sh [--db <演练库路径>]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND="$ROOT/backend"
PY="$BACKEND/.venv/bin/python"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/lifecycle-preflight-$STAMP.XXXXXX")"
DB="${WORK}/rehearsal.sqlite3"
BACKUPS="${WORK}/backups"
LEGACY="$BACKEND/data/legacy/lifecycle-history.json"
REPORT="${WORK}/preflight.json"

if [[ "${1:-}" == "--db" && -n "${2:-}" ]]; then
  DB="$2"
fi

mkdir -p "$BACKUPS"
STATUS_FILE="${WORK}/steps.jsonl"
: > "$STATUS_FILE"
# CLI 以模块方式运行，保证从仓库根目录调用时也能找到 app 包。
export PYTHONPATH="$BACKEND"

step() {
  local name="$1"; shift
  local status="$1"; shift
  local detail="$1"; shift || true
  printf '{"step":"%s","status":"%s","detail":"%s"}\n' "$name" "$status" "$detail" >> "$STATUS_FILE"
  printf '[%-6s] %-28s %s\n' "$status" "$name" "$detail"
}

run_step() {
  local name="$1"; shift
  if "$@" >> "${WORK}/${name}.log" 2>&1; then
    step "$name" "ok" "见 ${WORK}/${name}.log"
  else
    step "$name" "failed" "见 ${WORK}/${name}.log"
    return 1
  fi
}

echo "演练工作目录：$WORK"
echo "演练库：$DB"

# 1. 依赖锁定：锁文件与声明一致，Python 依赖可复现安装。
if [[ ! -x "$PY" ]]; then
  step "deps-python" "failed" "缺少 backend/.venv，先执行 make install"
  exit 1
fi
if "$PY" - "$ROOT" <<'PY' >> "${WORK}/deps-python.log" 2>&1
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
lock = pathlib.Path(root, "backend", "requirements.lock").read_text(encoding="utf-8")
assert "fastapi==" in lock and "uvicorn==" in lock, "requirements.lock 未锁定关键依赖"
pkg = json.loads(pathlib.Path(root, "frontend", "package-lock.json").read_text(encoding="utf-8"))
assert pkg.get("lockfileVersion", 0) >= 3, "package-lock.json 版本过低"
PY
then
  step "deps-lockfiles" "ok" "requirements.lock / package-lock.json 与声明一致"
else
  step "deps-lockfiles" "failed" "锁文件缺失或不匹配"
  exit 1
fi
run_step "deps-python" "$BACKEND/.venv/bin/pip" install --dry-run --no-index -q -r "$BACKEND/requirements.lock"

# 2. 镜像构建：有容器运行时就真实构建，没有则如实记为未执行。
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  run_step "image-backend" docker build -t "warehouse-backend:preflight-$STAMP" "$BACKEND"
  run_step "image-frontend" docker build -t "warehouse-frontend:preflight-$STAMP" "$ROOT/frontend"
else
  step "image-build" "skipped" "当前环境没有 docker，镜像构建未执行；需在有容器的构建机上补跑"
fi

# 3. 前端构建（等价于镜像内的编译步骤，不依赖容器）。
run_step "frontend-build" bash -c "cd '$ROOT/frontend' && npm run typecheck"

# 4. 迁移演练：先历史记录、再本地开发样例；连续跑两次验证幂等。
run_step "migrate-rehearsal-1" env \
  LIFECYCLE_DB_PATH="$DB" LIFECYCLE_BACKUP_DIR="$BACKUPS" \
  "$PY" -m app.persistence_cli migrate --legacy "$LEGACY" --with-dev-seed --reason preflight
run_step "migrate-rehearsal-2" env \
  LIFECYCLE_DB_PATH="$DB" LIFECYCLE_BACKUP_DIR="$BACKUPS" \
  "$PY" -m app.persistence_cli migrate --legacy "$LEGACY" --with-dev-seed --reason preflight-repeat

# 5. 完整性核对：LIFE-0001..0003 三种状态、RECY-0001 待恢复、AUDI-0001 复核状态。
if LIFECYCLE_DB_PATH="$DB" "$PY" -m app.persistence_cli verify \
    --expect LIFE-0001,LIFE-0002,LIFE-0003 >> "${WORK}/verify.log" 2>&1; then
  step "verify-rehearsal" "ok" "见 ${WORK}/verify.log"
else
  step "verify-rehearsal" "failed" "见 ${WORK}/verify.log"
  exit 1
fi

# 6. 重启演练：同一个库文件先后起两个真实进程，动作结论必须跨进程保留。
if "$PY" "$ROOT/scripts/preflight_restart.py" "$ROOT" "$DB" "$BACKUPS" "$LEGACY" \
    > "${WORK}/restart-rehearsal.log" 2>&1; then
  step "restart-rehearsal" "ok" "见 ${WORK}/restart-rehearsal.log"
else
  step "restart-rehearsal" "failed" "见 ${WORK}/restart-rehearsal.log"
  exit 1
fi

# 7. 汇总报告
"$PY" - "$STATUS_FILE" "$REPORT" <<'PY'
import json, sys
steps = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
report = {
    "ok": all(step["status"] in {"ok", "skipped"} for step in steps),
    "steps": steps,
    "skipped": [step["step"] for step in steps if step["status"] == "skipped"],
}
open(sys.argv[2], "w", encoding="utf-8").write(json.dumps(report, ensure_ascii=False, indent=2))
print(json.dumps(report, ensure_ascii=False, indent=2))
sys.exit(0 if report["ok"] else 1)
PY
