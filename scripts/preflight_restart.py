"""重启演练辅助脚本：用真实子进程起停两次服务，验证动作结论跨重启保留。

由 scripts/preflight.sh 调用，参数：<项目根> <演练库> <备份目录> <历史快照>。
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def call(port: int, path: str, payload: dict | None = None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())


def start(root: Path, env: dict[str, str]) -> tuple[subprocess.Popen, int]:
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(root / "backend"),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"服务启动失败：{proc.stderr.read() if proc.stderr else ''}")
        try:
            call(port, "/api/health")
            return proc, port
        except (urllib.error.URLError, ConnectionError):
            time.sleep(0.15)
    raise RuntimeError("服务启动超时")


def stop(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def main() -> int:
    root = Path(sys.argv[1]).resolve()
    db, backups, legacy = sys.argv[2], sys.argv[3], sys.argv[4]
    env = {
        **os.environ,
        "PYTHONPATH": str(root / "backend"),
        "LIFECYCLE_DB_PATH": db,
        "LIFECYCLE_BACKUP_DIR": backups,
        "LIFECYCLE_LEGACY_SEED": legacy,
    }

    first_proc, first_port = start(root, env)
    try:
        before = call(first_port, "/api/lifecycle/3")
        action = call(first_port, "/api/lifecycle/3/actions", {"values": {"action": "删除表"}})
        summary_first = {
            "health": call(first_port, "/api/health"),
            "before_action": before,
            "action": action,
        }
    finally:
        stop(first_proc)

    # 真正的新进程：模块全部重新加载，只能从库文件恢复状态。
    second_proc, second_port = start(root, env)
    try:
        summary_second = {
            "health": call(second_port, "/api/health"),
            "lifecycle": call(second_port, "/api/lifecycle?size=10"),
            "recycle": call(second_port, "/api/recycle?size=10"),
            "audit": call(second_port, "/api/audit?size=50"),
        }
    finally:
        stop(second_proc)

    rows = {row["策略编号"]: row for row in summary_second["lifecycle"]["items"]}
    recycle = {row["记录编号"]: row for row in summary_second["recycle"]["items"]}
    audit = {row["记录编号"]: row for row in summary_second["audit"]["items"]}
    problems: list[str] = []
    if rows.get("LIFE-0001", {}).get("status") != "已到期":
        problems.append("LIFE-0001 重启后未保持已到期")
    if len(rows) != 3:
        problems.append(f"生命周期策略数量异常：{len(rows)}")
    if rows.get("LIFE-0003", {}).get("status") != "已到期":
        problems.append("重启前的删除表动作未保留")
    if recycle.get("RECY-0001", {}).get("status") != "待恢复":
        problems.append("RECY-0001 重启后未保持待恢复")
    if audit.get("AUDI-0001", {}).get("status") != "已记录":
        problems.append("AUDI-0001 重启后未保持已记录")
    if not any(key.startswith("AUDI-LIFE-0003-") for key in audit):
        problems.append("动作流水未写入变更审计")

    result = {
        "ok": not problems,
        "problems": problems,
        "before_restart": summary_first,
        "after_restart": {
            "lifecycle": {key: [row["status"], row["pending"], row["最后操作"]] for key, row in rows.items()},
            "recycle": {key: row["status"] for key, row in recycle.items()},
            "audit": {key: [row["变更类型"], row["status"]] for key, row in audit.items()},
        },
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
