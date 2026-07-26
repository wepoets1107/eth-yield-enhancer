"""
ETH 收益增强策略 - 进程守护 (supervisor)

职责：
- 每 CHECK_INTERVAL 秒检查 5052 端口是否存活
- 端口死：pythonw 重拉 app.py（进程崩溃自愈）
- 端口活但实例 status != running：自动 POST /api/start 触发策略
- 自身日志写入 logs/supervisor.log (滚动, 2MB×3)

用法：
    Start-Process -WindowStyle Hidden pythonw.exe supervisor.py
或通过 .bat 启动（登录自启时放「启动」文件夹）。

说明：
- pythonw 下 stdout 是黑洞, supervisor 自身日志只落盘, 不影响。
- supervisor 用 DETACHED_PROCESS 拉起 app.py, 即使 supervisor 退出, app.py 仍独立存活。
- supervisor 本身崩溃不会自愈(极简守护, 崩溃概率低); 如需更高可用可外层再包一层。
"""

import os
import sys
import json
import time
import socket
import subprocess
import logging
import logging.handlers
import urllib.request
import urllib.error

WORK_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHONW = r"C:\Users\张无忌\.workbuddy\binaries\python\envs\default\Scripts\pythonw.exe"
HOST, PORT = "127.0.0.1", 5052
CHECK_INTERVAL = 10
START_GRACE = 20
STATUS_URL = f"http://{HOST}:{PORT}/api/status"
START_URL = f"http://{HOST}:{PORT}/api/start"


def _setup_logging():
    log_dir = os.path.join(WORK_DIR, "logs")
    os.makedirs(log_dir, exist_ok=True)
    fh = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "supervisor.log"),
        maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8",
    )
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] supervisor: %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not any(isinstance(h, logging.handlers.RotatingFileHandler) for h in root.handlers):
        root.addHandler(fh)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root.handlers):
        root.addHandler(logging.StreamHandler(sys.stdout))


def port_alive():
    try:
        with socket.create_connection((HOST, PORT), timeout=3):
            return True
    except OSError:
        return False


def get_status():
    """返回 status dict; 连不上返回 None; 503(引擎未初始化)也解析 body 返回。"""
    try:
        with urllib.request.urlopen(STATUS_URL, timeout=4) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        # 端口活但引擎未初始化(503) 仍视为"有响应", 解析 body 拿 status
        try:
            return json.loads(e.read().decode("utf-8"))
        except (ValueError, OSError):
            return None
    except (urllib.error.URLError, OSError, ValueError):
        return None


def post_start():
    try:
        req = urllib.request.Request(START_URL, method="POST", data=b"")
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status
    except (urllib.error.URLError, OSError):
        return None


def launch():
    logging.info("重拉 ETH 实例: pythonw app.py --port 5052 --symbol ETH_USDC")
    try:
        subprocess.Popen(
            [PYTHONW, "app.py", "--port", "5052", "--symbol", "ETH_USDC"],
            cwd=WORK_DIR,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        logging.info("已发起 app.py 启动")
    except Exception as e:
        logging.error("启动 app.py 失败: %s", e)


def main():
    _setup_logging()
    logging.info("=== ETH supervisor 启动, 监控端口 %d ===", PORT)
    first = True
    while True:
        try:
            if not port_alive():
                logging.warning("端口 %d 未监听, 确认后重拉", PORT)
                time.sleep(5)
                if not port_alive():
                    launch()
                    time.sleep(START_GRACE)
                    # 重拉后引擎必为 stopped, 直接触发 start (幂等安全)
                    logging.info("实例已重拉, 触发 POST /api/start")
                    post_start()
                else:
                    logging.info("端口已恢复(可能是重启抖动), 不动作")
            else:
                st = get_status()
                if st is None:
                    logging.warning("端口活但 /api/status 无响应, 下一轮再查")
                elif st.get("status") != "running":
                    logging.warning("实例 status=%s, 触发 POST /api/start", st.get("status"))
                    post_start()
                elif first:
                    logging.info("实例已在运行 (status=running)")
            first = False
        except Exception as e:
            logging.error("supervisor 主循环异常: %s", e)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("supervisor 收到中断, 退出")
