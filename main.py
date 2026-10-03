import os

import uvicorn

from app.main import create_app

# 默认启动即“开箱可投递”：
# - 内置模拟接收端挂载在同一进程的 /_receiver/recv（127.0.0.1:8000）；
# - 预置具名目标 default -> 该地址，事件声明 receiver="default" 即真实投递；
# - SQLite 使用本地文件，重启后目标登记与未完成事件都在。
_BASE = os.environ.get("WEBHOOK_BASE_URL", "http://127.0.0.1:8000")
_DB = os.environ.get("WEBHOOK_DB", "data/webhook.db")

app = create_app(db_path=_DB,
                 targets={"default": f"{_BASE}/_receiver/recv"})

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
