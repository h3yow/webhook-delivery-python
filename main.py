import uvicorn

from app.main import create_app

# 默认即可真实跑通一次提交与投递：
# - 状态持久化到 ./events.db，重启后目标与未完成事件自动恢复；
# - 预置目标 "default" 指向内置模拟接收端（/_receiver/recv），
#   提交事件时 receiver 填 "default" 即可投递成功。
app = create_app(
    db_path="events.db",
    default_targets={"default": "http://127.0.0.1:8000/_receiver/recv"},
)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
