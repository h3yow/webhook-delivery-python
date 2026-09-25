import uvicorn

from app.api import create_app

app = create_app(db_path="webhook_delivery.db")


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
