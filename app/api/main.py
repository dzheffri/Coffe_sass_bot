from fastapi import FastAPI
from app.profile_logic import get_user_cups_data

app = FastAPI(title="Coffee Club API")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/users/{telegram_user_id}/cups")
def user_cups(telegram_user_id: int):
    return get_user_cups_data(telegram_user_id)
