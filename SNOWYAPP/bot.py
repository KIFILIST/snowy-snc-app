import os
import re
import json
import asyncio
import logging
import asyncpg
import uvicorn
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    WebAppInfo, 
    LabeledPrice, 
    PreCheckoutQuery,
    BotCommand,
    MenuButtonWebApp
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SnowySNC")

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"

class TaskItem(BaseModel):
    id: int
    title: str
    reward: int
    done: bool

class UserData(BaseModel):
    username: str
    balance: int
    tasks: List[TaskItem]

class LeaderRow(BaseModel):
    username: str
    balance: int

class DB:
    def __init__(self, url: str):
        self.url = url
        self.pool = None

    async def connect(self):
        for i in range(1, 11):
            try:
                self.pool = await asyncpg.create_pool(
                    self.url,
                    min_size=5,
                    max_size=20,
                    command_timeout=60
                )
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            username TEXT PRIMARY KEY,
                            balance BIGINT DEFAULT 0,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                logger.info("DATABASE CONNECTED")
                return True
            except Exception as e:
                logger.error(f"DB ATTEMPT {i} FAILED: {e}")
                await asyncio.sleep(5)
        return False

    async def get_bal(self, user: str) -> int:
        async with self.pool.acquire() as conn:
            res = await conn.fetchval("SELECT balance FROM users WHERE username = $1", user.lower())
            if res is None:
                await conn.execute("INSERT INTO users (username, balance) VALUES ($1, 0)", user.lower())
                return 0
            return res

    async def add_bal(self, user: str, amt: int) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                INSERT INTO users (username, balance) VALUES ($1, $2)
                ON CONFLICT (username) DO UPDATE SET balance = users.balance + $2
                RETURNING balance
            """, user.lower(), amt)

    async def top(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT 50")
            return [dict(r) for r in rows]

db = DB(DATABASE_URL)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❄️ Открыть Snowy App", web_app=WebAppInfo(url=WEBAPP_URL))]],
        resize_keyboard=True
    )

@dp.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(q.id, ok=True)

@dp.message(F.successful_payment)
async def success_pay(m: types.Message):
    pay = m.successful_payment.invoice_payload
    if pay.startswith("buy:"):
        _, target, amt = pay.split(":")
        nb = await db.add_bal(target, int(amt))
        await m.answer(f"✅ Успешно! +{amt} SNC. Баланс: {nb} ❄️")

@dp.message(F.web_app_data)
async def web_data(m: types.Message):
    try:
        data = json.loads(m.web_app_data.data)
        if data.get("type") == "transfer":
            u = (m.from_user.username or f"id{m.from_user.id}").lower()
            t = str(data.get("target")).lower().replace("@", "").strip()
            v = int(data.get("amount"))
            if v <= 0 or u == t: return
            if await db.get_bal(u) < v:
                await m.answer("❌ Мало SNC.")
                return
            async with db.pool.acquire() as conn:
                if not await conn.fetchval("SELECT 1 FROM users WHERE username = $1", t):
                    await m.answer(f"❌ Игрок {t} не в базе.")
                    return
                await db.add_bal(u, -v)
                await db.add_bal(t, v)
                await m.answer(f"✅ Перевод {v} для {t} выполнен ❄️")
    except Exception as e:
        logger.error(e)

@dp.message(Command("start"))
async def start(m: types.Message):
    u = (m.from_user.username or f"id{m.from_user.id}").lower()
    b = await db.get_bal(u)
    await m.answer(f"❄️ Привет, {u}!\nБаланс: {b} SNC", reply_markup=main_kb())

@dp.message(F.chat.type == "private")
async def admin_msg(m: types.Message):
    u = (m.from_user.username or f"id{m.from_user.id}").lower()
    if m.from_user.id in ADMIN_IDS:
        match = re.match(r"^@(\w+)\s+([+-]?\d+)$", m.text or "")
        if match:
            target, amt = match.group(1).lower(), int(match.group(2))
            res = await db.add_bal(target, amt)
            await m.answer(f"⚙️ {target}: {res} SNC")
            return
    await m.answer(f"❄️ Баланс: {await db.get_bal(u)} SNC", reply_markup=main_kb())

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not await db.connect():
        logger.critical("DB FAILED")
    await bot.set_my_commands([BotCommand(command="start", description="Меню")])
    await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Snowy App", web_app=WebAppInfo(url=WEBAPP_URL)))
    await bot.delete_webhook(drop_pending_updates=True)
    task = asyncio.create_task(dp.start_polling(bot))
    yield
    task.cancel()
    if db.pool: await db.pool.close()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def health():
    return {"status": "ok", "db": db.pool is not None}

@app.get("/api/user/{u}", response_model=UserData)
async def api_u(u: str):
    b = await db.get_bal(u.lower())
    return {"username": u, "balance": b, "tasks": [{"id":1,"title":"Группа","reward":100,"done":False}]}

@app.get("/api/leaderboard", response_model=List[LeaderRow])
async def api_top():
    return await db.top()

@app.get("/api/buy_snc/{u}/{snc}/{stars}")
async def api_pay(u: str, snc: int, stars: int):
    url = await bot.create_invoice_link(title=f"{snc} SNC", description="Buy", payload=f"buy:{u.lower()}:{snc}", provider_token="", currency="XTR", prices=[LabeledPrice(label="SNC", amount=int(stars))])
    return {"invoice_url": url}

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))