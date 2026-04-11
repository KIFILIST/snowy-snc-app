import os
import re
import json
import asyncio
import asyncpg
import uvicorn
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, balance INTEGER)")

async def check_user(username: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
        if row is None:
            await conn.execute("INSERT INTO users (username, balance) VALUES ($1, $2)", username, 0)
            return 0
        return row['balance']

async def update_balance(username: str, amount: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE username = $2", amount, username)
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
        return row['balance'] if row else 0

def get_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text="🚀 Открыть SNC App", web_app=WebAppInfo(url=WEBAPP_URL))
    ]], resize_keyboard=True)

def get_username(user: types.User):
    return user.username.lower() if user.username else f"user_{user.id}"

@dp.message(F.web_app_data)
async def web_app_handler(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        return

    if data.get("type") == "transfer":
        sender = get_username(message.from_user)
        target = str(data.get("target")).lower().replace("@", "")
        
        try:
            amount = int(data.get("amount"))
        except ValueError:
            await message.answer("❌ Неверный формат суммы.")
            return

        if amount <= 0 or sender == target:
            return

        sender_balance = await check_user(sender)
        if sender_balance < amount:
            await message.answer("❌ Недостаточно SNC на балансе.")
            return

        async with db_pool.acquire() as conn:
            target_row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", target)

        if target_row is None:
            await message.answer(f"❌ Пользователь *{target}* не найден.", parse_mode="Markdown")
            return

        await update_balance(sender, -amount)
        await update_balance(target, amount)
        await message.answer(f"✅ Успешный перевод!\nОтправлено *{amount}* SNC игроку *{target}*.", parse_mode="Markdown")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    if message.from_user.id in ADMIN_IDS:
        text = "👑 *SNC Admin*\n\nЛС: `@username +100` / `@username -50`\nГруппы: `/addsnc @username 100`"
    else:
        text = f"👋 Привет, *{username}*!\n\nТвой баланс: *{balance}* SNC ❄️"
    await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(Command("mysnc"))
async def cmd_mysnc(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    markup = get_keyboard() if message.chat.type == "private" else None
    await message.answer(f"❄️ У пользователя *{username}* *{balance}* SNC", parse_mode="Markdown", reply_markup=markup)

@dp.message(Command("addsnc"))
async def cmd_addsnc(message: types.Message):
    if message.chat.type != "private" and message.from_user.id in ADMIN_IDS:
        match = re.match(r"^/addsnc\s+@(\w+)\s+([+-]?\d+)$", message.text or "")
        if match:
            target = match.group(1).lower()
            amt = int(match.group(2))
            await check_user(target)
            new_balance = await update_balance(target, amt)
            await message.answer(f"✅ У пользователя *{target}* *{new_balance}* SNC", parse_mode="Markdown")

@dp.message(F.chat.type == "private")
async def handle_private(message: types.Message):
    username = get_username(message.from_user)
    await check_user(username)
    if message.from_user.id in ADMIN_IDS:
        match = re.match(r"^@(\w+)\s+([+-]?\d+)$", message.text or "")
        if match:
            target = match.group(1).lower()
            amt = int(match.group(2))
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", target)
            if row is not None:
                new_balance = await update_balance(target, amt)
                await message.answer(f"✅ У пользователя *{target}* *{new_balance}* SNC", parse_mode="Markdown")
            else:
                await message.answer(f"❌ Пользователь *@{target}* не найден.", parse_mode="Markdown")
            return
    balance = await check_user(username)
    await message.answer(f"❄️ У пользователя *{username}* *{balance}* SNC", parse_mode="Markdown", reply_markup=get_keyboard())

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    polling_task = asyncio.create_task(dp.start_polling(bot))
    yield
    polling_task.cancel()
    if db_pool:
        await db_pool.close()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/user/{username}")
async def api_get_user(username: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
        balance = row['balance'] if row else 0
        return {
            "username": username,
            "balance": balance,
            "tasks": [
                {"id": 1, "title": "Подписаться на Telegram канал", "reward": 50, "done": False},
                {"id": 2, "title": "Пригласить 3 друзей", "reward": 150, "done": False}
            ]
        }

@app.get("/api/leaderboard")
async def api_get_leaderboard():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC")
        return [{"username": row['username'], "balance": row['balance']} for row in rows]

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("bot:app", host="0.0.0.0", port=port)