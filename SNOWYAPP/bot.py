import os
import re
import json
import asyncio
import asyncpg
import uvicorn
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, LabeledPrice, PreCheckoutQuery

load_dotenv()

logging.basicConfig(level=logging.INFO)
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, balance INTEGER DEFAULT 0)")

async def check_user(username: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
        if row is None:
            await conn.execute("INSERT INTO users (username, balance) VALUES ($1, 0)", username)
            return 0
        return row['balance']

async def update_balance(username: str, amount: int):
    async with db_pool.acquire() as conn:
        await conn.execute("INSERT INTO users (username, balance) VALUES ($1, $2) ON CONFLICT (username) DO UPDATE SET balance = users.balance + $2", username, amount)
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
        return row['balance'] if row else 0

def get_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text="🚀 Открыть SNC App", web_app=WebAppInfo(url=WEBAPP_URL))
    ]], resize_keyboard=True)

def get_username(user: types.User):
    return user.username.lower() if user.username else f"user_{user.id}"

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_q: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("buy_snc:"):
        _, target_user, snc_amount = payload.split(":")
        new_balance = await update_balance(target_user, int(snc_amount))
        await message.answer(f"🎉 *Оплата успешна!*\nНачислено: *{snc_amount} SNC*\nВаш баланс: *{new_balance} SNC*", parse_mode="Markdown")

@dp.message(F.web_app_data)
async def web_app_handler(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
    except Exception:
        return

    if data.get("type") == "transfer":
        sender = get_username(message.from_user)
        target = str(data.get("target")).lower().replace("@", "")
        amount = int(data.get("amount"))

        if amount <= 0 or sender == target: return

        if await check_user(sender) < amount:
            await message.answer("❌ Недостаточно SNC.")
            return

        async with db_pool.acquire() as conn:
            if await conn.fetchval("SELECT 1 FROM users WHERE username = $1", target):
                await update_balance(sender, -amount)
                await update_balance(target, amount)
                await message.answer(f"✅ Переведено *{amount} SNC* игроку *{target}*.", parse_mode="Markdown")
            else:
                await message.answer(f"❌ Пользователь *{target}* не найден.")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    text = f"👋 Привет, *{username}*!\n\nТвой баланс: *{balance}* SNC ❄️"
    if message.from_user.id in ADMIN_IDS:
        text += "\n\n👑 *Admin*\nЛС: `@user +100` / `@user -50`\nГруппы: `/addsnc @user 100`"
    await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(Command("addsnc"))
async def cmd_addsnc(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        match = re.search(r"@(\w+)\s+([+-]?\d+)", message.text)
        if match:
            target, amt = match.group(1).lower(), int(match.group(2))
            new_bal = await update_balance(target, amt)
            await message.answer(f"✅ *{target}*: {new_bal} SNC")

@dp.message(F.chat.type == "private")
async def handle_private_admin(message: types.Message):
    username = get_username(message.from_user)
    await check_user(username)
    if message.from_user.id in ADMIN_IDS:
        match = re.match(r"^@(\w+)\s+([+-]?\d+)$", message.text or "")
        if match:
            target, amt = match.group(1).lower(), int(match.group(2))
            new_bal = await update_balance(target, amt)
            await message.answer(f"✅ *{target}*: {new_bal} SNC")
            return
    await message.answer(f"❄️ Твой баланс: *{await check_user(username)}* SNC", parse_mode="Markdown", reply_markup=get_keyboard())

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    polling_task = asyncio.create_task(dp.start_polling(bot))
    yield
    polling_task.cancel()
    if db_pool: await db_pool.close()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.get("/api/user/{username}")
async def api_get_user(username: str):
    return {"username": username, "balance": await check_user(username.lower()), "tasks": [{"id": 1, "title": "Вступить в группу", "reward": 50, "done": False}]}

@app.get("/api/leaderboard")
async def api_get_leaderboard():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT 50")
        return [{"username": r['username'], "balance": r['balance']} for r in rows]

@app.get("/api/buy_snc/{username}/{amount_snc}/{amount_stars}")
async def api_create_invoice(username: str, amount_snc: int, amount_stars: int):
    invoice_url = await bot.create_invoice_link(
        title=f"Покупка {amount_snc} SNC",
        description=f"Пополнение баланса на {amount_snc} монет ❄️",
        payload=f"buy_snc:{username.lower()}:{amount_snc}",
        provider_token="", currency="XTR",
        prices=[LabeledPrice(label=f"{amount_snc} SNC", amount=int(amount_stars))]
    )
    return {"invoice_url": invoice_url}

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))