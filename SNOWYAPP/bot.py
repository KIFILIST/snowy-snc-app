import asyncio
import os
import json
import re
import asyncpg
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, LabeledPrice, PreCheckoutQuery, InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/sncapp.html"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()
db_pool = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)
    async with db_pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, balance INTEGER DEFAULT 0)")

async def check_user(username: str):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        if row is None:
            await conn.execute("INSERT INTO users (username, balance) VALUES ($1, $2)", username.lower(), 0)
            return 0
        return row['balance']

async def update_balance(username: str, amount: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE username = $2", amount, username.lower())
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        return row['balance'] if row else 0

def get_username(user: types.User):
    return user.username.lower() if user.username else f"user_{user.id}"

def get_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text="🚀 Ворваться в Штаб", web_app=WebAppInfo(url=WEBAPP_URL))
    ]], resize_keyboard=True)

@app.get("/api/user/{username}")
async def api_get_user(username: str):
    balance = await check_user(username)
    tasks = [
        {"title": "Вступить в отряд", "reward": 500, "done": False},
        {"title": "Пригласить бойца", "reward": 1000, "done": False}
    ]
    return {"username": username, "balance": balance, "tasks": tasks}

@app.get("/api/leaderboard")
async def api_get_leaderboard():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT 10")
        return [dict(r) for r in rows]

@app.get("/api/buy_snc/{username}/{amount_snc}/{amount_stars}")
async def api_create_invoice(username: str, amount_snc: int, amount_stars: int):
    prices = [LabeledPrice(label=f"{amount_snc} SNC", amount=amount_stars)]
    try:
        link = await bot.create_invoice_link(
            title="Пополнение SNC",
            description=f"Пакет на {amount_snc} SNC для {username}",
            payload=json.dumps({"user": username, "amount": amount_snc}),
            currency="XTR",
            prices=prices
        )
        return {"invoice_url": link}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    if message.from_user.id in ADMIN_IDS:
        text = "👑 *SNC Admin*\n\nЛС: `@username +100` / `@username -50`\nГруппы: `/addsnc @username 100`"
        await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())
    else:
        text = f"👋 Привет, *{username}*!\n\nТвой баланс: *{balance}* SNC ❄️"
        await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(Command("mysnc"))
async def cmd_mysnc(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    text = f"❄️ У пользователя *{username}* *{balance}* SNC"
    await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(Command("mynfts"))
async def cmd_mynfts(message: types.Message):
    username = get_username(message.from_user)
    await check_user(username)
    text = f"🖼️ У пользователя *{username}* пока нет NFT."
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("addsnc"))
async def cmd_addsnc(message: types.Message):
    if message.chat.type != "private" and message.from_user.id in ADMIN_IDS:
        match = re.match(r"^/addsnc\s+@(\w+)\s+([+-]?\d+)$", message.text or "")
        if match:
            target = match.group(1).lower()
            amt = int(match.group(2))
            await check_user(target)
            new_balance = await update_balance(target, amt)
            text = f"✅ У пользователя *{target}* *{new_balance}* SNC"
            await message.answer(text, parse_mode="Markdown")

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
                text = f"✅ У пользователя *{target}* *{new_balance}* SNC"
            else:
                text = f"❌ Пользователь *@{target}* не найден."
            await message.answer(text, parse_mode="Markdown")
            return
    balance = await check_user(username)
    text = f"❄️ У пользователя *{username}* *{balance}* SNC"
    await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.pre_checkout_query()
async def process_pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@dp.message(F.successful_payment)
async def success_pay(message: types.Message):
    payload = json.loads(message.successful_payment.invoice_payload)
    user = payload['user']
    amount = payload['amount']
    await update_balance(user, amount)
    await message.answer(f"✅ Зачислено {amount} SNC!")

async def run_bot():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

async def run_api():
    port = int(os.getenv("PORT", 8080))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    await init_db()
    await asyncio.gather(run_bot(), run_api())

if __name__ == "__main__":
    asyncio.run(main())