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
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, LabeledPrice, PreCheckoutQuery

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

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_q: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_q.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("buy_snc:"):
        _, target_user, snc_amount = payload.split(":")
        new_balance = await update_balance(target_user, int(snc_amount))
        await message.answer(f"🎉 *Оплата прошла успешно!*\n\nЗачислено: *{snc_amount} SNC*\nБаланс: *{new_balance} SNC*", parse_mode="Markdown")

@dp.message(F.web_app_data)
async def web_app_handler(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
    except:
        return

    if data.get("type") == "transfer":
        sender = get_username(message.from_user)
        target = str(data.get("target")).lower().replace("@", "")
        amount = int(data.get("amount"))

        if amount <= 0 or sender == target: return

        s_bal = await check_user(sender)
        if s_bal < amount:
            await message.answer("❌ Недостаточно SNC.")
            return

        async with db_pool.acquire() as conn:
            t_row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", target)

        if t_row is None:
            await message.answer(f"❌ Игрок *{target}* не найден.", parse_mode="Markdown")
            return

        await update_balance(sender, -amount)
        await update_balance(target, amount)
        await message.answer(f"✅ Переведено *{amount} SNC* игроку *{target}*.", parse_mode="Markdown")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    text = f"👋 Привет, *{username}*!\nТвой баланс: *{balance}* SNC ❄️"
    if message.from_user.id in ADMIN_IDS:
        text += "\n\n👑 Вы админ. Используйте `@username +100` для начисления."
    await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(F.chat.type == "private")
async def handle_private(message: types.Message):
    username = get_username(message.from_user)
    await check_user(username)
    
    if message.from_user.id in ADMIN_IDS:
        match = re.match(r"^@(\w+)\s+([+-]?\d+)$", message.text or "")
        if match:
            target, amt = match.group(1).lower(), int(match.group(2))
            async with db_pool.acquire() as conn:
                row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", target)
            if row:
                nb = await update_balance(target, amt)
                await message.answer(f"✅ *{target}*: {nb} SNC")
            else:
                await message.answer("❌ Не найден.")
            return

    balance = await check_user(username)
    await message.answer(f"❄️ Твой баланс: *{balance}* SNC", parse_mode="Markdown", reply_markup=get_keyboard())

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    task = asyncio.create_task(dp.start_polling(bot))
    yield
    task.cancel()
    if db_pool: await db_pool.close()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/user/{username}")
async def api_user(username: str):
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
        return {"username": username, "balance": r['balance'] if r else 0, "tasks": [{"id": 1, "title": "Вступить в группу", "reward": 100, "done": False}]}

@app.get("/api/leaderboard")
async def api_top():
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT 10")
        return [{"username": r['username'], "balance": r['balance']} for r in rows]

@app.get("/api/buy_snc/{username}/{snc}/{stars}")
async def api_pay(username: str, snc: int, stars: int):
    link = await bot.create_invoice_link(title=f"{snc} SNC", description="Пополнение", payload=f"buy_snc:{username}:{snc}", provider_token="", currency="XTR", prices=[LabeledPrice(label="SNC", amount=stars)])
    return {"invoice_url": link}

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))