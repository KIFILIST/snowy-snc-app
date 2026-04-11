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
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    WebAppInfo, 
    LabeledPrice, 
    PreCheckoutQuery, 
    InlineKeyboardMarkup, 
    InlineKeyboardButton
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL")
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()
db_pool = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_db():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY, 
                balance INTEGER DEFAULT 0
            )
        """)

async def check_user(username: str):
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        if row is None:
            await conn.execute("INSERT INTO users (username, balance) VALUES ($1, 0)", username.lower())
            return 0
        return row['balance']

async def update_balance(username: str, amount: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE username = $2", amount, username.lower())
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        return row['balance'] if row else 0

def get_username(user: types.User):
    if user.username:
        return user.username.lower()
    return f"user_{user.id}"

def get_keyboard():
    buttons = [
        [KeyboardButton(text="🚀 Ворваться в Штаб", web_app=WebAppInfo(url=WEBAPP_URL))]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

@app.get("/api/user/{username}")
async def api_get_user(username: str):
    try:
        balance = await check_user(username)
        user_data = {
            "username": username,
            "balance": balance,
            "tasks": [
                {"title": "Вступить в отряд", "reward": 500, "done": False},
                {"title": "Пригласить бойца", "reward": 1000, "done": False},
                {"title": "Разведка местности", "reward": 300, "done": False}
            ]
        }
        return user_data
    except Exception as e:
        print(f"API Error (User): {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/api/leaderboard")
async def api_get_leaderboard():
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT 10")
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"API Error (Leaderboard): {e}")
        raise HTTPException(status_code=500, detail="Database error")

@app.get("/api/buy_snc/{username}/{amount_snc}/{amount_stars}")
async def api_create_invoice(username: str, amount_snc: int, amount_stars: int):
    try:
        prices = [LabeledPrice(label=f"{amount_snc} SNC", amount=int(amount_stars))]
        invoice_link = await bot.create_invoice_link(
            title="Пополнение Snowy Coins",
            description=f"Приобретение {amount_snc} SNC для аккаунта {username}",
            payload=json.dumps({"user": username, "amount": amount_snc}),
            provider_token="",
            currency="XTR",
            prices=prices
        )
        return {"invoice_url": invoice_link}
    except Exception as e:
        print(f"API Error (Invoice): {e}")
        raise HTTPException(status_code=500, detail=str(e))

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    
    if message.from_user.id in ADMIN_IDS:
        msg = (
            "👑 *SNC ADMINISTRATION*\n\n"
            "Ваш статус подтвержден.\n"
            "Управление балансом:\n"
            "• ЛС: `@username +100` или `@username -50`\n"
            "• Группы: `/addsnc @username 100`"
        )
        await message.answer(msg, parse_mode="Markdown", reply_markup=get_keyboard())
    else:
        msg = f"👋 Приветствуем, *{username}*!\n\nТекущие активы: *{balance}* SNC ❄️"
        await message.answer(msg, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(Command("mysnc"))
async def cmd_mysnc(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    await message.answer(f"❄️ Информация по счету *{username}*:\nБаланс: *{balance}* SNC", parse_mode="Markdown")

@dp.message(Command("mynfts"))
async def cmd_mynfts(message: types.Message):
    username = get_username(message.from_user)
    await check_user(username)
    await message.answer(f"🖼️ Склад артефактов *{username}* пуст.", parse_mode="Markdown")

@dp.message(Command("addsnc"))
async def cmd_addsnc(message: types.Message):
    if message.chat.type != "private" and message.from_user.id in ADMIN_IDS:
        pattern = r"^/addsnc\s+@?(\w+)\s+([+-]?\d+)$"
        match = re.match(pattern, message.text or "")
        if match:
            target = match.group(1).lower()
            amount = int(match.group(2))
            await check_user(target)
            new_bal = await update_balance(target, amount)
            await message.answer(f"✅ Операция завершена.\nАккаунт: @{target}\nНовый баланс: {new_bal} SNC")

@dp.message(F.chat.type == "private")
async def handle_private_logic(message: types.Message):
    user_id = message.from_user.id
    username = get_username(message.from_user)
    await check_user(username)
    
    if user_id in ADMIN_IDS:
        admin_pattern = r"^@?(\w+)\s+([+-]?\d+)$"
        match = re.match(admin_pattern, message.text or "")
        if match:
            target = match.group(1).lower()
            amount = int(match.group(2))
            
            pool = await get_db()
            async with pool.acquire() as conn:
                exists = await conn.fetchrow("SELECT username FROM users WHERE username = $1", target)
            
            if exists:
                new_bal = await update_balance(target, amount)
                await message.answer(f"✅ Успешно.\n@{target}: {new_bal} SNC")
            else:
                await message.answer(f"❌ Объект @{target} не найден в базе.")
            return

    current_balance = await check_user(username)
    await message.answer(f"❄️ Ваш баланс: {current_balance} SNC", reply_markup=get_keyboard())

@dp.pre_checkout_query()
async def process_pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)

@dp.message(F.successful_payment)
async def success_payment_handler(message: types.Message):
    pay_data = json.loads(message.successful_payment.invoice_payload)
    target_user = pay_data['user']
    add_amount = pay_data['amount']
    
    new_total = await update_balance(target_user, add_amount)
    await message.answer(
        f"✅ Платеж подтвержден!\n"
        f"Зачислено: {add_amount} SNC\n"
        f"Текущий баланс: {new_total} SNC"
    )

async def start_services():
    await init_db()
    
    app_port = int(os.getenv("PORT", 8080))
    api_config = uvicorn.Config(app, host="0.0.0.0", port=app_port, log_level="info")
    api_server = uvicorn.Server(api_config)
    
    await bot.delete_webhook(drop_pending_updates=True)
    
    await asyncio.gather(
        dp.start_polling(bot),
        api_server.serve()
    )

if __name__ == "__main__":
    try:
        asyncio.run(start_services())
    except (KeyboardInterrupt, SystemExit):
        pass