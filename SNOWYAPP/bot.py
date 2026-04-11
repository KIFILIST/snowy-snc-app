import os
import re
import json
import asyncio
import aiosqlite
import uvicorn
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"
DB_PATH = "dtb.sqlite"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, balance INTEGER)")
        await db.commit()

async def check_user(username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT balance FROM users WHERE username = ?", (username,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                await db.execute("INSERT INTO users (username, balance) VALUES (?, ?)", (username, 0))
                await db.commit()
                return 0
            return row[0]

async def update_balance(username: str, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = balance + ? WHERE username = ?", (amount, username))
        await db.commit()
        async with db.execute("SELECT balance FROM users WHERE username = ?", (username,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

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

        if amount <= 0:
            await message.answer("❌ Сумма перевода должна быть больше нуля.")
            return

        if sender == target:
            await message.answer("❌ Нельзя перевести SNC самому себе.")
            return

        sender_balance = await check_user(sender)
        if sender_balance < amount:
            await message.answer("❌ Недостаточно SNC на балансе.")
            return

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT balance FROM users WHERE username = ?", (target,)) as cursor:
                target_row = await cursor.fetchone()

        if target_row is None:
            await message.answer(f"❌ Пользователь *{target}* не найден в базе.", parse_mode="Markdown")
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
        await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())
    else:
        text = f"👋 Привет, *{username}*!\n\nТвой баланс: *{balance}* SNC ❄️"
        await message.answer(text, parse_mode="Markdown", reply_markup=get_keyboard())

@dp.message(Command("mysnc"))
async def cmd_mysnc(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    text = f"❄️ У пользователя *{username}* *{balance}* SNC"
    markup = get_keyboard() if message.chat.type == "private" else None
    await message.answer(text, parse_mode="Markdown", reply_markup=markup)

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
            
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT balance FROM users WHERE username = ?", (target,)) as cursor:
                    row = await cursor.fetchone()
                    
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    polling_task = asyncio.create_task(dp.start_polling(bot))
    yield
    polling_task.cancel()
    await bot.session.close()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "Snowy SNC Bot is running perfectly!", "tech": "FastAPI + Aiogram"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)