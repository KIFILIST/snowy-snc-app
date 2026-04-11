import os
import re
import json
import asyncio
import logging
import asyncpg
import uvicorn
from typing import List, Optional
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    WebAppInfo, 
    LabeledPrice, 
    PreCheckoutQuery,
    BotCommand
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"

class UserSchema(BaseModel):
    username: str
    balance: int
    tasks: List[dict]

class LeaderboardEntry(BaseModel):
    username: str
    balance: int

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        for attempt in range(1, 6):
            try:
                self.pool = await asyncpg.create_pool(
                    self.dsn,
                    min_size=2,
                    max_size=15,
                    command_timeout=30,
                    max_inactive_connection_lifetime=300
                )
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            username TEXT PRIMARY KEY,
                            balance BIGINT DEFAULT 0,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                logger.info("Database engine initialized successfully.")
                return True
            except Exception as e:
                logger.error(f"Database connection attempt {attempt} failed: {e}")
                await asyncio.sleep(attempt * 2)
        return False

    async def get_user_balance(self, username: str) -> int:
        username = username.lower()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username)
            if row is None:
                await conn.execute("INSERT INTO users (username, balance) VALUES ($1, 0)", username)
                return 0
            return row['balance']

    async def adjust_balance(self, username: str, amount: int) -> int:
        username = username.lower()
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                INSERT INTO users (username, balance) 
                VALUES ($1, $2) 
                ON CONFLICT (username) 
                DO UPDATE SET balance = users.balance + $2 
                RETURNING balance
            """, username, amount)

    async def get_top_players(self, limit: int = 50) -> List[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

    async def close(self):
        if self.pool:
            await self.pool.close()

db = DatabaseManager(DATABASE_URL)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Открыть SNC App", web_app=WebAppInfo(url=WEBAPP_URL))]],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

@dp.pre_checkout_query()
async def process_pre_checkout(query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(query.id, ok=True)

@dp.message(F.successful_payment)
async def handle_payment_success(message: types.Message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("buy_snc:"):
        parts = payload.split(":")
        target_user = parts[1]
        snc_amount = int(parts[2])
        new_balance = await db.adjust_balance(target_user, snc_amount)
        
        logger.info(f"Payment success: {target_user} bought {snc_amount} SNC")
        
        await message.answer(
            f"✨ *Транзакция завершена!*\n\n"
            f"Начислено: +*{snc_amount}* SNC ❄️\n"
            f"Текущий баланс: *{new_balance}* SNC",
            parse_mode="Markdown"
        )

@dp.message(F.web_app_data)
async def handle_webapp_data(message: types.Message):
    try:
        data = json.loads(message.web_app_data.data)
        if data.get("type") == "transfer":
            sender = (message.from_user.username or f"id{message.from_user.id}").lower()
            target = str(data.get("target")).lower().replace("@", "").strip()
            amount = int(data.get("amount"))

            if amount <= 0:
                await message.answer("❌ Сумма должна быть больше нуля.")
                return
            if sender == target:
                await message.answer("❌ Нельзя переводить монеты самому себе.")
                return

            current_sender_bal = await db.get_user_balance(sender)
            if current_sender_bal < amount:
                await message.answer("❌ Ошибка: Недостаточно SNC на балансе.")
                return

            async with db.pool.acquire() as conn:
                target_exists = await conn.fetchval("SELECT 1 FROM users WHERE username = $1", target)
                if not target_exists:
                    await message.answer(f"❌ Игрок *{target}* еще ни разу не заходил в бота.", parse_mode="Markdown")
                    return

                await db.adjust_balance(sender, -amount)
                await db.adjust_balance(target, amount)
                
                await message.answer(f"✅ *Успешный перевод!*\n\nОтправлено: *{amount}* SNC ❄️\nПолучатель: *{target}*", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"WebApp data error: {e}")

@dp.message(Command("start"))
async def start_command(message: types.Message):
    user_ref = (message.from_user.username or f"id{message.from_user.id}").lower()
    balance = await db.get_user_balance(user_ref)
    
    welcome_text = (
        f"❄️ *Добро пожаловать в Snowy SNC!*\n\n"
        f"Ваш аккаунт: `@{user_ref}`\n"
        f"Ваш баланс: *{balance}* SNC\n\n"
        f"Используйте кнопку ниже, чтобы запустить приложение и управлять своими активами."
    )
    
    if message.from_user.id in ADMIN_IDS:
        welcome_text += "\n\n🛠 *Админ-панель активна:*\nНачисление: `@username +100`"
    
    await message.answer(welcome_text, parse_mode="Markdown", reply_markup=main_keyboard())

@dp.message(F.chat.type == "private")
async def private_message_router(message: types.Message):
    user_ref = (message.from_user.username or f"id{message.from_user.id}").lower()
    
    if message.from_user.id in ADMIN_IDS:
        admin_match = re.match(r"^@(\w+)\s+([+-]?\d+)$", message.text or "")
        if admin_match:
            target_user = admin_match.group(1).lower()
            change_amount = int(admin_match.group(2))
            
            new_total = await db.adjust_balance(target_user, change_amount)
            await message.answer(f"⚙️ *Статус изменен:*\nПользователь: `@{target_user}`\nНовый баланс: *{new_total}* SNC ❄️", parse_mode="Markdown")
            return

    current_balance = await db.get_user_balance(user_ref)
    await message.answer(f"❄️ Ваш текущий баланс: *{current_balance}* SNC", parse_mode="Markdown", reply_markup=main_keyboard())

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_status = await db.connect()
    if not db_status:
        logger.critical("Failed to connect to database. Shutdown initiated.")
        return

    await bot.set_my_commands([
        BotCommand(command="start", description="Запустить бота / Баланс"),
    ])
    
    await bot.delete_webhook(drop_pending_updates=True)
    polling_task = asyncio.create_task(dp.start_polling(bot))
    logger.info("Bot polling started.")
    
    yield
    
    polling_task.cancel()
    await db.close()
    await bot.session.close()
    logger.info("Services gracefully shut down.")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/user/{username}", response_model=UserSchema)
async def get_user_data(username: str):
    try:
        balance = await db.get_user_balance(username.lower())
        return {
            "username": username,
            "balance": balance,
            "tasks": [
                {"id": 1, "title": "Подписаться на канал", "reward": 150, "done": False},
                {"id": 2, "title": "Пригласить друга", "reward": 500, "done": False}
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/leaderboard", response_model=List[LeaderboardEntry])
async def get_top_list():
    try:
        return await db.get_top_players(50)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/buy_snc/{username}/{snc}/{stars}")
async def create_payment_link(username: str, snc: int, stars: int):
    try:
        invoice_link = await bot.create_invoice_link(
            title=f"Пакет {snc} SNC",
            description=f"Приобретение {snc} монет для Snowy SNC App ❄️",
            payload=f"buy_snc:{username.lower()}:{snc}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"{snc} SNC", amount=int(stars))]
        )
        return {"invoice_url": invoice_link}
    except Exception as e:
        logger.error(f"Invoice error: {e}")
        raise HTTPException(status_code=500, detail="Could not create invoice link")

if __name__ == "__main__":
    app_port = int(os.getenv("PORT", 8000))
    uvicorn.run("bot:app", host="0.0.0.0", port=app_port, log_level="info")