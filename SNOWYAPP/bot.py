import os
import re
import json
import asyncio
import logging
import asyncpg
import uvicorn
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    WebAppInfo, 
    LabeledPrice, 
    PreCheckoutQuery,
    BotCommand,
    MenuButtonWebApp,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("SnowySNC.Enterprise")

class Settings:
    TOKEN = os.getenv("BOT_TOKEN")
    DATABASE_URL = os.getenv("DATABASE_URL")
    ADMINS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
    WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"
    PORT = int(os.getenv("PORT", 8000))
    MIN_POOL_SIZE = 10
    MAX_POOL_SIZE = 30

class TaskSchema(BaseModel):
    id: int
    title: str
    reward: int
    done: bool

class UserResponse(BaseModel):
    username: str
    balance: int
    level: int
    joined_at: str
    tasks: List[TaskSchema]

class LeaderboardEntry(BaseModel):
    username: str
    balance: int
    rank: int

class GlobalStats(BaseModel):
    total_users: int
    total_snc: int
    active_today: int

class DatabaseManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        for attempt in range(1, 11):
            try:
                self.pool = await asyncpg.create_pool(
                    self.dsn,
                    min_size=Settings.MIN_POOL_SIZE,
                    max_size=Settings.MAX_POOL_SIZE,
                    command_timeout=60,
                    timeout=30,
                    max_inactive_connection_lifetime=300
                )
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            username TEXT PRIMARY KEY,
                            balance BIGINT DEFAULT 0,
                            level INTEGER DEFAULT 1,
                            xp INTEGER DEFAULT 0,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            last_login TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                        CREATE TABLE IF NOT EXISTS transactions (
                            id SERIAL PRIMARY KEY,
                            sender TEXT,
                            receiver TEXT,
                            amount BIGINT,
                            tx_type TEXT,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                logger.info("Database initialized successfully.")
                return True
            except Exception as e:
                logger.error(f"Connection attempt {attempt} failed: {e}")
                await asyncio.sleep(3)
        return False

    async def sync_user(self, username: str) -> Dict[str, Any]:
        username = username.lower()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO users (username, last_login) VALUES ($1, CURRENT_TIMESTAMP)
                ON CONFLICT (username) DO UPDATE SET last_login = CURRENT_TIMESTAMP
                RETURNING username, balance, level, created_at
            """, username)
            return dict(row)

    async def get_balance(self, username: str) -> int:
        username = username.lower()
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT balance FROM users WHERE username = $1", username) or 0

    async def update_balance(self, username: str, amount: int, tx_type: str = "system") -> int:
        username = username.lower()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                new_bal = await conn.fetchval("""
                    INSERT INTO users (username, balance) VALUES ($1, $2)
                    ON CONFLICT (username) DO UPDATE SET balance = users.balance + $2
                    RETURNING balance
                """, username, amount)
                await conn.execute(
                    "INSERT INTO transactions (sender, amount, tx_type) VALUES ($1, $2, $3)",
                    username, amount, tx_type
                )
                return new_bal

    async def process_transfer(self, sender: str, receiver: str, amount: int):
        sender, receiver = sender.lower(), receiver.lower()
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                s_bal = await conn.fetchval("SELECT balance FROM users WHERE username = $1 FOR UPDATE", sender)
                if s_bal < amount: return False, "Insufficient funds"
                
                t_exists = await conn.fetchval("SELECT 1 FROM users WHERE username = $1", receiver)
                if not t_exists: return False, "Receiver not found"
                
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE username = $2", amount, sender)
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE username = $2", amount, receiver)
                await conn.execute(
                    "INSERT INTO transactions (sender, receiver, amount, tx_type) VALUES ($1, $2, $3, 'transfer')",
                    sender, receiver, amount
                )
                return True, "Success"

    async def get_top_players(self, limit: int = 50):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

    async def get_stats(self):
        async with self.pool.acquire() as conn:
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            total_snc = await conn.fetchval("SELECT SUM(balance) FROM users")
            active = await conn.fetchval("SELECT COUNT(*) FROM users WHERE last_login > NOW() - INTERVAL '24 hours'")
            return {"total_users": total_users, "total_snc": total_snc or 0, "active_today": active}

db = DatabaseManager(Settings.DATABASE_URL)
bot = Bot(token=Settings.TOKEN)
dp = Dispatcher()

def main_markup():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❄️ Открыть Snowy App", web_app=WebAppInfo(url=Settings.WEBAPP_URL))]],
        resize_keyboard=True,
        input_field_placeholder="Управление Snowy SNC..."
    )

@dp.pre_checkout_query()
async def handle_pre_checkout(query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(query.id, ok=True)

@dp.message(F.successful_payment)
async def handle_payment(message: types.Message):
    payload = message.successful_payment.invoice_payload
    if payload.startswith("snc_buy:"):
        _, user, amount = payload.split(":")
        new_balance = await db.update_balance(user, int(amount), "stars_purchase")
        await message.answer(
            f"💠 *Пополнение завершено!*\n\nЗачислено: +*{amount}* SNC\nТекущий баланс: *{new_balance}* SNC ❄️",
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
            
            if amount <= 0 or sender == target: return
            
            success, msg = await db.process_transfer(sender, target, amount)
            if success:
                await message.answer(f"✅ *Успешный перевод!*\n\nВы отправили *{amount}* SNC игроку *{target}*.", parse_mode="Markdown")
            else:
                await message.answer(f"❌ *Ошибка транзакции:*\n{msg}", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"WebApp processing error: {e}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = (message.from_user.username or f"id{message.from_user.id}").lower()
    data = await db.sync_user(user_id)
    
    text = (
        f"❄️ *Snowy SNC Interface*\n\n"
        f"Аккаунт: `@{user_id}`\n"
        f"Баланс: *{data['balance']}* SNC\n"
        f"Уровень: *{data['level']}*\n\n"
        f"Используйте кнопку ниже для доступа к приложению."
    )
    
    if message.from_user.id in Settings.ADMINS:
        text += "\n\n🛠 *Админ-функции активны*"
        
    await message.answer(text, parse_mode="Markdown", reply_markup=main_markup())

@dp.message(Command("admin"))
async def cmd_admin_stats(message: types.Message):
    if message.from_user.id not in Settings.ADMINS: return
    stats = await db.get_stats()
    text = (
        "📊 *Статистика системы:*\n\n"
        f"Всего пользователей: *{stats['total_users']}*\n"
        f"Всего SNC в обороте: *{stats['total_snc']}*\n"
        f"Активны за 24ч: *{stats['active_today']}*"
    )
    await message.answer(text, parse_mode="Markdown")

@dp.message(F.chat.type == "private")
async def private_router(message: types.Message):
    user_id = (message.from_user.username or f"id{message.from_user.id}").lower()
    
    if message.from_user.id in Settings.ADMINS:
        match = re.match(r"^@(\w+)\s+([+-]?\d+)$", message.text or "")
        if match:
            target, amt = match.group(1).lower(), int(match.group(2))
            res = await db.update_balance(target, amt, "admin_edit")
            await message.answer(f"⚙️ *Реестр изменен*\nЮзер: `@{target}`\nБаланс: *{res}* SNC", parse_mode="Markdown")
            return

    bal = await db.get_balance(user_id)
    await message.answer(f"❄️ Ваш баланс: *{bal}* SNC", reply_markup=main_markup(), parse_mode="Markdown")

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not await db.connect():
        logger.critical("Critical error: Database is unreachable.")
        raise RuntimeError("DB_OFFLINE")
        
    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="admin", description="Статистика (Админ)")
    ])
    
    await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Snowy App", web_app=WebAppInfo(url=Settings.WEBAPP_URL)))
    await bot.delete_webhook(drop_pending_updates=True)
    
    polling_task = asyncio.create_task(dp.start_polling(bot))
    logger.info("Bot polling initiated.")
    
    yield
    
    polling_task.cancel()
    if db.pool: await db.pool.close()
    await bot.session.close()

app = FastAPI(lifespan=lifespan, title="Snowy SNC Backend", version="3.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/api/user/{username}", response_model=UserResponse)
async def api_user_data(username: str):
    data = await db.sync_user(username)
    return {
        "username": username,
        "balance": data['balance'],
        "level": data['level'],
        "joined_at": data['created_at'].strftime("%Y-%m-%d"),
        "tasks": [
            {"id": 1, "title": "Ежедневная проверка", "reward": 100, "done": False},
            {"id": 2, "title": "Пригласить напарника", "reward": 500, "done": False}
        ]
    }

@app.get("/api/leaderboard", response_model=List[LeaderboardEntry])
async def api_leaderboard():
    rows = await db.get_top_players(50)
    return [{"username": r['username'], "balance": r['balance'], "rank": i+1} for i, r in enumerate(rows)]

@app.get("/api/stats", response_model=GlobalStats)
async def api_global_stats():
    return await db.get_stats()

@app.get("/api/buy_snc/{u}/{snc}/{stars}")
async def api_invoice(u: str, snc: int, stars: int):
    try:
        url = await bot.create_invoice_link(
            title=f"Контейнер {snc} SNC",
            description=f"Энергетический пакет SNC для @{u} ❄️",
            payload=f"snc_buy:{u.lower()}:{snc}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"{snc} SNC", amount=int(stars))]
        )
        return {"invoice_url": url}
    except Exception as e:
        logger.error(f"Invoice fail: {e}")
        raise HTTPException(status_code=500)

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=Settings.PORT, workers=4, access_log=True)