import os
import re
import json
import asyncio
import logging
import asyncpg
import uvicorn
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from pydantic import BaseModel, Field, validator
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
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
    MenuButtonWebApp,
    ContentType
)
from aiogram.exceptions import TelegramBadRequest

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S"
)
logger = logging.getLogger("SnowySNC.Core")

class AppConfig:
    TOKEN: str = os.getenv("BOT_TOKEN")
    DB_URL: str = os.getenv("DATABASE_URL")
    ADMINS: List[int] = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
    WEBAPP: str = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"
    PORT: int = int(os.getenv("PORT", 8000))

class TaskModel(BaseModel):
    id: int
    title: str
    reward: int
    done: bool = False

class UserProfile(BaseModel):
    username: str
    balance: int
    level: int = 1
    tasks: List[TaskModel] = []
    last_active: Optional[datetime] = None

class LeaderboardRow(BaseModel):
    username: str
    balance: int

class DatabaseProvider:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def start(self):
        for attempt in range(1, 7):
            try:
                self.pool = await asyncpg.create_pool(
                    self.dsn,
                    min_size=10,
                    max_size=30,
                    max_queries=50000,
                    max_inactive_connection_lifetime=600,
                    command_timeout=60
                )
                async with self.pool.acquire() as conn:
                    await conn.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                            username TEXT PRIMARY KEY,
                            balance BIGINT DEFAULT 0,
                            level INTEGER DEFAULT 1,
                            xp INTEGER DEFAULT 0,
                            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                logger.info("Database engine started successfully.")
                return True
            except Exception as e:
                logger.error(f"Failed to connect to PG (Attempt {attempt}): {e}")
                await asyncio.sleep(attempt * 1.5)
        return False

    async def get_or_create_user(self, username: str) -> Dict[str, Any]:
        username = username.lower()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT username, balance, level FROM users WHERE username = $1", username)
            if not row:
                row = await conn.fetchrow(
                    "INSERT INTO users (username, balance) VALUES ($1, 0) RETURNING username, balance, level", 
                    username
                )
            return dict(row)

    async def increment_balance(self, username: str, amount: int) -> int:
        username = username.lower()
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                INSERT INTO users (username, balance) 
                VALUES ($1, $2) 
                ON CONFLICT (username) 
                DO UPDATE SET balance = users.balance + $2, updated_at = CURRENT_TIMESTAMP
                RETURNING balance
            """, username, amount)

    async def get_top_list(self, limit: int = 50) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT username, balance FROM users ORDER BY balance DESC LIMIT $1", limit)
            return [dict(r) for r in rows]

    async def stop(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed.")

storage = DatabaseProvider(AppConfig.DB_URL)
bot = Bot(token=AppConfig.TOKEN)
dp = Dispatcher()

def ui_keyboard():
    builder = [[KeyboardButton(text="❄️ Запустить Snowy App", web_app=WebAppInfo(url=AppConfig.WEBAPP))]]
    return ReplyKeyboardMarkup(keyboard=builder, resize_keyboard=True, input_field_placeholder="Управление счетом...")

@dp.pre_checkout_query()
async def on_pre_checkout(query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(query.id, ok=True)

@dp.message(F.successful_payment)
async def on_payment_done(message: types.Message):
    info = message.successful_payment.invoice_payload
    if info.startswith("topup:"):
        _, target, snc_val = info.split(":")
        final_bal = await storage.increment_balance(target, int(snc_val))
        await message.answer(
            f"💠 *Транзакция подтверждена*\n\n"
            f"Объект: `{target}`\n"
            f"Пополнение: +*{snc_val}* SNC\n"
            f"Итоговый баланс: *{final_bal}* SNC",
            parse_mode="Markdown"
        )

@dp.message(F.web_app_data)
async def on_webapp_event(message: types.Message):
    try:
        raw_payload = json.loads(message.web_app_data.data)
        if raw_payload.get("type") == "transfer":
            sender_id = (message.from_user.username or f"id{message.from_user.id}").lower()
            target_id = str(raw_payload.get("target")).lower().replace("@", "").strip()
            val = int(raw_payload.get("amount"))

            if val <= 0 or sender_id == target_id:
                return

            current_s_bal = (await storage.get_or_create_user(sender_id))['balance']
            if current_s_bal < val:
                await message.answer("⚠️ Ошибка: недостаточно единиц SNC.")
                return

            async with storage.pool.acquire() as conn:
                check_t = await conn.fetchval("SELECT 1 FROM users WHERE username = $1", target_id)
                if not check_t:
                    await message.answer(f"⚠️ Субъект `@{target_id}` не найден в реестре.", parse_mode="Markdown")
                    return
                
                async with conn.transaction():
                    await storage.increment_balance(sender_id, -val)
                    await storage.increment_balance(target_id, val)
                    
            await message.answer(
                f"✅ *Перевод выполнен*\n\n"
                f"Отправитель: `@{sender_id}`\n"
                f"Получатель: `@{target_id}`\n"
                f"Сумма: *{val}* SNC",
                parse_mode="Markdown"
            )
    except Exception as exc:
        logger.error(f"WebApp logic fail: {exc}")

@dp.message(Command("start"))
async def on_cmd_start(message: types.Message):
    u_id = (message.from_user.username or f"id{message.from_user.id}").lower()
    data = await storage.get_or_create_user(u_id)
    
    output = (
        f"🧊 *SNOWY SNC INTERFACE*\n\n"
        f"Идентификатор: `@{u_id}`\n"
        f"Текущий баланс: *{data['balance']}* SNC\n"
        f"Уровень доступа: *{data['level']}*"
    )
    
    if message.from_user.id in AppConfig.ADMINS:
        output += "\n\n🛠 *Административный доступ подтвержден.*"
        
    await message.answer(output, parse_mode="Markdown", reply_markup=ui_keyboard())

@dp.message(F.chat.type == "private")
async def on_private_interaction(message: types.Message):
    u_id = (message.from_user.username or f"id{message.from_user.id}").lower()
    
    if message.from_user.id in AppConfig.ADMINS:
        admin_rx = re.match(r"^@(\w+)\s+([+-]?\d+)$", message.text or "")
        if admin_rx:
            t_user = admin_rx.group(1).lower()
            t_amt = int(admin_rx.group(2))
            res_bal = await storage.increment_balance(t_user, t_amt)
            await message.answer(f"💠 Реестр обновлен. `@{t_user}`: *{res_bal}* SNC.")
            return

    u_data = await storage.get_or_create_user(u_id)
    await message.answer(f"❄️ Баланс: *{u_data['balance']}* SNC", reply_markup=ui_keyboard())

@asynccontextmanager
async def app_lifespan(app: FastAPI):
    if not await storage.start():
        logger.critical("Database initialization failed. Shifting to emergency stop.")
        raise SystemExit(1)

    await bot.set_my_commands([BotCommand(command="start", description="Главная консоль")])
    await bot.set_chat_menu_button(menu_button=MenuButtonWebApp(text="Snowy App", web_app=WebAppInfo(url=AppConfig.WEBAPP)))
    
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(dp.start_polling(bot))
    logger.info("Bot background polling started.")
    
    yield
    
    await storage.stop()
    await bot.session.close()

app = FastAPI(lifespan=app_lifespan, title="Snowy SNC API Service", version="2.5.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/user/{username}", response_model=UserProfile)
async def api_fetch_user(username: str):
    try:
        raw = await storage.get_or_create_user(username)
        return {
            "username": raw['username'],
            "balance": raw['balance'],
            "level": raw['level'],
            "tasks": [
                {"id": 1, "title": "Ежедневное сканирование", "reward": 75, "done": False},
                {"id": 2, "title": "Приглашение рекрута", "reward": 300, "done": False}
            ]
        }
    except Exception as e:
        logger.error(f"API Error: {e}")
        raise HTTPException(status_code=500, detail="Internal core failure")

@app.get("/api/leaderboard", response_model=List[LeaderboardRow])
async def api_fetch_top():
    try:
        return await storage.get_top_list(50)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/buy_snc/{username}/{snc_val}/{star_val}")
async def api_generate_invoice(username: str, snc_val: int, star_val: int):
    try:
        link = await bot.create_invoice_link(
            title=f"Контейнер {snc_val} SNC",
            description=f"Приобретение энергетических единиц SNC для модуля @{username}",
            payload=f"topup:{username.lower()}:{snc_val}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"{snc_val} SNC", amount=int(star_val))]
        )
        return {"invoice_url": link}
    except Exception as e:
        logger.error(f"Invoice generation failed: {e}")
        raise HTTPException(status_code=500, detail="Gateway error")

if __name__ == "__main__":
    uvicorn.run("bot:app", host="0.0.0.0", port=AppConfig.PORT, reload=False, workers=4)