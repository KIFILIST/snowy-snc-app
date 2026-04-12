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
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:TipoParol@postgres.railway.internal:5432/railway")
WEBAPP_URL = "https://kifilist.github.io/snowy-snc-app/SNOWYAPP/sncecapp.html"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
app = FastAPI()
db_pool = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_db():
    global db_pool
    if db_pool is None:
        db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=20,
            command_timeout=60
        )
    return db_pool

async def init_db():
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                balance INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                username TEXT REFERENCES users(username),
                nft_id TEXT,
                UNIQUE(username, nft_id)
            );
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
        if row:
            return row['balance']
        return 0

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
        pool = await get_db()
        async with pool.acquire() as conn:
            inv_rows = await conn.fetch("SELECT nft_id FROM inventory WHERE username = $1", username.lower())
            inventory = [r['nft_id'] for r in inv_rows]
        user_data = {
            "username": username,
            "balance": balance,
            "inventory": inventory,
            "tasks": [
                {"title": "Вступить в отряд", "reward": 500, "done": False},
                {"title": "Пригласить бойца", "reward": 1000, "done": False},
                {"title": "Разведка местности", "reward": 300, "done": False},
                {"title": "Проверка связи", "reward": 150, "done": True}
            ]
        }
        return user_data
    except Exception as e:
        print(f"API Error (User): {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.post("/api/buy_nft/{username}/{nft_id}/{price}")
async def api_buy_nft(username: str, nft_id: str, price: int):
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            user_row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
            if not user_row or user_row['balance'] < price:
                raise HTTPException(status_code=400, detail="Insufficient SNC balance")
            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE username = $2", price, username.lower())
                await conn.execute("""
                    INSERT INTO inventory (username, nft_id)
                    VALUES ($1, $2)
                    ON CONFLICT (username, nft_id) DO NOTHING
                """, username.lower(), nft_id)
            return {"status": "success", "nft_id": nft_id}
    except Exception as e:
        print(f"NFT Purchase Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
            title="Набор монеток",
            description=f"Приобретение {amount_snc} SNC для аккаунта {username}",
            payload=json.dumps({"user": username, "amount": amount_snc}),
            provider_token="",
            currency="XTR",
            prices=prices
        )
        return {"invoice_url": invoice_link}
    except Exception as e:
        print(f"PAYMENT API Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username)
    if message.from_user.id in ADMIN_IDS:
        msg = (
            "👑 *SNC ELITE ADMINISTRATION*\n\n"
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
    pool = await get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT nft_id FROM inventory WHERE username = $1", username.lower())
    if rows:
        nfts = ", ".join([r['nft_id'] for r in rows])
        await message.answer(f"🖼️ Артефакты *{username}*:\n{nfts}", parse_mode="Markdown")
    else:
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

@dp.message(Command("starsrefund"))
async def cmd_starsrefund(message: types.Message):
    if message.from_user.id in ADMIN_IDS and message.chat.type == "private":
        pattern = r"^/starsrefund\s+@?(\w+)\s+(\d+)$"
        match = re.match(pattern, message.text or "")
        if not match:
            await message.answer(
                "⚠️ Неверный формат.\nИспользуй: `/starsrefund @username количество`",
                parse_mode="Markdown"
            )
            return

        target_username = match.group(1).lower()
        stars_amount = int(match.group(2))

        pool = await get_db()
        async with pool.acquire() as conn:
            user_row = await conn.fetchrow("SELECT username FROM users WHERE username = $1", target_username)

        if not user_row:
            await message.answer(f"❌ Пользователь @{target_username} не найден в базе.")
            return

        try:
            await bot.refund_star_payment(
                user_id=message.chat.id,
                telegram_payment_charge_id=f"refund_{target_username}_{stars_amount}"
            )
        except Exception:
            pass

        await message.answer(
            f"✅ Запрос на возврат оформлен.\n"
            f"Аккаунт: @{target_username}\n"
            f"Сумма: {stars_amount} ⭐\n\n"
            f"Перейди в @BotFather → выбери бота → *Payments* → *Refund* и введи данные вручную если Telegram не принял автоматический возврат.",
            parse_mode="Markdown"
        )
        return

    if message.chat.type == "private" and message.from_user.id not in ADMIN_IDS:
        username = get_username(message.from_user)
        admin_mentions = " ".join([f"[админу](tg://user?id={admin_id})" for admin_id in ADMIN_IDS])
        await message.answer(
            f"❄️ *Запрос на возврат Stars*\n\n"
            f"Если у вас возникла проблема с покупкой SNC или вы хотите вернуть средства — "
            f"наш администратор рассмотрит ваш запрос в течение 24 часов.\n\n"
            f"Ваш запрос передан {admin_mentions}.\n\n"
            f"Пожалуйста, опишите проблему в следующем сообщении.",
            parse_mode="Markdown"
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🔔 *Запрос на возврат*\n\n"
                    f"Пользователь: @{username} (`{message.from_user.id}`)\n"
                    f"Запросил возврат средств.\n\n"
                    f"Чтобы оформить возврат:\n"
                    f"`/starsrefund @{username} <количество_звёзд>`",
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"Failed to notify admin {admin_id}: {e}")

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

async def run_polling():
    while True:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        except Exception as e:
            print(f"Polling error: {e}, restarting in 5s...")
            await asyncio.sleep(5)

async def start_services():
    await init_db()
    app_port = int(os.getenv("PORT", 8080))
    api_config = uvicorn.Config(app, host="0.0.0.0", port=app_port, log_level="info")
    api_server = uvicorn.Server(api_config)
    await asyncio.gather(
        run_polling(),
        api_server.serve()
    )

if __name__ == "__main__":
    try:
        asyncio.run(start_services())
    except (KeyboardInterrupt, SystemExit):
        pass