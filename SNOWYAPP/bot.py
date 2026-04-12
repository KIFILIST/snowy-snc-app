import asyncio
import os
import json
import re
import random
import asyncpg
import uvicorn
from datetime import date, datetime, timezone
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

STATIC_QUESTS = [
    {
        "id": "ref_1",
        "title": "Первый призыв",
        "description": "Пригласи 1 друга по реферальной ссылке",
        "reward": 25,
        "type": "referral",
        "required": 1
    },
    {
        "id": "ref_3",
        "title": "Малый отряд",
        "description": "Пригласи 3 друзей по реферальной ссылке",
        "reward": 60,
        "type": "referral",
        "required": 3
    },
    {
        "id": "ref_5",
        "title": "Элитное звено",
        "description": "Пригласи 5 друзей по реферальной ссылке",
        "reward": 120,
        "type": "referral",
        "required": 5
    },
    {
        "id": "ref_10",
        "title": "Командир роты",
        "description": "Пригласи 10 друзей по реферальной ссылке",
        "reward": 300,
        "type": "referral",
        "required": 10
    },
    {
        "id": "first_transfer",
        "title": "Первый перевод",
        "description": "Отправь SNC другому пользователю",
        "reward": 10,
        "type": "transfer",
        "required": 1
    },
    {
        "id": "first_nft",
        "title": "Коллекционер",
        "description": "Купи свой первый артефакт",
        "reward": 150,
        "type": "nft",
        "required": 1
    },
    {
        "id": "first_purchase",
        "title": "Инвестор",
        "description": "Пополни баланс через Звездную Биржу.",
        "reward": 150,
        "type": "purchase",
        "required": 1
    },
    {
        "id": "transfer_5",
        "title": "Казначей штаба",
        "description": "Сделай 5 переводов",
        "reward": 40,
        "type": "transfer",
        "required": 5
    },
    {
        "id": "nft_3",
        "title": "Хранитель реликвий",
        "description": "Собери 5 артефактов",
        "reward": 1000,
        "type": "nft",
        "required": 5
    },
]

DAILY_POOL = [
    {"id": "daily_login",     "title": "Явка",             "description": "Открой приложение сегодня",              "reward": 2},
    {"id": "daily_leaderboard","title": "Разведка",         "description": "Загляни в таблицу лидеров",             "reward": 1},
    {"id": "daily_shop",      "title": "Обход рынка",       "description": "Открой вкладку Биржа",                  "reward": 1},
    {"id": "daily_transfer",  "title": "Связной",           "description": "Отправь перевод кому-нибудь сегодня",   "reward": 10},
    {"id": "daily_bot",       "title": "Сеанс связи",       "description": "Напиши боту любое сообщение",           "reward": 2},
    {"id": "daily_nfts",      "title": "Инвентаризация",    "description": "Открой вкладку Мои NFT",                "reward": 1},
    {"id": "daily_check_bal", "title": "Аудит счёта",       "description": "Проверь баланс на главной странице",    "reward": 1},
    {"id": "daily_invite",    "title": "Вербовка",          "description": "Пригласи кого-нибудь в бот сегодня",    "reward": 10},
]

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
                balance INTEGER DEFAULT 0,
                user_id BIGINT,
                referred_by TEXT,
                referral_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS inventory (
                id SERIAL PRIMARY KEY,
                username TEXT REFERENCES users(username),
                nft_id TEXT,
                UNIQUE(username, nft_id)
            );
            CREATE TABLE IF NOT EXISTS quest_progress (
                username TEXT,
                quest_id TEXT,
                progress INTEGER DEFAULT 0,
                done BOOLEAN DEFAULT FALSE,
                claimed BOOLEAN DEFAULT FALSE,
                PRIMARY KEY (username, quest_id)
            );
            CREATE TABLE IF NOT EXISTS daily_quests (
                username TEXT,
                quest_date DATE,
                quest_ids TEXT,
                done_ids TEXT DEFAULT '',
                PRIMARY KEY (username, quest_date)
            );
            CREATE TABLE IF NOT EXISTS transfers (
                id SERIAL PRIMARY KEY,
                from_user TEXT,
                to_user TEXT,
                amount INTEGER,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS user_id BIGINT;")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referred_by TEXT;")
        await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS referral_count INTEGER DEFAULT 0;")

async def check_user(username: str, user_id: int = None):
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        if row is None:
            await conn.execute(
                "INSERT INTO users (username, balance, user_id) VALUES ($1, 0, $2) ON CONFLICT DO NOTHING",
                username.lower(), user_id
            )
            return 0
        if user_id:
            await conn.execute("UPDATE users SET user_id = $1 WHERE username = $2 AND user_id IS NULL", user_id, username.lower())
        return row['balance']

async def update_balance(username: str, amount: int):
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE username = $2", amount, username.lower())
        row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        if row:
            return row['balance']
        return 0

async def get_quest_progress(username: str):
    pool = await get_db()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT quest_id, progress, done, claimed FROM quest_progress WHERE username = $1", username.lower())
        return {r['quest_id']: dict(r) for r in rows}

async def update_quest_progress(username: str, quest_type: str, increment: int = 1):
    pool = await get_db()
    relevant = [q for q in STATIC_QUESTS if q['type'] == quest_type]
    async with pool.acquire() as conn:
        for quest in relevant:
            row = await conn.fetchrow(
                "SELECT progress, done FROM quest_progress WHERE username = $1 AND quest_id = $2",
                username.lower(), quest['id']
            )
            if row and row['done']:
                continue
            current = row['progress'] if row else 0
            new_progress = current + increment
            done = new_progress >= quest['required']
            await conn.execute("""
                INSERT INTO quest_progress (username, quest_id, progress, done)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (username, quest_id) DO UPDATE
                SET progress = $3, done = $4
            """, username.lower(), quest['id'], new_progress, done)

async def get_or_create_daily(username: str):
    pool = await get_db()
    today = date.today()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT quest_ids, done_ids FROM daily_quests WHERE username = $1 AND quest_date = $2",
            username.lower(), today
        )
        if row:
            ids = row['quest_ids'].split(',') if row['quest_ids'] else []
            done = row['done_ids'].split(',') if row['done_ids'] else []
            return ids, [d for d in done if d]
        chosen = random.sample([q['id'] for q in DAILY_POOL], min(3, len(DAILY_POOL)))
        await conn.execute(
            "INSERT INTO daily_quests (username, quest_date, quest_ids, done_ids) VALUES ($1, $2, $3, '')",
            username.lower(), today, ','.join(chosen)
        )
        return chosen, []

async def complete_daily(username: str, quest_id: str):
    pool = await get_db()
    today = date.today()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT quest_ids, done_ids FROM daily_quests WHERE username = $1 AND quest_date = $2",
            username.lower(), today
        )
        if not row:
            return False, 0
        ids = row['quest_ids'].split(',')
        done = [d for d in row['done_ids'].split(',') if d]
        if quest_id not in ids or quest_id in done:
            return False, 0
        quest_info = next((q for q in DAILY_POOL if q['id'] == quest_id), None)
        if not quest_info:
            return False, 0
        done.append(quest_id)
        await conn.execute(
            "UPDATE daily_quests SET done_ids = $1 WHERE username = $2 AND quest_date = $3",
            ','.join(done), username.lower(), today
        )
        await update_balance(username, quest_info['reward'])
        return True, quest_info['reward']

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
            nft_count = len(inventory)
            transfer_count = await conn.fetchval(
                "SELECT COUNT(*) FROM transfers WHERE from_user = $1", username.lower()
            )
            purchase_count = await conn.fetchval(
                "SELECT COUNT(*) FROM quest_progress WHERE username = $1 AND quest_id = 'first_purchase' AND done = TRUE",
                username.lower()
            )
            ref_row = await conn.fetchrow("SELECT referral_count FROM users WHERE username = $1", username.lower())
            ref_count = ref_row['referral_count'] if ref_row else 0

        progress_map = await get_quest_progress(username)

        static_tasks = []
        for q in STATIC_QUESTS:
            p = progress_map.get(q['id'], {})
            current_progress = p.get('progress', 0)
            done = p.get('done', False)
            claimed = p.get('claimed', False)

            if q['type'] == 'referral':
                current_progress = ref_count
                done = ref_count >= q['required']
            elif q['type'] == 'transfer':
                current_progress = transfer_count
                done = transfer_count >= q['required']
            elif q['type'] == 'nft':
                current_progress = nft_count
                done = nft_count >= q['required']
            elif q['type'] == 'purchase':
                current_progress = 1 if purchase_count else 0
                done = bool(purchase_count)

            static_tasks.append({
                "id": q['id'],
                "title": q['title'],
                "description": q['description'],
                "reward": q['reward'],
                "progress": current_progress,
                "required": q['required'],
                "done": done,
                "claimed": claimed,
                "type": "static"
            })

        daily_ids, done_daily_ids = await get_or_create_daily(username)
        daily_tasks = []
        for qid in daily_ids:
            info = next((q for q in DAILY_POOL if q['id'] == qid), None)
            if info:
                daily_tasks.append({
                    "id": info['id'],
                    "title": info['title'],
                    "description": info['description'],
                    "reward": info['reward'],
                    "done": qid in done_daily_ids,
                    "claimed": qid in done_daily_ids,
                    "type": "daily"
                })

        return {
            "username": username,
            "balance": balance,
            "inventory": inventory,
            "tasks": static_tasks,
            "daily_tasks": daily_tasks
        }
    except Exception as e:
        print(f"API Error (User): {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

@app.post("/api/claim_quest/{username}/{quest_id}")
async def api_claim_quest(username: str, quest_id: str):
    try:
        pool = await get_db()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT done, claimed FROM quest_progress WHERE username = $1 AND quest_id = $2",
                username.lower(), quest_id
            )

            quest_info = next((q for q in STATIC_QUESTS if q['id'] == quest_id), None)
            if not quest_info:
                raise HTTPException(status_code=404, detail="Quest not found")

            if row and row['claimed']:
                raise HTTPException(status_code=400, detail="Already claimed")

            inv_rows = await conn.fetch("SELECT nft_id FROM inventory WHERE username = $1", username.lower())
            nft_count = len(inv_rows)
            transfer_count = await conn.fetchval("SELECT COUNT(*) FROM transfers WHERE from_user = $1", username.lower())
            ref_row = await conn.fetchrow("SELECT referral_count FROM users WHERE username = $1", username.lower())
            ref_count = ref_row['referral_count'] if ref_row else 0
            purchase_count = await conn.fetchval(
                "SELECT COUNT(*) FROM quest_progress WHERE username = $1 AND quest_id = 'first_purchase' AND done = TRUE",
                username.lower()
            )

            done = False
            if quest_info['type'] == 'referral':
                done = ref_count >= quest_info['required']
            elif quest_info['type'] == 'transfer':
                done = transfer_count >= quest_info['required']
            elif quest_info['type'] == 'nft':
                done = nft_count >= quest_info['required']
            elif quest_info['type'] == 'purchase':
                done = bool(purchase_count)

            if not done:
                raise HTTPException(status_code=400, detail="Quest not completed yet")

            await conn.execute("""
                INSERT INTO quest_progress (username, quest_id, progress, done, claimed)
                VALUES ($1, $2, $3, TRUE, TRUE)
                ON CONFLICT (username, quest_id) DO UPDATE SET done = TRUE, claimed = TRUE
            """, username.lower(), quest_id, quest_info['required'])

        new_balance = await update_balance(username, quest_info['reward'])
        return {"status": "success", "reward": quest_info['reward'], "balance": new_balance}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Claim quest error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/complete_daily/{username}/{quest_id}")
async def api_complete_daily(username: str, quest_id: str):
    try:
        success, reward = await complete_daily(username, quest_id)
        if not success:
            raise HTTPException(status_code=400, detail="Quest already done or not assigned today")
        pool = await get_db()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", username.lower())
        return {"status": "success", "reward": reward, "balance": row['balance'] if row else 0}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Complete daily error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/transfer/{from_user}/{to_user}/{amount}")
async def api_transfer(from_user: str, to_user: str, amount: int):
    try:
        if amount <= 0:
            raise HTTPException(status_code=400, detail="Invalid amount")
        if from_user.lower() == to_user.lower():
            raise HTTPException(status_code=400, detail="Cannot transfer to yourself")
        pool = await get_db()
        async with pool.acquire() as conn:
            sender = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", from_user.lower())
            if not sender or sender['balance'] < amount:
                raise HTTPException(status_code=400, detail="Insufficient balance")
            receiver = await conn.fetchrow("SELECT username FROM users WHERE username = $1", to_user.lower())
            if not receiver:
                raise HTTPException(status_code=404, detail="Recipient not found")
            async with conn.transaction():
                await conn.execute("UPDATE users SET balance = balance - $1 WHERE username = $2", amount, from_user.lower())
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE username = $2", amount, to_user.lower())
                await conn.execute(
                    "INSERT INTO transfers (from_user, to_user, amount) VALUES ($1, $2, $3)",
                    from_user.lower(), to_user.lower(), amount
                )
            new_balance = await conn.fetchval("SELECT balance FROM users WHERE username = $1", from_user.lower())

        await update_quest_progress(from_user, 'transfer', 1)
        return {"status": "success", "balance": new_balance}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Transfer error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
        await update_quest_progress(username, 'nft', 1)
        return {"status": "success", "nft_id": nft_id}
    except HTTPException:
        raise
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
            title="Пакет монеток",
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
    user_id = message.from_user.id
    args = message.text.split()
    referrer = None

    if len(args) > 1 and args[1].startswith("ref_"):
        referrer = args[1][4:].lower()

    await check_user(username, user_id)

    if referrer and referrer != username:
        pool = await get_db()
        async with pool.acquire() as conn:
            already = await conn.fetchrow("SELECT referred_by FROM users WHERE username = $1", username.lower())
            if already and not already['referred_by']:
                await conn.execute(
                    "UPDATE users SET referred_by = $1 WHERE username = $2",
                    referrer, username.lower()
                )
                await conn.execute(
                    "UPDATE users SET referral_count = referral_count + 1 WHERE username = $1",
                    referrer
                )
                ref_reward = 300
                await update_balance(referrer, ref_reward)
                try:
                    ref_row = await conn.fetchrow("SELECT user_id FROM users WHERE username = $1", referrer)
                    if ref_row and ref_row['user_id']:
                        await bot.send_message(
                            ref_row['user_id'],
                            f"🎉 По вашей ссылке зарегистрировался @{username}!\nВам начислено *{ref_reward} SNC* ❄️",
                            parse_mode="Markdown"
                        )
                except Exception:
                    pass

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
        balance = await check_user(username)
        ref_link = f"https://t.me/{(await bot.get_me()).username}?start=ref_{username}"
        msg = (
            f"👋 Приветствуем, *{username}*!\n\n"
            f"Текущие активы: *{balance}* SNC ❄️\n\n"
            f"🔗 Ваша реферальная ссылка:\n`{ref_link}`\n"
            f"За каждого приглашённого — *300 SNC*"
        )
        await message.answer(msg, parse_mode="Markdown", reply_markup=get_keyboard())

    await complete_daily(username, "daily_bot")

@dp.message(Command("mysnc"))
async def cmd_mysnc(message: types.Message):
    username = get_username(message.from_user)
    balance = await check_user(username, message.from_user.id)
    await message.answer(f"❄️ Информация по счету *{username}*:\nБаланс: *{balance}* SNC", parse_mode="Markdown")

@dp.message(Command("ref"))
async def cmd_ref(message: types.Message):
    username = get_username(message.from_user)
    await check_user(username, message.from_user.id)
    pool = await get_db()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT referral_count FROM users WHERE username = $1", username.lower())
    ref_count = row['referral_count'] if row else 0
    bot_info = await bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{username}"
    await message.answer(
        f"🔗 *Реферальная программа*\n\n"
        f"Ваша ссылка:\n`{ref_link}`\n\n"
        f"Приглашено бойцов: *{ref_count}*\n"
        f"За каждого — *300 SNC*",
        parse_mode="Markdown"
    )

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
                    f"🔔 *Запрос на возврат Stars*\n\n"
                    f"Пользователь: @{username} (`{message.from_user.id}`)\n"
                    f"Запросил возврат средств.\n\n"
                    f"Чтобы оформить возврат:\n"
                    f"`/starsrefund @{username} <количество_звёзд>`",
                    parse_mode="Markdown"
                )
            except Exception as e:
                print(f"Failed to notify admin {admin_id}: {e}")

@dp.message(Command("starsrefund_revoke"))
async def cmd_starsrefund_revoke(message: types.Message):
    if message.from_user.id not in ADMIN_IDS or message.chat.type != "private":
        return
    pattern = r"^/starsrefund_revoke\s+@?(\w+)$"
    match = re.match(pattern, message.text or "")
    if not match:
        await message.answer(
            "⚠️ Неверный формат.\nИспользуй: `/starsrefund_revoke @username`",
            parse_mode="Markdown"
        )
        return
    target_username = match.group(1).lower()
    pool = await get_db()
    async with pool.acquire() as conn:
        purchases = await conn.fetch("""
            SELECT quest_id FROM quest_progress
            WHERE username = $1 AND quest_id = 'first_purchase' AND done = TRUE
        """, target_username)
        if not purchases:
            await message.answer(f"❌ У @{target_username} нет записей о покупках SNC.")
            return
        user_row = await conn.fetchrow("SELECT balance FROM users WHERE username = $1", target_username)
        if not user_row:
            await message.answer(f"❌ Пользователь @{target_username} не найден.")
            return
        transfer_rows = await conn.fetch("""
            SELECT amount FROM transfers WHERE to_user = $1
            AND created_at > NOW() - INTERVAL '30 days'
            ORDER BY created_at DESC LIMIT 1
        """, target_username)

    snc_to_remove = 0
    for row in await pool.acquire().__aenter__() if False else []:
        pass

    async with pool.acquire() as conn:
        last_purchase = await conn.fetchrow("""
            SELECT amount FROM transfers WHERE to_user = $1
            ORDER BY created_at DESC LIMIT 1
        """, target_username)

    snc_to_remove = last_purchase['amount'] if last_purchase else 0

    if snc_to_remove <= 0:
        await message.answer(
            f"⚠️ Не удалось определить сумму последней покупки @{target_username}.\n"
            f"Используй `/addsnc @{target_username} -<сумма>` вручную.",
            parse_mode="Markdown"
        )
        return

    new_balance = await update_balance(target_username, -snc_to_remove)
    if new_balance < 0:
        await update_balance(target_username, snc_to_remove)
        await message.answer(f"❌ У @{target_username} недостаточно SNC для списания {snc_to_remove}.")
        return

    await message.answer(
        f"🚫 *Возврат отклонён — SNC списаны*\n\n"
        f"Аккаунт: @{target_username}\n"
        f"Списано: {snc_to_remove} SNC\n"
        f"Новый баланс: {new_balance} SNC",
        parse_mode="Markdown"
    )

@dp.message(F.chat.type == "private")
async def handle_private_logic(message: types.Message):
    user_id = message.from_user.id
    username = get_username(message.from_user)
    await check_user(username, user_id)

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

    await complete_daily(username, "daily_bot")
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
    pool = await get_db()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO quest_progress (username, quest_id, progress, done, claimed)
            VALUES ($1, 'first_purchase', 1, TRUE, FALSE)
            ON CONFLICT (username, quest_id) DO NOTHING
        """, target_user.lower())
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