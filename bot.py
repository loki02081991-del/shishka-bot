# bot.py — SHISHKA RESTOBAR (final unified)
# Brand: "Шишка Restobar — там где браслет решает"
# Features:
# - Guest menu: codes (17:00–19:00, valid to 02:00), reservations wizard, address, menu placeholder, promos, channel
# - Admin panel: r_today, r_find, r_confirm/cancel, purge expired codes, day stats, broadcast notify, /admin inline panel
# - Daily reminder at 16:55 (5 min before window) to ADMIN_NOTIFY_CHAT_IDS
# - Daily report at 03:00 with yesterday stats to ADMIN_NOTIFY_CHAT_IDS
# - Access mode open/closed with approve/block
# Requires: Python 3.11+, aiogram==3.13.1, python-dotenv, tzdata (Windows)
from aiogram import F
import os
import asyncio
from datetime import datetime, timezone
import sqlite3
import random
from datetime import datetime, timedelta, date
from typing import Optional, List, Union
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
# === Logging setup ===
import logging

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "bot.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("shishka-bot")


import requests
from dotenv import load_dotenv

load_dotenv()
IIKO_API_KEY = os.getenv("IIKO_API_KEY")  # или напрямую как строку

def get_iiko_token():
    try:
        url = f"https://m1.iiko.cards/api/0/auth/access_token?apiLogin={IIKO_API_KEY}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["token"]
    except Exception as e:
        logger.error("[IIKO] Не удалось получить токен: %s", e)
        return None


def find_guest_in_iiko(phone: str) -> dict | None:
    token = get_iiko_token()
    if not token:
        return None

    url = "https://m1.iiko.cards/api/0/loyalty/customer/info"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"phone": phone.strip().replace(" ", "")}

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=10)
        print("[IIKO][DEBUG]", resp.status_code, resp.text)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print("[IIKO][ERROR] Поиск карты:", e)
        return None

INVISIBLE = "\u2063"  # невидимый символ, безопасный для пустых сообщений


APP_VERSION = "SHISHKA bot v4.0 final"
TZ = ZoneInfo("Asia/Tashkent")

# ===== ENV =====
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path, override=True)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

def _parse_int_list(raw: str) -> List[int]:
    raw = (raw or "").replace(" ", "").strip()
    out: List[int] = []
    for p in raw.split(","):
        s = p.strip()
        if s and s.lstrip("-").isdigit():
            out.append(int(s))
    return out
def _parse_hhmm(raw: str) -> tuple[int, int]:
    """Парсит строку формата HH:MM в кортеж (часы, минуты)."""
    try:
        h, m = raw.split(":")
        return int(h), int(m)
    except Exception:
        return (0, 0)

def _parse_targets(raw: str) -> List[Union[int, str]]:
    raw = (raw or "").replace(" ", "").strip()
    out: List[Union[int, str]] = []
    for p in raw.split(","):
        s = p.strip()
        if not s: 
            continue
        if s.startswith("@"):
            out.append(s)
        elif s.lstrip("-").isdigit():
            out.append(int(s))
    return out

ADMIN_IDS = _parse_int_list(os.getenv("ADMIN_IDS", ""))
# по умолчанию уведомления в @Restobar_Shishka
default_notify = "@Restobar_Shishka"
ADMIN_NOTIFY_CHAT_IDS = _parse_targets(os.getenv("ADMIN_NOTIFY_CHAT_IDS", default_notify))

ADDRESS = os.getenv("ADDRESS", "ул Кичик Миробод 26 Shishka Restobar").strip()
MAP_URL = os.getenv("MAP_URL", f"https://maps.google.com/?q={ADDRESS.replace(' ', '%20')}")
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/Restobar_Shishka")

ACCESS_MODE = (os.getenv("ACCESS_MODE", "open") or "open").lower()   # open | closed
ACCESS_HINT = os.getenv("ACCESS_HINT", "Бот по приглашению. Мы свяжемся с вами после проверки.")

logger.info("[BOOT] VERSION: %s", APP_VERSION)
print("[BOOT] ADMIN_IDS:", ADMIN_IDS)
print("[BOOT] ADMIN_NOTIFY_CHAT_IDS:", ADMIN_NOTIFY_CHAT_IDS)
print("[BOOT] ACCESS_MODE:", ACCESS_MODE)
print("[BOOT] ADDRESS:", ADDRESS)

# === Prize broadcast schedule (from .env or defaults) ===
PRIZE_DAY    = int(os.getenv("PRIZE_DAY", "6"))      # 0=Mon ... 6=Sun
PRIZE_HOUR   = int(os.getenv("PRIZE_HOUR", "18"))
PRIZE_MINUTE = int(os.getenv("PRIZE_MINUTE", "0"))


# ===== aiogram =====
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    BotCommand, BotCommandScopeDefault, BotCommandScopeChat
    
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import BotCommandScopeChatAdministrators

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ===== DB (SQLite) =====
DB_PATH = os.path.join(os.path.dirname(__file__), "codes.db")
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;")
conn.execute("PRAGMA synchronous=NORMAL;")
cur = conn.cursor()

# === GUESTS ===
cur.execute("""
CREATE TABLE IF NOT EXISTS guests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    user_id INTEGER NOT NULL
);
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS feedbacks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  text TEXT,
  photo_id TEXT,
  created_at TEXT NOT NULL
);
""")

# users
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  tg_first_name TEXT,
  tg_last_name  TEXT,
  tg_username   TEXT,
  name          TEXT,
  phone         TEXT,
  guest_count   INTEGER NOT NULL DEFAULT 1,
  source        TEXT,
  approved      INTEGER NOT NULL DEFAULT 0,
  blocked       INTEGER NOT NULL DEFAULT 0,
  joined_at     TEXT NOT NULL,
  last_seen     TEXT NOT NULL
);
""")
# reservations
cur.execute("""
CREATE TABLE IF NOT EXISTS reservations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER,
  guest_name TEXT NOT NULL,
  guest_phone TEXT NOT NULL,
  covers INTEGER NOT NULL,
  r_date TEXT NOT NULL,  -- YYYY-MM-DD
  r_time TEXT NOT NULL,  -- HH:MM
  note TEXT,
  status TEXT NOT NULL DEFAULT 'new',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_res_date ON reservations(r_date)")
cur.execute("CREATE INDEX IF NOT EXISTS idx_res_phone ON reservations(guest_phone)")

# codes
cur.execute("""
CREATE TABLE IF NOT EXISTS codes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id   INTEGER NOT NULL,
  code      TEXT    NOT NULL,
  issued_at TEXT    NOT NULL,
  expires_at TEXT   NOT NULL,
  day_key   TEXT    NOT NULL,   -- YYYY-MM-DD (Ташкент)
  valid     INTEGER NOT NULL DEFAULT 1
);
""")

# prizes (гости с призами)
cur.execute("""
CREATE TABLE IF NOT EXISTS prizes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guest_name TEXT NOT NULL,
  guest_phone TEXT NOT NULL,
  prize TEXT NOT NULL,
  user_id INTEGER,
  created_at TEXT NOT NULL
);
""")

# === RANDOM REWARDS ===
cur.execute("""
CREATE TABLE IF NOT EXISTS random_rewards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    prize TEXT NOT NULL,
    reward_code TEXT NOT NULL,
    date_issued TEXT NOT NULL
);
""")
cur.execute("CREATE INDEX IF NOT EXISTS idx_reward_code ON random_rewards(reward_code)")

# Расширяем таблицу random_rewards, если нет нужных полей
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN redeemed INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN redeemed_by_user_id INTEGER")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN redeemed_by_username TEXT")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN redeemed_by_fullname TEXT")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN redeemed_at TEXT")
except sqlite3.OperationalError:
    pass
    
conn.commit()
# --- Создание доп. колонок (безопасно, игнорируем ошибки) ---
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN expiry_date TEXT")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN notified_24h INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN expired INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass
conn.commit()

conn.commit()
cur.execute("CREATE INDEX IF NOT EXISTS idx_codes_user_day ON codes(user_id, day_key)")
conn.commit()
# --- Создание доп. колонок (безопасно, игнорируем ошибки) ---
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN expiry_date TEXT")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN notified_24h INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN expired INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass
# 🔧 добавляем недостающие поля победителя (если их ещё нет)
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN winner_username TEXT")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE random_rewards ADD COLUMN winner_fullname TEXT")
except sqlite3.OperationalError:
    pass
conn.commit()

# ===== Helpers =====
def now_tz() -> datetime:
    return datetime.now(TZ)

def ymd(dt: datetime | date) -> str:
    if isinstance(dt, date) and not isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")

def upsert_user_from_tg(msg: Message, source: Optional[str] = None):
    uid = msg.from_user.id
    fn  = (msg.from_user.first_name or "").strip()
    ln  = (msg.from_user.last_name  or "").strip()
    un  = (msg.from_user.username   or "").strip()
    now = now_tz().isoformat()
    cur.execute("SELECT user_id FROM users WHERE user_id=?", (uid,))
    if cur.fetchone() is None:
        expiry = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        cur.execute("""
          INSERT INTO users (user_id, tg_first_name, tg_last_name, tg_username, name, phone, guest_count,
                             source, approved, blocked, joined_at, last_seen)
          VALUES (?, ?, ?, ?, NULL, NULL, 1, ?, 0, 0, ?, ?)
        """, (uid, fn, ln, un, (source or None), now, now))
    else:
        cur.execute("""
          UPDATE users SET tg_first_name=?, tg_last_name=?, tg_username=?, last_seen=? WHERE user_id=?
        """, (fn, ln, un, now, uid))
    conn.commit()

def is_admin(user_id: int) -> bool:
    return str(user_id) in [str(x) for x in ADMIN_IDS]


def is_blocked(user_id: int) -> bool:
    cur.execute("SELECT blocked FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    return bool(r and r[0])

def is_approved(user_id: int) -> bool:
    if ACCESS_MODE != "closed":
        return True
    cur.execute("SELECT approved FROM users WHERE user_id=?", (user_id,))
    r = cur.fetchone()
    return bool(r and r[0])

def approve_user(user_id: int):
    cur.execute("UPDATE users SET approved=1, blocked=0 WHERE user_id=?", (user_id,))
    conn.commit()

def block_user(user_id: int):
    cur.execute("UPDATE users SET blocked=1, approved=0 WHERE user_id=?", (user_id,))
    conn.commit()

async def safe_reply(msg: Message, text: str, **kwargs):
    """Безопасная отправка сообщения пользователю (с меню, если возможно)."""
    try:
        await msg.answer(text, reply_markup=main_reply_kb(), **kwargs)
    except Exception as e:
        print(f"[safe_reply][WARN] Не удалось отправить сообщение: {e}")

# ===== Codes: window & discounts =====
CODES_WINDOW_START = _parse_hhmm(os.getenv("CODES_WINDOW_START", "12:00"))
CODES_WINDOW_END   = _parse_hhmm(os.getenv("CODES_WINDOW_END", "19:00"))
VALID_UNTIL_HHMM   = _parse_hhmm(os.getenv("VALID_UNTIL_HHMM", "22:00"))

# 0=Mon ... 6=Sun
DISCOUNTS = {0: 40, 1: 30, 2: 30, 3: 30, 4: 20, 5: 20, 6: 30}  # Sun=30%

def discount_for_date(dt: datetime) -> int:
    return DISCOUNTS.get(dt.weekday(), 0)

def today_discount() -> int:
    return discount_for_date(now_tz())

def is_in_window(dt: datetime) -> bool:
    s_h, s_m = CODES_WINDOW_START
    e_h, e_m = CODES_WINDOW_END
    start = dt.replace(hour=s_h, minute=s_m, second=0, microsecond=0)
    end   = dt.replace(hour=e_h, minute=e_m, second=0, microsecond=0)
    return start <= dt <= end

def valid_until_for_day(dt: datetime) -> datetime:
    v_h, v_m = VALID_UNTIL_HHMM
    return (dt + timedelta(days=1)).replace(hour=v_h, minute=v_m, second=0, microsecond=0)

def gen_code(n: int = 6) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choice(alphabet) for _ in range(n))

def user_code_for_day(user_id: int, day_key: str):
    cur.execute("SELECT id, code, issued_at, expires_at, valid FROM codes WHERE user_id=? AND day_key=? LIMIT 1",
                (user_id, day_key))
    return cur.fetchone()

def create_code_for_user(user_id: int):
    now = now_tz()
    day_key = ymd(now)
    row = user_code_for_day(user_id, day_key)
    if row:
        _id, code, issued_at, expires_at, valid = row
        return code, datetime.fromisoformat(issued_at), datetime.fromisoformat(expires_at)
    code = gen_code(6)
    issued_at = now
    expires_at = valid_until_for_day(now)
    cur.execute("""
      INSERT INTO codes (user_id, code, issued_at, expires_at, day_key, valid)
      VALUES (?, ?, ?, ?, ?, 1)
    """, (user_id, code, issued_at.isoformat(), expires_at.isoformat(), day_key))
    conn.commit()
    return code, issued_at, expires_at

def invalidate_expired():
    cur.execute("UPDATE codes SET valid=0 WHERE valid=1 AND expires_at<=?", (now_tz().isoformat(),))
    conn.commit()

# ===== Reservations =====
def create_reservation(user_id: Optional[int], name: str, phone: str, covers: int, r_date: str, r_time: str, note: Optional[str]):
    now = now_tz().isoformat()
    cur.execute("""
        INSERT INTO reservations (user_id, guest_name, guest_phone, covers, r_date, r_time, note, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)
    """, (user_id, name.strip(), phone.strip(), max(1, covers), r_date, r_time, (note or None), now, now))
    conn.commit()
    return cur.lastrowid

def get_reservations_by_date(r_date: str):
    cur.execute("""
        SELECT id, guest_name, guest_phone, covers, r_time, status, note
        FROM reservations WHERE r_date=? ORDER BY r_time ASC
    """, (r_date,))
    return cur.fetchall()

def find_reservations_by_phone(phone: str):
    cur.execute("""
        SELECT id, guest_name, guest_phone, covers, r_date, r_time, status
        FROM reservations WHERE guest_phone LIKE ?
        ORDER BY r_date DESC, r_time DESC
    """, (f"%{phone}%",))
    return cur.fetchall()

def set_res_status(res_id: int, status: str):
    cur.execute("UPDATE reservations SET status=?, updated_at=? WHERE id=?", (status, now_tz().isoformat(), res_id))
    conn.commit()

# ===== Notifications =====
def _notify_targets() -> List[Union[int, str]]:
    targets = list(ADMIN_NOTIFY_CHAT_IDS) + [x for x in ADMIN_IDS]
    seen = set()
    res = []
    for t in targets:
        key = str(t)
        if key not in seen:
            seen.add(key)
            res.append(t)
    return res

async def notify_admins(text: str, kb: Optional[InlineKeyboardBuilder] = None):
    for cid in _notify_targets():
        try:
            if kb:
                await bot.send_message(cid, text, reply_markup=kb.as_markup(), disable_web_page_preview=True)
            else:
                await bot.send_message(cid, text, disable_web_page_preview=True)
        except Exception as e:
            print(f"[NOTIFY][ERROR] chat_id={cid}: {e}")

    # ===== Button labels (constants) =====
BTN_REG  = "🧾 Регистрация"
BTN_CODE = "🎟 Получить код"
BTN_RES  = "🍽 Забронировать стол"
BTN_ADDR = "📍 Адрес"
BTN_MENU = "🍴 Меню ресторана"
BTN_ACT  = "📢 Акции / события"
BTN_LUCK = "🎲 Испытай удачу"
BTN_FEED = "💬 Отзывы / фото"
BTN_PRIZE = "🎁 Узнать свой приз"
BTN_MY_CARD = "📇 Моя карта"

def main_reply_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_CODE), KeyboardButton(text=BTN_RES)],
            [KeyboardButton(text=BTN_LUCK), KeyboardButton(text=BTN_PRIZE)],
            [KeyboardButton(text=BTN_MENU), KeyboardButton(text=BTN_ADDR)],
            [KeyboardButton(text=BTN_FEED)]
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие..."
    )


def inline_main_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()
    kb.button(text=BTN_REG,  callback_data="register_guest")
    kb.button(text=BTN_CODE, callback_data="get_code")
    kb.button(text=BTN_RES,  callback_data="reserve")
    kb.button(text=BTN_ADDR, callback_data="address")
    kb.button(text=BTN_MENU, callback_data="menu_food")
    kb.button(text=BTN_ACT,  url=CHANNEL_URL)
    kb.button(text=BTN_LUCK, callback_data="try_luck")
    kb.adjust(1)
    return kb

def admin_panel_kb() -> InlineKeyboardBuilder:
    kb = InlineKeyboardBuilder()

    # Раздел "Брони"
    kb.button(text="📅 Брони на сегодня", callback_data="adm_today")
    kb.button(text="📊 Статистика за день", callback_data="adm_stats")

    # Раздел "Пользователи"
    kb.button(text="👥 Пользователи", callback_data="adm_users")
    kb.button(text="🧊 Неактивные", callback_data="adm_inactive")

    # Раздел "Призы"
    kb.button(text="🎁 Список призов", callback_data="adm_prizes")
    kb.button(text="➕ Добавить приз", callback_data="adm_add_prize")

    # Раздел "Техническое"
    kb.button(text="🔑 Проверить / Удалить код", callback_data="redeem_code")
    kb.button(text="🧹 Очистить коды", callback_data="adm_purge")
    kb.button(text="♻️ Перезапуск", callback_data="adm_restart")

    kb.adjust(2)
    return kb



# ===== Commands =====
async def set_commands():
    # Команды для обычных пользователей
    user_cmds = [
        BotCommand(command="start", description="Главное меню"),
        BotCommand(command="code", description="Получить код (12:00–19:00)"),
        BotCommand(command="address", description="Наш адрес"),
    ]
    # Команды для админов (чистые, только нужное)
    admin_cmds = [
        BotCommand(command="admin", description="Админ-панель"),
        BotCommand(command="stats", description="Быстрый отчёт за день"),
        BotCommand(command="restart", description="♻️ Перезапуск бота"),
    ]
    await bot.set_my_commands(user_cmds, scope=BotCommandScopeDefault())
    await bot.set_my_commands(admin_cmds)



    for admin_id in ADMIN_IDS:
        try:
            await bot.set_my_commands(admin_cmds, scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception:
            pass

# ===== Access gate =====
async def _guard_access_and_notify_admins(msg: Message) -> bool:
    if is_blocked(msg.from_user.id):
        await msg.answer("Доступ закрыт.")
        return True
    if ACCESS_MODE == "closed":
        cur.execute("SELECT approved FROM users WHERE user_id=?", (msg.from_user.id,))
        r = cur.fetchone()
        if not (r and r[0]):
            await msg.answer(ACCESS_HINT)
            kb = InlineKeyboardBuilder()
            kb.button(text=f"✅ Одобрить {msg.from_user.id}", callback_data=f"approve:{msg.from_user.id}")
            kb.button(text=f"⛔ Заблокировать {msg.from_user.id}", callback_data=f"block:{msg.from_user.id}")
            kb.adjust(1)
            txt = (
                "🆕 Запрос на доступ:\n"
                f"ID: <code>{msg.from_user.id}</code>\n"
                f"Имя: {msg.from_user.first_name or ''} {msg.from_user.last_name or ''} @{msg.from_user.username or ''}"
            )
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, txt, reply_markup=kb.as_markup())
                except Exception:
                    pass
            return True
    return False

@dp.message(Command("start"))
async def cmd_start(msg: Message):
    user_id = msg.from_user.id
    name = msg.from_user.full_name

    upsert_user_from_tg(msg)

    # Проверка доступа (ожидает модерации — выход)
    if await _guard_access_and_notify_admins(msg):
        return

    # ✅ Если доступ есть, проверяем — есть ли уже номер
    cur.execute("SELECT 1 FROM guests WHERE user_id = ?", (user_id,))
    has_phone = cur.fetchone()

    if not has_phone:
        kb = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
            resize_keyboard=True
        )
        await msg.answer(
            "📲 Пожалуйста, поделитесь номером телефона для подтверждения доступа.",
            reply_markup=kb
        )
        return

    # 📲 Если номер есть — показать главное меню
    await msg.answer(
    f"👋 Привет, {msg.from_user.first_name or 'гость'}!\n"
    "Добро пожаловать в SHISHKA RESTOBAR 🍸\n"
    "Выберите действие ниже:",
    reply_markup=main_reply_kb()
)


def is_registered(user_id: int) -> bool:
    cur.execute("SELECT 1 FROM guests WHERE user_id = ?", (user_id,))
    return bool(cur.fetchone())


@dp.message(F.text == BTN_REG)
async def btn_register_guest(msg: Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True
    )
    await msg.answer("Поделитесь номером телефона:", reply_markup=kb)
    
@dp.message(F.text == BTN_FEED)
async def feedback_start(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return

    WAIT_FEEDBACK[msg.from_user.id] = {"started_at": now_tz()}
    await msg.answer(
        "📝 Напишите отзыв одним или несколькими сообщениями.\n"
        "Можно прикрепить до 10 фото (или по одному).\n\n"
        "Когда закончите — отправьте слово <b>ГОТОВО</b>.",
        reply_markup=main_reply_kb()
    )

@dp.message(F.text == BTN_CODE)
async def btn_get_code(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return
    await code_cmd(msg)


@dp.message(F.text == BTN_RES)
async def btn_reserve(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return
    await start_reserve_flow_from_message(msg)

@dp.message(F.text == BTN_MENU)
async def btn_menu_food(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return

    url = "https://Shishkaone.myresto.online"
    await msg.answer(
        f"🍽 <b>Меню ресторана SHISHKA RESTOBAR</b>\n\n"
        f"Ознакомьтесь с блюдами и напитками по ссылке ниже 👇\n"
        f"🔗 <a href='{url}'>Открыть меню</a>",
        disable_web_page_preview=False
    )


@dp.message(F.text == BTN_LUCK)
async def btn_try_luck(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return
    await run_try_luck_from_message(msg)


@dp.message(F.text == BTN_ACT)
async def btn_promos_exact(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return
    await msg.answer("🎉 Акции / события: следите в нашем канале!\n" + CHANNEL_URL)

@dp.message(F.text == BTN_ADDR)
async def btn_address(msg: Message):
    if not is_registered(msg.from_user.id):
        await msg.answer("⛔ Сначала зарегистрируйтесь — нажмите /start и отправьте номер телефона.")
        return
    kb = InlineKeyboardBuilder()
    kb.button(text="🗺 Открыть на карте", url=MAP_URL)
    kb.adjust(1)
    await msg.answer(
        f"📍 <b>{ADDRESS}</b>\n"
        "Ждём вас в Shishka Restobar 🍸",
        reply_markup=kb.as_markup()
    )
    
@dp.message(Command("version"))
async def version(msg: Message):
    await msg.answer(APP_VERSION)

@dp.message(lambda m: m.from_user and m.from_user.id in WAIT_FEEDBACK and (m.text or "").strip().lower() == "готово")
async def feedback_done(msg: Message):
    WAIT_FEEDBACK.pop(msg.from_user.id, None)
    await msg.answer("✅ Спасибо за отзыв! Мы его посмотрим как можно скорее.")
async def _send_to_owner(text: str = "", photo_file_id: str | None = None):
    # ЛИЧНО ТЕБЕ: отправляем всем ADMIN_IDS; если пусто — в первый из ADMIN_NOTIFY_CHAT_IDS
    targets: list[Union[int, str]] = ADMIN_IDS[:] or (ADMIN_NOTIFY_CHAT_IDS[:1])
    for cid in targets:
        try:
            if photo_file_id:
                await bot.send_photo(cid, photo=photo_file_id, caption=text or " ")
            else:
                await bot.send_message(cid, text, disable_web_page_preview=True)
        except Exception as e:
            print("[FEEDBACK][ERROR]", e)

def _fmt_user_line(m: Message) -> str:
    u = m.from_user
    name = (u.first_name or "") + (" " + u.last_name if u.last_name else "")
    uname = f"@{u.username}" if u.username else ""
    return f"👤 {name} {uname}\n🆔 <code>{u.id}</code>"

@dp.message(lambda m: m.from_user and m.from_user.id in WAIT_FEEDBACK and (m.text or m.caption))
async def feedback_text(msg: Message):
    # текст/подпись
    text = (msg.text or msg.caption or "").strip()
    header = "💬 <b>Новый отзыв</b>\n" + _fmt_user_line(msg)
    body = f"\n\n{text}" if text else ""
    await _send_to_owner(header + body)
    # подтверждение гостю (можно убрать, если много сообщений)
    await msg.answer("✅ Большое спасибо.")

@dp.message(lambda m: m.from_user and m.from_user.id in WAIT_FEEDBACK and m.photo)
async def feedback_photo(msg: Message):
    # берём самое большое фото
    file_id = msg.photo[-1].file_id
    caption = (msg.caption or "").strip()
    header = "🖼 <b>Фото к отзыву</b>\n" + _fmt_user_line(msg)
    body = f"\n\n{caption}" if caption else ""
    await _send_to_owner(header + body, photo_file_id=file_id)
    await msg.answer("🖼 Фото получено.")

@dp.message(F.contact)
async def contact_handler(msg: Message):
    """Обработка контакта, сохраняем в базу guests"""
    phone = msg.contact.phone_number
    name = msg.from_user.full_name
    user_id = msg.from_user.id

    cur.execute("SELECT id FROM guests WHERE phone = ?", (phone,))
    exists = cur.fetchone()

    if exists:
        cur.execute(
            "UPDATE guests SET name = ?, user_id = ? WHERE phone = ?",
            (name, user_id, phone)
        )
        await msg.answer("✅ Ваш профиль обновлён! Теперь вы можете получать призы 🎁")
    else:
        cur.execute(
            "INSERT INTO guests (name, phone, user_id) VALUES (?, ?, ?)",
            (name, phone, user_id)
        )

        # 🔧 Временно отключена интеграция с iiko:
        # register_guest_in_iiko(name, phone, user_id)

        await msg.answer("✅ Вы зарегистрированы! Теперь вы участвуете в акциях 🎉")

    # Привязка приза к user_id, если телефон совпадает
    cur.execute("""
        UPDATE prizes
        SET user_id = ?
        WHERE guest_phone = ?
    """, (user_id, phone))

    conn.commit()

    # Главное меню после успешной регистрации
    await msg.answer(
        "📋 Добро пожаловать в SHISHKA RESTOBAR! С браслетом действуют особые цены 🍸",
        reply_markup=main_reply_kb()
    )



@dp.message(Command("myid"))
async def myid(msg: Message):
    await msg.answer(f"🆔 Ваш Telegram ID: {msg.from_user.id}")

@dp.message(Command("address"))
async def address_cmd(msg: Message):
    await msg.answer(f"📍 <b>Адрес:</b> {ADDRESS}\n🗺 <a href='{MAP_URL}'>Открыть на карте</a>", disable_web_page_preview=True)

@dp.message(Command("users"))
async def count_users(msg: Message):
    """Показывает количество подписчиков"""
    if not is_admin(msg.from_user.id):
        return

    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]

    cur.execute("SELECT joined_at FROM users ORDER BY joined_at DESC LIMIT 1")
    last = cur.fetchone()
    last_join = datetime.fromisoformat(last[0]).strftime("%d.%m %H:%M") if last else "—"

    await msg.answer(f"👥 Всего пользователей: <b>{total}</b>\n🕒 Последний вход: {last_join}")

@dp.callback_query(F.data == "address")
async def cb_address(cb: CallbackQuery):
    await cb.message.answer(f"📍 <b>Адрес:</b> {ADDRESS}\n🗺 <a href='{MAP_URL}'>Открыть на карте</a>", disable_web_page_preview=True)
    await cb.answer()
# ===== Helpers to reuse for ReplyKeyboard buttons =====
@dp.callback_query(F.data == "adm_users")
async def cb_adm_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): 
        return await cb.answer()
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]
    await cb.message.answer(f"👥 Всего пользователей: <b>{total}</b>")
    await cb.answer()


@dp.callback_query(F.data == "adm_inactive")
async def cb_adm_inactive(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    limit_date = (now_tz() - timedelta(days=30)).isoformat()
    cur.execute("SELECT COUNT(*) FROM users WHERE last_seen < ?", (limit_date,))
    inactive = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]
    await cb.message.answer(f"🧊 Неактивных более 30 дней: <b>{inactive}</b> из {total}")
    await cb.answer()


@dp.callback_query(F.data == "adm_prizes")
async def cb_adm_prizes(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): 
        return await cb.answer()
    cur.execute("SELECT id, guest_name, guest_phone, prize FROM prizes")
    rows = cur.fetchall()
    if not rows:
        await cb.message.answer("🎁 Список призов пуст.")
        return await cb.answer()
    lines = [f"#{pid} {name} ({phone}) — {prize}" for pid, name, phone, prize in rows]
    await cb.message.answer("🎁 <b>Призы:</b>\n" + "\n".join(lines))
    await cb.answer()


@dp.callback_query(F.data == "adm_add_prize")
async def cb_adm_add_prize(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    await cb.message.answer(
        "Введите вручную:\n<code>/add_prize Имя +9989XXXXXXX Приз</code>\n"
        "Пример:\n<code>/add_prize Азиз +998901234567 Кальян</code>"
    )
    await cb.answer()


@dp.callback_query(F.data == "adm_restart")
async def cb_adm_restart(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return await cb.answer()
    await cb.message.answer("♻️ Перезапуск бота...\n(занимает несколько секунд)")
    import os, sys
    os.execv(sys.executable, ['python'] + sys.argv)

async def start_reserve_flow_from_message(msg: Message):
    """Запускаем мастер бронирования от текстовой кнопки."""
    if is_blocked(msg.from_user.id) or (ACCESS_MODE == "closed" and not is_approved(msg.from_user.id)):
        await msg.answer(ACCESS_HINT)
        return
    RES_TMP[msg.from_user.id] = {"step": "name", "data": {}}
    await msg.answer("📝 Введите имя для брони:")

async def run_try_luck_from_message(msg: Message):
    """Розыгрыш призов от текстовой кнопки (логика из cb_try_luck)."""
    user_id = msg.from_user.id
    now = now_tz()
    today = now.date()

    # Проверяем, играл ли гость в последние 7 дней
    cur.execute(
        "SELECT date_issued FROM random_rewards WHERE user_id=? ORDER BY date_issued DESC LIMIT 1",
        (user_id,),
    )
    row = cur.fetchone()
    if row:
        last_play = datetime.fromisoformat(row[0]).date()
        if (today - last_play).days < 7:
            await msg.answer("🎮 Вы уже играли на этой неделе! Попробуйте позже 😉")
            return

    # Рандомайзер призов
    prizes = [
        "🍺 Бокал пива",
        "🎟 Браслет который решает",
        "🍽 Кофе на выбор ",
        "🥗 Салат греческий",
        "🥗 Салат Оливье",
        "💨 Кальян",
        "🎁 40% скидка на браслет",
        "🎁 50% скидка на браслет",
        "🎁 30% скидка на браслет",
        "🍺 2 Бокала пива",
        "🥗 Салат Цезарь",
        "🥗 Наливка ",
        "🎟 Бесплатный браслет другу с которым вы пришли",

        
    ]
    prize = random.choice(prizes)

    # Генерируем уникальный код
    reward_code = ''.join(random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(6))

            # === Сохраняем приз сразу с датой окончания ===
    expiry_date = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()

    cur.execute("""
    INSERT INTO random_rewards 
    (user_id, prize, reward_code, date_issued, expiry_date, notified_24h, expired,
     winner_username, winner_fullname)
    VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?)
""", (
    user_id,
    prize,
    reward_code,
    now.isoformat(),
    expiry_date,
    msg.from_user.username,
    msg.from_user.full_name
))

    conn.commit()


    # Сообщение пользователю
    if "Подарочный купон" in prize:
        text = (
            f"🎉 <b>Поздравляем!</b>\n"
            f"Вы выиграли <b>{prize}</b>!\n\n"
            f"Передайте другу этот купон:\n<code>{reward_code}</code>\n"
            f"Он даёт 50% скидку на браслет! 🩶"
        )
    else:
        text = (
            f"🎉 <b>Поздравляем!</b>\n"
            f"Вы выиграли <b>{prize}</b>!\n"
            f"Ваш код: <code>{reward_code}</code>\n\n"
            f"Покажите этот код при визите в Shishka Restobar 💫"
        )
    await safe_reply(msg, text)

    # Уведомим админов
    await notify_admins(f"🎲 Игрок @{msg.from_user.username or msg.from_user.full_name} выиграл: {prize} (код: {reward_code})")

@dp.callback_query(F.data == "menu_food")
async def cb_menu_food(cb: CallbackQuery):
    url = "https://Shishkaone.myresto.online"
    await cb.message.answer(
        f"🍽 <b>Меню ресторана SHISHKA RESTOBAR</b>\n\n"
        f"Посмотреть все блюда можно по ссылке 👇\n"
        f"🔗 <a href='{url}'>Открыть меню</a>",
        disable_web_page_preview=False
    )
    await cb.answer()


@dp.callback_query(F.data == "promos")
async def cb_promos(cb: CallbackQuery):
    await cb.message.answer("🎉 Акции / события: следите в нашем канале!\n" + CHANNEL_URL)
    await cb.answer()

# codes: callback & /code
@dp.callback_query(F.data == "get_code")
async def cb_get_code(cb: CallbackQuery):
    if is_blocked(cb.from_user.id) or (ACCESS_MODE == "closed" and not is_approved(cb.from_user.id)):
        await cb.message.answer(ACCESS_HINT); return await cb.answer()
    now = now_tz()
    if not is_in_window(now):
        s_h,s_m=CODES_WINDOW_START; e_h,e_m=CODES_WINDOW_END
        await cb.message.answer(f"Коды выдаются только с <b>{s_h:02d}:{s_m:02d}</b> до <b>{e_h:02d}:{e_m:02d}</b>.")
        return await cb.answer()
    code, issued_at, expires_at = create_code_for_user(cb.from_user.id)
    disc = today_discount()
    await cb.message.answer(
        "🎟 <b>Ваш код на браслет</b>\n"
        f"Код: <code>{code}</code>\n"
        f"Скидка сегодня: <b>{disc}%</b>\n"
        f"Выдан: {issued_at.strftime('%H:%M')} | Действует до: {expires_at.strftime('%H:%M')}"
    )
    await cb.answer()

@dp.message(Command("code"))
async def code_cmd(msg: Message):
    if is_blocked(msg.from_user.id) or (ACCESS_MODE == "closed" and not is_approved(msg.from_user.id)):
        return await msg.answer(ACCESS_HINT)
    now = now_tz()
    if not is_in_window(now):
        s_h,s_m=CODES_WINDOW_START; e_h,e_m=CODES_WINDOW_END
        return await msg.answer(f"Коды выдаются только с <b>{s_h:02d}:{s_m:02d}</b> до <b>{e_h:02d}:{e_m:02d}</b>.")
    code, issued_at, expires_at = create_code_for_user(msg.from_user.id)
    disc = today_discount()
    await msg.answer(
        "🎟 <b>Ваш код на браслет</b>\n"
        f"Код: <code>{code}</code>\n"
        f"Скидка сегодня: <b>{disc}%</b>\n"
        f"Выдан: {issued_at.strftime('%H:%M')} | Действует до: {expires_at.strftime('%H:%M')}"
    )

# ===== Reservations wizard =====
RES_TMP: dict[int, dict] = {}
# ждём от пользователя отзыв (режим обратной связи)
WAIT_FEEDBACK: dict[int, dict] = {}

@dp.callback_query(F.data == "reserve")
async def reserve_start(cb: CallbackQuery):
    if is_blocked(cb.from_user.id) or not is_approved(cb.from_user.id):
        await cb.message.answer(ACCESS_HINT); return await cb.answer()
    RES_TMP[cb.from_user.id] = {"step":"name","data":{}}
    await cb.message.answer("📝 Введите имя для брони:")
    await cb.answer()

@dp.message(lambda m: RES_TMP.get(m.from_user.id, {}).get("step") == "name")
async def res_get_name(msg: Message):
    RES_TMP[msg.from_user.id]["data"]["name"] = (msg.text or "").strip()[:60]
    RES_TMP[msg.from_user.id]["step"] = "phone"
    await msg.answer("📞 Введите телефон (например, +998901234567):")

@dp.message(lambda m: RES_TMP.get(m.from_user.id, {}).get("step") == "phone")
async def res_get_phone(msg: Message):
    phone = (msg.text or "").strip()
    if len(phone) < 7:
        return await msg.answer("Похоже на некорректный номер. Введите ещё раз:")
    RES_TMP[msg.from_user.id]["data"]["phone"] = phone
    RES_TMP[msg.from_user.id]["step"] = "date"
    today = ymd(now_tz())
    await msg.answer(f"📆 Дата визита YYYY-MM-DD (например, {today}):")

@dp.message(lambda m: RES_TMP.get(m.from_user.id, {}).get("step") == "date")
async def res_get_date(msg: Message):
    d = (msg.text or "").strip()
    try:
        _ = datetime.strptime(d, "%Y-%m-%d")
    except Exception:
        return await msg.answer("Неверная дата. Введите в формате YYYY-MM-DD:")
    RES_TMP[msg.from_user.id]["data"]["date"] = d
    RES_TMP[msg.from_user.id]["step"] = "time"
    await msg.answer("⏰ Время визита HH:MM (например, 20:00):")

@dp.message(lambda m: RES_TMP.get(m.from_user.id, {}).get("step") == "time")
async def res_get_time(msg: Message):
    t = (msg.text or "").strip()
    try:
        _ = datetime.strptime(t, "%H:%M")
    except Exception:
        return await msg.answer("Неверное время. Введите в формате HH:MM:")
    RES_TMP[msg.from_user.id]["data"]["time"] = t
    RES_TMP[msg.from_user.id]["step"] = "covers"
    await msg.answer("👥 Количество гостей (цифрой):")

@dp.message(lambda m: RES_TMP.get(m.from_user.id, {}).get("step") == "covers")
async def res_get_covers(msg: Message):
    try:
        covers = max(1, int((msg.text or "").strip()))
    except Exception:
        return await msg.answer("Введите количество гостей числом, например 4:")
    RES_TMP[msg.from_user.id]["data"]["covers"] = covers
    RES_TMP[msg.from_user.id]["step"] = "note"
    await msg.answer("✍️ Пожелания (опционально). Если нет — отправьте «-»:")

@dp.message(lambda m: RES_TMP.get(m.from_user.id, {}).get("step") == "note")
async def res_get_note(msg: Message):
    data = RES_TMP[msg.from_user.id]["data"]
    note = None if (msg.text or "").strip() == "-" else (msg.text or "").strip()[:200]
    rid = create_reservation(
        user_id=msg.from_user.id,
        name=data["name"], phone=data["phone"], covers=data["covers"],
        r_date=data["date"], r_time=data["time"], note=note
    )
    RES_TMP.pop(msg.from_user.id, None)
    await msg.answer(
        "✅ Бронь принята!\n\n"
        f"Номер: <code>{rid}</code>\n"
        f"Имя: {data['name']}\n"
        f"Телефон: {data['phone']}\n"
        f"Дата/время: {data['date']} {data['time']}\n"
        f"Гостей: {data['covers']}\n"
        f"Статус: new\n"
        f"{('Пожелание: ' + note) if note else ''}"
    )
    kb = InlineKeyboardBuilder()
    kb.button(text=f"✅ Подтвердить #{rid}", callback_data=f"approve_res:{rid}")
    kb.button(text=f"🛑 Отменить #{rid}", callback_data=f"cancel_res:{rid}")
    kb.adjust(2)
    ulink = f"@{msg.from_user.username}" if msg.from_user.username else f"id {msg.from_user.id}"
    await notify_admins(
        "🆕 <b>Новая бронь</b>\n"
        f"#{rid} — {data['date']} {data['time']}\n"
        f"👤 {data['name']} | 📞 {data['phone']}\n"
        f"👥 Гостей: {data['covers']}\n"
        f"✍️ Пожелание: {note or '—'}\n"
        f"Источник: {ulink}",
        kb=kb
    )

@dp.callback_query(F.data.startswith("approve_res:"))
async def cb_res_approve(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    rid = int(cb.data.split(":", 1)[1])
    set_res_status(rid, "confirmed")
    await cb.message.answer(f"✅ Бронь #{rid} подтверждена.")
    await cb.answer()

@dp.callback_query(F.data.startswith("cancel_res:"))
async def cb_res_cancel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    rid = int(cb.data.split(":", 1)[1])
    set_res_status(rid, "cancelled")
    await cb.message.answer(f"🛑 Бронь #{rid} отменена.")
    await cb.answer()

# ===== Admin: commands and panel =====
@dp.message(Command("admin"))
async def admin_panel(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    await msg.answer("🛠 Админ-панель", reply_markup=admin_panel_kb().as_markup())

@dp.callback_query(F.data == "adm_today")
async def cb_adm_today(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    today = ymd(now_tz())
    rows = get_reservations_by_date(today)
    if not rows:
        await cb.message.answer("На сегодня броней нет."); return await cb.answer()
    lines = ["📆 Брони на сегодня:"]
    for (rid, name, phone, covers, r_time, status, note) in rows:
        line = f"#{rid} {r_time} — {name} ({phone}), гостей: {covers}, статус: {status}"
        if note: line += f"\n   ✍️ {note}"
        lines.append(line)
    await cb.message.answer("\n".join(lines)); await cb.answer()

@dp.callback_query(F.data == "adm_purge")
async def cb_adm_purge(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    before = cur.execute("SELECT COUNT(*) FROM codes WHERE valid=1").fetchone()[0]
    invalidate_expired()
    after = cur.execute("SELECT COUNT(*) FROM codes WHERE valid=1").fetchone()[0]
    await cb.message.answer(f"🧹 Просроченные коды аннулированы.\nАктивных было: {before}, стало: {after}.")
    await cb.answer()

def stats_for_day(day: date) -> tuple[int,int,int]:
    dkey = ymd(day)
    cur.execute("SELECT COUNT(*) FROM codes WHERE day_key=?", (dkey,))
    codes_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM reservations WHERE r_date=?", (dkey,))
    res_count = cur.fetchone()[0]
    disc = DISCOUNTS.get(day.weekday(), 0)
    return codes_count, res_count, disc

@dp.callback_query(F.data == "adm_stats")
async def cb_adm_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): 
        return await cb.answer()
    today = now_tz().date()
    codes, resv, disc = stats_for_day(today)
    await cb.message.answer(
        f"📊 Сегодня ({ymd(today)}):\n"
        f"🎟 Выдано кодов: <b>{codes}</b>\n"
        f"🍽 Брони: <b>{resv}</b>\n"
        f"💰 Скидка дня: <b>{disc}%</b>"
    )
    await cb.answer()


@dp.callback_query(F.data == "adm_broadcast")
async def cb_adm_broadcast(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    await cb.message.answer("Отправь текст рассылки ответом на это сообщение.")
    await cb.answer()
@dp.callback_query(F.data == "adm_test_prizes")
async def cb_adm_test_prizes(cb: CallbackQuery):
    """Кнопка из админ-панели для тестовой рассылки"""
    if not is_admin(cb.from_user.id):
        return await cb.answer()

    # Чтобы не копировать всю логику, просто вызываем /test_prizes вручную
    msg = cb.message
    await test_prizes(msg)
    await cb.answer()


@dp.message(Command("stats"))
async def stats_cmd(msg: Message):
    if not is_admin(msg.from_user.id): return
    today = now_tz().date()
    codes, resv, disc = stats_for_day(today)
    await msg.answer(f"📊 Сегодня ({ymd(today)}):\n— Выдано кодов: {codes}\n— Брони: {resv}\n— Скидка дня: {disc}%")
import os
import sys

@dp.message(Command("restart"))
async def restart_bot(msg: Message):
    """Перезапуск бота вручную (только для админов)"""
    if msg.from_user.id not in ADMIN_IDS:
        await msg.answer("⛔ У вас нет прав для перезапуска бота.")
        return

    await msg.answer("♻️ Перезапуск бота...")

    # Отправляем уведомление в консоль
    print(f"[SYSTEM] Бот перезапускается по команде от @{msg.from_user.username} ({msg.from_user.id})")

    # Закрываем соединения с БД, если нужно
    try:
        conn.commit()
        conn.close()
    except Exception:
        pass

    # Перезапуск процесса
    os.execv(sys.executable, ['python'] + sys.argv)

@dp.message(Command("purge"))
async def purge_cmd(msg: Message):
    if not is_admin(msg.from_user.id): return
    before = cur.execute("SELECT COUNT(*) FROM codes WHERE valid=1").fetchone()[0]
    invalidate_expired()
    after = cur.execute("SELECT COUNT(*) FROM codes WHERE valid=1").fetchone()[0]
    await msg.answer(f"🧹 Просроченные коды аннулированы.\nАктивных было: {before}, стало: {after}.")

@dp.message(Command("r_today"))
async def r_today(msg: Message):
    if not is_admin(msg.from_user.id): return
    rows = get_reservations_by_date(ymd(now_tz()))
    if not rows: return await msg.answer("На сегодня броней нет.")
    lines = ["📆 Брони на сегодня:"]
    for (rid, name, phone, covers, r_time, status, note) in rows:
        line = f"#{rid} {r_time} — {name} ({phone}), гостей: {covers}, статус: {status}"
        if note: line += f"\n   ✍️ {note}"
        lines.append(line)
    await msg.answer("\n".join(lines))

@dp.message(Command("r_find"))
async def r_find(msg: Message):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2: return await msg.answer("Использование: <code>/r_find 90</code>")
    rows = find_reservations_by_phone(parts[1].strip())
    if not rows: return await msg.answer("Ничего не найдено.")
    lines = ["🔎 Найденные брони:"]
    for (rid, name, phone, covers, r_date, r_time, status) in rows[:30]:
        lines.append(f"#{rid} {r_date} {r_time} — {name} ({phone}), гостей: {covers}, статус: {status}")
    await msg.answer("\n".join(lines))

@dp.message(Command("r_confirm"))
async def r_confirm(msg: Message):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await msg.answer("Использование: <code>/r_confirm 123</code>")
    set_res_status(int(parts[1]), "confirmed")
    await msg.answer(f"✅ Бронь #{parts[1]} подтверждена.")

@dp.message(Command("r_cancel"))
async def r_cancel(msg: Message):
    if not is_admin(msg.from_user.id): return
    parts = msg.text.split()
    if len(parts) != 2 or not parts[1].isdigit():
        return await msg.answer("Использование: <code>/r_cancel 123</code>")
    set_res_status(int(parts[1]), "cancelled")
    await msg.answer(f"🛑 Бронь #{parts[1]} отменена.")

@dp.message(Command("notify_test"))
async def notify_test(msg: Message):
    if not is_admin(msg.from_user.id): return
    text = "🔔 Тест уведомления от бота."
    await notify_admins(text)
    await msg.answer("✅ Разослано.")
    # ===== PRIZES MODULE =====
def create_prize(name: str, phone: str, prize: str, user_id: int | None = None):
    cur.execute("""
        INSERT INTO prizes (guest_name, guest_phone, prize, user_id, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (name, phone, prize, user_id, now_tz().isoformat()))
    conn.commit()

def get_all_prizes():
    cur.execute("SELECT id, guest_name, guest_phone, prize FROM prizes ORDER BY id ASC")
    return cur.fetchall()

def del_prize(pid: int):
    cur.execute("DELETE FROM prizes WHERE id=?", (pid,))
    conn.commit()

def clear_prizes():
    cur.execute("DELETE FROM prizes")
    conn.commit()
@dp.message(Command("test_prizes"))
async def test_prizes(msg: Message):
    """Тестовая рассылка призов"""
    if not is_admin(msg.from_user.id):
        await msg.answer("⛔ Нет доступа")
        return

    await msg.answer("🎁 Запускаем тестовую рассылку призов...")

    sent_ok = 0
    sent_fail = 0

    # Берём все призы и связываем с user_id из таблицы guests
    cur.execute("""
        SELECT 
            p.guest_name, 
            p.guest_phone, 
            p.prize, 
            g.user_id
        FROM prizes p
        LEFT JOIN guests g 
        ON REPLACE(g.phone, '+', '') LIKE '%' || REPLACE(p.guest_phone, '+', '') || '%'
    """)
    prizes = cur.fetchall()

    for prize in prizes:
        name, phone, prize_name, user_id = prize

        text = (
            "🎉 <b>Shishka Restobar</b> — там, где браслет решает!\n"
            "Мы помним о вашем призе,но и вы захватите свой купон с собой :) 🎁\n"
            f"Ваш приз: <b>{prize_name}</b>\n\n"
            "📅 Забронируйте стол и используйте свой подарок уже сегодня!"
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="🍽 Забронировать стол", url="https://t.me/Restobar_Shishka")

        try:
            if user_id:
                await bot.send_message(user_id, text, reply_markup=kb.as_markup(), parse_mode="HTML")
                sent_ok += 1
                print(f"[OK] Приз отправлен {name} ({phone}) — {prize_name}")
            else:
                await notify_admins(
                    f"⚠️ Не удалось отправить сообщение призёру\n"
                    f"{name} ({phone}) — {prize_name}\n"
                    f"(гость не активировал бота)\n"
                    f"📩 Приз будет автоматически отправлен после регистрации."
                )
                print(f"[WAITING] {name} ({phone}) ожидает активации для приза: {prize_name}")
                sent_fail += 1

        except Exception as e:
            logger.warning("[PRIZE] %s", e)
            sent_fail += 1

    await msg.answer(
        f"📬 Тестовая рассылка завершена.\n"
        f"✅ Отправлено: {sent_ok}\n"
        f"🚫 Не доставлено: {sent_fail}"
    )

# ====== UPDATED PRIZE COMMANDS ======
@dp.message(Command("add_prize"))
async def add_prize(msg: Message):
    """Добавление приза и мгновенное уведомление гостя (если активен)"""
    if not is_admin(msg.from_user.id):
        return

    parts = msg.text.split(maxsplit=3)
    if len(parts) < 4:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Добавить приз", switch_inline_query_current_chat="/add_prize Имя +9989XXXXXXX Приз")
        kb.adjust(1)
        return await msg.answer(
            "📋 Чтобы добавить гостя с призом, используйте формат:\n\n"
            "<code>/add_prize Имя +9989XXXXXXX Приз</code>\n\n"
            "Пример:\n<code>/add_prize Азиз +998901234567 Вино бокал</code>",
            reply_markup=kb.as_markup()
        )

    name, phone, prize = parts[1], parts[2], parts[3]
    create_prize(name, phone, prize)

    # ищем активного пользователя по номеру
    cur.execute("""
        SELECT user_id FROM guests
        WHERE REPLACE(phone, '+', '') LIKE '%' || REPLACE(?, '+', '') || '%'
    """, (phone,))
    row = cur.fetchone()

    if row:
        user_id = row[0]
        kb = InlineKeyboardBuilder()
        kb.button(text="🍽 Забронировать стол", callback_data="reserve")
        kb.adjust(1)
        text = (
            "🎉 <b>Shishka Restobar</b>\n"
            "Вы получили подарок!\n\n"
            f"🎁 <b>{prize}</b>\n\n"
            "👉 Забронируйте стол и используйте свой подарок уже сегодня!"
        )
        try:
            await bot.send_message(user_id, text, reply_markup=kb.as_markup())
            await msg.answer(f"✅ Приз добавлен и отправлен пользователю!\n👤 {name}\n📞 {phone}\n🎁 {prize}")
            await notify_admins(f"📤 Приз отправлен гостю @{user_id}: {prize}")
        except Exception as e:
            await msg.answer(f"⚠️ Добавлен, но не удалось отправить сообщение ({e})")
    else:
        await msg.answer(f"✅ Добавлен приз (гость не активировал бота):\n👤 {name}\n📞 {phone}\n🎁 {prize}")
        await notify_admins(
            f"🆕 Добавлен приз (гость не найден в боте):\n👤 {name}\n📞 {phone}\n🎁 {prize}\n"
            f"Он получит напоминание в пятницу {PRIZE_HOUR:02d}:{PRIZE_MINUTE:02d}."
        )



@dp.message(Command("prizes"))
async def prizes_list(msg: Message):
    """Вывод списка призов"""
    if not is_admin(msg.from_user.id):
        return
    rows = get_all_prizes()
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить приз", callback_data="add_prize_hint")
    kb.adjust(1)
    if not rows:
        return await msg.answer("🎁 Список призов пуст.", reply_markup=kb.as_markup())
    lines = [f"#{pid} {name} ({phone}) — {prize}" for pid, name, phone, prize in rows]
    await msg.answer("🎁 <b>Текущие призы:</b>\n" + "\n".join(lines), reply_markup=kb.as_markup())
    
@dp.callback_query(F.data == "add_prize_hint")
async def cb_add_prize_hint(cb: CallbackQuery):
    await cb.message.answer(
        "📋 Чтобы добавить гостя с призом, введите:\n\n"
        "<code>/add_prize Имя +9989XXXXXXX Приз</code>\n\n"
        "Пример:\n<code>/add_prize Азиз +998901234567 Вино MSA бокал</code>"
    )
    await cb.answer()

@dp.message(Command("del_prize"))
async def prize_delete(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].isdigit():
        return await msg.answer("Использование: /del_prize ID")
    pid = int(parts[1])
    del_prize(pid)
    await msg.answer(f"🗑 Приз #{pid} удалён.")

@dp.message(Command("clear_prizes"))
async def prizes_clear(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    clear_prizes()
    await msg.answer("🧹 Все призы удалены.")


@dp.message(Command("whereami"))
async def whereami(msg: Message):
    if not is_admin(msg.from_user.id): return
    await msg.answer(f"chat.id = <code>{msg.chat.id}</code>\nchat.type = {msg.chat.type}\nchat.title = {msg.chat.title}")
@dp.message(Command("rewards"))
async def list_rewards(msg: Message):
    """Показать, кто что выиграл"""
    if not is_admin(msg.from_user.id):
        return

    cur.execute("SELECT user_id, prize, date_issued FROM random_rewards ORDER BY date_issued DESC")
    rows = cur.fetchall()

    if not rows:
        return await msg.answer("🎲 История розыгрышей пуста.")

    lines = []
    for user_id, prize, date_issued in rows:
        date_obj = datetime.fromisoformat(date_issued)
        date_str = date_obj.strftime("%d.%m %H:%M")
        lines.append(f"👤 {user_id} — {prize} ({date_str})")

    text = "🎲 <b>История розыгрышей:</b>\n" + "\n".join(lines[:30])
    await safe_reply(msg, text)

@dp.message(Command("redeem"))
async def redeem_code(msg: Message):
    """Погашение кода админом с уведомлением"""
    if not is_admin(msg.from_user.id):
        return

    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        return await msg.answer("Использование: <code>/redeem CODE</code>\nПример: <code>/redeem ABC123</code>")

    code = parts[1].strip().upper()

    # Ищем код
    cur.execute("""
        SELECT id, user_id, prize, date_issued, redeemed
        FROM random_rewards
        WHERE reward_code = ?
        LIMIT 1
    """, (code,))
    row = cur.fetchone()

    if not row:
        return await msg.answer(f"❌ Код <code>{code}</code> не найден.")
    
    rid, user_id, prize, date_issued, redeemed = row

    if redeemed:
        cur.execute("""
            SELECT redeemed_by_fullname, redeemed_by_username, redeemed_at
            FROM random_rewards
            WHERE id = ?
        """, (rid,))
        used = cur.fetchone()
        if used:
            name, username, used_at = used
            await msg.answer(
                f"⚠️ Код уже был использован.\n"
                f"👤 {name or username or 'Неизвестно'}\n"
                f"🕒 {used_at or 'Без даты'}"
            )
        else:
            await msg.answer("⚠️ Этот код уже был использован ранее.")
        return

    # Отмечаем как использованный
    redeemer_id = msg.from_user.id
    redeemer_username = msg.from_user.username
    redeemer_fullname = msg.from_user.full_name
    from datetime import datetime, timedelta, timezone
    tz = timezone(timedelta(hours=5))  # Ташкент
    redeemed_at = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


    cur.execute("""
        UPDATE random_rewards
        SET redeemed = 1,
            redeemed_by_user_id = ?,
            redeemed_by_username = ?,
            redeemed_by_fullname = ?,
            redeemed_at = ?
        WHERE id = ?
    """, (redeemer_id, redeemer_username, redeemer_fullname, redeemed_at, rid))
    conn.commit()

            # Уведомляем всех админов
    # Получаем данные о настоящем победителе (из winner_*)
    cur.execute("""
        SELECT winner_username, winner_fullname 
        FROM random_rewards 
        WHERE id = ?
    """, (rid,))
    winner_data = cur.fetchone()

    if winner_data:
        winner_username, winner_fullname = winner_data
    else:
        winner_username, winner_fullname = None, None

    # Пробуем получить номер телефона победителя из таблицы guests
    cur.execute("SELECT phone FROM guests WHERE user_id = ?", (user_id,))
    phone_row = cur.fetchone()
    winner_phone = phone_row[0] if phone_row and phone_row[0] else '—'

    text_admin = (
        f"🎟 <b>Код погашен!</b>\n"
        f"🎫 Код: <code>{code}</code>\n"
        f"🏆 Приз: {prize}\n"
        f"👤 Погасил: @{redeemer_username or '—'}\n"
        f"🕒 Время: {redeemed_at}\n"
        f"📩 Выиграл: @{winner_username or '—'} ({winner_phone})"
    )


    for aid in ADMIN_IDS:
        try:
            await bot.send_message(aid, text_admin)
        except Exception as e:
            print(f"[NOTIFY][ERROR] не удалось отправить админу {aid}: {e}")
            pass

    await msg.answer(f"✅ Код <code>{code}</code> подтверждён.\n🎁 Приз: <b>{prize}</b>")

    try:
        await bot.send_message(user_id, f"🔔 Ваш код <code>{code}</code> был успешно использован. Спасибо! 🎉")
    except Exception:
        pass

@dp.message(Command("inactive_report"))
async def inactive_report(msg: Message):
    if not is_admin(msg.from_user.id):
        return
    limit_date = (now_tz() - timedelta(days=30)).isoformat()
    cur.execute("SELECT COUNT(*) FROM users WHERE last_seen < ?", (limit_date,))
    inactive = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]
    await msg.answer(f"🧊 Неактивных более 30 дней: <b>{inactive}</b> из {total}")

@dp.message(F.text == "🎁 Узнать свой приз")
async def show_all_prizes(msg: Message):
    """Показать все призы пользователя"""
    user_id = msg.from_user.id

    cur.execute("""
        SELECT prize, reward_code, date_issued, redeemed, redeemed_at
        FROM random_rewards
        WHERE user_id = ?
        ORDER BY id DESC
    """, (user_id,))
    results = cur.fetchall()

    if not results:
        await msg.answer("😔 У вас пока нет выигрышей.\nНажмите 🎲 <b>Испытай удачу</b>, чтобы сыграть!")
        return

    message_lines = ["🎉 <b>Ваши призы:</b>\n"]
    for idx, (prize, code, date, used, redeemed_at) in enumerate(results, start=1):
        try:
            date_str = datetime.fromisoformat(date).strftime("%d.%m.%Y %H:%M")
        except Exception:
            date_str = date
        if used:
            used_time = redeemed_at or "—"
            status = f"⚠️ <b>Уже использован</b> ({used_time})"
        else:
            status = "🟢 <b>Активен</b>"
        message_lines.append(
            f"{idx}. <b>{prize}</b>\n"
            f"🔢 Код: <code>{code}</code>\n"
            f"📅 Выдан: {date_str}\n"
            f"{status}\n"
        )

    text = "\n".join(message_lines)
    await msg.answer(text, reply_markup=main_reply_kb())



# Access callbacks
@dp.callback_query(F.data.startswith("approve:"))
async def cb_approve(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    uid = int(cb.data.split(":", 1)[1])
    approve_user(uid)
    try:
        await bot.send_message(uid, "✅ Доступ одобрен. Нажмите /start")
    except Exception:
        pass
    await cb.message.answer(f"✅ Одобрен доступ для {uid}")
    await cb.answer()

@dp.callback_query(F.data.startswith("block:"))
async def cb_block(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return await cb.answer()
    uid = int(cb.data.split(":", 1)[1])
    block_user(uid)
    try:
        await bot.send_message(uid, "⛔ Доступ закрыт.")
    except Exception:
        pass
    await cb.message.answer(f"⛔ Заблокирован {uid}")
    await cb.answer()
    
@dp.callback_query(F.data == "redeem_code")
async def cb_redeem_prompt(cb: CallbackQuery):
    """Показывает подсказку админу, как ввести код для удаления"""
    if not is_admin(cb.from_user.id):
        return

    kb = InlineKeyboardBuilder()
    kb.button(text="🧾 Ввести код", switch_inline_query_current_chat="/redeem CODE")
    kb.adjust(1)

    await cb.message.answer(
        "🔑 Чтобы удалить использованный код, введи команду в формате:\n\n"
        "<code>/redeem CODE</code>\n\n"
        "Пример:\n<code>/redeem ABC123</code>",
        reply_markup=kb.as_markup()
    )
    await cb.answer()
   

# ===== Background jobs =====
def stats_for_day_ymd_str(ymd_str: str) -> tuple[int,int,int]:
    cur.execute("SELECT COUNT(*) FROM codes WHERE day_key=?", (ymd_str,))
    codes_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM reservations WHERE r_date=?", (ymd_str,))
    res_count = cur.fetchone()[0]
    dt = datetime.strptime(ymd_str, "%Y-%m-%d").date()
    disc = DISCOUNTS.get(dt.weekday(), 0)
    return codes_count, res_count, disc
async def rewards_expiry_task():
    """Следит за сроком действия призов"""
    while True:
        try:
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)

            cur.execute("""
                SELECT id, user_id, prize, reward_code, expiry_date, notified_24h, expired
                FROM random_rewards
                WHERE redeemed = 0
            """)
            for rid, uid, prize, code, expiry_date, notified, expired in cur.fetchall():
                if not expiry_date:
                    continue
                try:
                    expiry = datetime.fromisoformat(expiry_date)
                except Exception:
                    continue

                delta = (expiry - now).total_seconds()

                # 🔔 за 24 часа до истечения
                if 0 < delta <= 24*3600 and not notified:
                    await bot.send_message(
                        uid,
                        f"⏳ Ваш приз <b>{prize}</b> (код <code>{code}</code>) "
                        "истекает через 24 часа! Заберите подарок у администратора 🎁"
                    )
                    cur.execute("UPDATE random_rewards SET notified_24h = 1 WHERE id = ?", (rid,))
                    conn.commit()

                # ❌ истёк
                if delta <= 0 and not expired:
                    cur.execute("UPDATE random_rewards SET expired = 1 WHERE id = ?", (rid,))
                    conn.commit()
                    await bot.send_message(
                        uid,
                        f"❌ Ваш приз <b>{prize}</b> (код <code>{code}</code>) истёк и больше недоступен."
                    )
                    for aid in ADMIN_IDS:
                        try:
                            await bot.send_message(
                                aid,
                                f"⚠️ Приз истёк\nКод: <code>{code}</code>\nПриз: {prize}\nПользователь ID: {uid}"
                            )
                        except Exception:
                            pass
        except Exception as e:
            print(f"[rewards_expiry_task] ошибка: {e}")

        await asyncio.sleep(1800)

async def notifier_task():
    sent_reminder_day = None
    sent_report_day = None
    while True:
        try:
            now = now_tz()
            day = ymd(now)

            # 11:55 reminder
            target_rem = now.replace(hour=11, minute=55, second=0, microsecond=0)
            if sent_reminder_day != day and abs((now - target_rem).total_seconds()) <= 90:
                disc = today_discount()
                s_h,s_m=CODES_WINDOW_START; e_h,e_m=CODES_WINDOW_END
                text = (f"⏳ Через 5 минут (в {s_h:02d}:{s_m:02d}) стартует окно получения кода!\n"
                        f"Сегодня скидка по браслету: <b>{disc}%</b>.\n"
                        f"Окно: <b>{s_h:02d}:{s_m:02d}–{e_h:02d}:{e_m:02d}</b>.\n"
                        "Жмите «🎟 Получить код» в боте.")
                # 🔔 Отправляем уведомление всем подписанным пользователям
                try:
                    limit_date = (now - timedelta(days=30)).isoformat()
                    cur.execute("""
                    SELECT user_id FROM users
                    WHERE approved = 1 AND blocked = 0 AND last_seen >= ?
                    """, (limit_date,))
                    all_users = [r[0] for r in cur.fetchall()]
                    for uid in all_users:
                        try:
                            await bot.send_message(uid, text)
                        except Exception as e:
                            print(f"[WARN] Не удалось отправить уведомление пользователю {uid}: {e}")
                except Exception as e:
                    print(f"[DB][ERROR] Ошибка при рассылке подписчикам: {e}")

                # 📢 Отправляем уведомление также администраторам
                await notify_admins(text)
                sent_reminder_day = day

            # 03:00 report for yesterday
            target_rep = now.replace(hour=1, minute=0, second=0, microsecond=0)
            if sent_report_day != day and abs((now - target_rep).total_seconds()) <= 59:
                yesterday = (now - timedelta(days=1)).date()
                ykey = ymd(yesterday)
                codes, resv, disc = stats_for_day_ymd_str(ykey)
                text = (f"📊 Отчёт за вчера ({ykey}):\n"
                        f"— Выдано кодов: <b>{codes}</b>\n"
                        f"— Создано броней: <b>{resv}</b>\n"
                        f"— Скидка дня была: <b>{disc}%</b>")
                await notify_admins(text)
                sent_report_day = day

            # Friday 13:00 prizes broadcast (НЕ в except!)
            target_prize = now.replace(hour=PRIZE_HOUR, minute=PRIZE_MINUTE, second=0, microsecond=0)
            if now.weekday() == PRIZE_DAY and abs((now - target_prize).total_seconds()) <= 59:

                cur.execute("SELECT id, guest_name, guest_phone, prize, user_id FROM prizes ORDER BY id ASC")
                prizes = cur.fetchall()
                if prizes:
                    sent_ok = 0
                    sent_fail = 0
                    for pid, name, phone, prize, user_id in prizes:
                        text = (
                            "🎉 <b>Shishka Restobar</b> — там, где браслет решает!\n"
                            "Мы помним о вашем призе 🎁\n"
                            f"Ваш приз: <b>{prize}</b>\n\n"
                            "👉 Забронируйте стол и используйте свой приз уже сегодня!"
                        )
                        kb = InlineKeyboardBuilder()
                        kb.button(text="🍽 Забронировать стол", callback_data="reserve")
                        kb.adjust(1)
                        try:
                            if user_id:
                                await bot.send_message(user_id, text, reply_markup=kb.as_markup())
                                sent_ok += 1
                            else:
                                await notify_admins(
                                    f"⚠️ Не удалось отправить сообщение призёру\n"
                                    f"{name} ({phone}) — {prize}\n(гость не активировал бота)"
                                )
                                sent_fail += 1
                        except Exception as e:
                            print("[PRIZE][ERROR]", e)
                            sent_fail += 1
                    clear_prizes()
                    await notify_admins(
                        f"📤 Рассылка призов завершена.\n✅ Отправлено: {sent_ok}\n🕳 Не доставлено: {sent_fail}"
                    )

        except Exception as e:
            logger.exception("[NOTIFIER] %s", e)

        await asyncio.sleep(30)

async def weekly_report_task():
    """Еженедельный отчёт администраторам (по понедельникам в 10:00)"""
    last_report_week = None
    while True:
        try:
            now = now_tz()
            iso_year, iso_week, _ = now.isocalendar()

            # проверяем понедельник, 10:00
            target_time = now.replace(hour=10, minute=0, second=0, microsecond=0)
            if now.weekday() == 0 and (last_report_week != (iso_year, iso_week)) and abs((now - target_time).total_seconds()) <= 59:
                # вычисляем диапазон прошлой недели
                week_start = now - timedelta(days=7)
                week_end = now - timedelta(days=1)
                start_key = ymd(week_start)
                end_key = ymd(week_end)

                # подсчёт пользователей
                cur.execute("""
                    SELECT COUNT(*) FROM users
                    WHERE DATE(joined_at) BETWEEN ? AND ?
                """, (start_key, end_key))
                new_users = cur.fetchone()[0]

                # коды
                cur.execute("""
                    SELECT COUNT(*) FROM codes
                    WHERE DATE(issued_at) BETWEEN ? AND ?
                """, (start_key, end_key))
                codes = cur.fetchone()[0]

                # брони
                cur.execute("""
                    SELECT COUNT(*) FROM reservations
                    WHERE DATE(created_at) BETWEEN ? AND ?
                """, (start_key, end_key))
                resv = cur.fetchone()[0]

                # призы (автоматические)
                cur.execute("""
                    SELECT COUNT(*) FROM random_rewards
                    WHERE DATE(date_issued) BETWEEN ? AND ?
                """, (start_key, end_key))
                rewards = cur.fetchone()[0]

                text = (
                    f"📊 <b>Отчёт за неделю</b>\n"
                    f"📅 {week_start.strftime('%d.%m')}–{week_end.strftime('%d.%m')}\n\n"
                    f"👥 Новых пользователей: <b>{new_users}</b>\n"
                    f"🎟 Выдано кодов: <b>{codes}</b>\n"
                    f"🍽 Создано броней: <b>{resv}</b>\n"
                    f"🎲 Разыграно призов: <b>{rewards}</b>"
                )

                await notify_admins(text)
                last_report_week = (iso_year, iso_week)

            await asyncio.sleep(60)
        except Exception as e:
            print(f"[WEEKLY-REPORT][ERROR] {e}")
            await asyncio.sleep(60)
        
# ===== Run =====
async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is empty. Set it in .env")
    print("✅ Bot starting... Time now (Tashkent):", now_tz().strftime("%Y-%m-%d %H:%M:%S"))
    print("✅ VERSION:", APP_VERSION)
    
    await set_commands()
    
    # фоновые задачи
    asyncio.create_task(notifier_task())
    asyncio.create_task(rewards_expiry_task())  # ← правильное имя и без лишней 's'
    asyncio.create_task(weekly_report_task())

  
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped.")
