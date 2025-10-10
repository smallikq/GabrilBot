import os
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Tuple, Optional, List, Dict, Any
import pandas as pd
from tqdm import tqdm
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
import parser_cfg
import logging
import json
from pathlib import Path
import aiofiles
import zipfile
import tempfile
import shutil

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup,
                           InlineKeyboardButton, FSInputFile, BufferedInputFile)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# Configure enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_logs.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Initialize bot and dispatcher
bot = Bot(token=parser_cfg.BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Global variables
pending_missed_days = []
active_tasks = {}  # Для отслеживания активных задач
user_settings = {}  # Настройки пользователей

# Enhanced Excel schema
COLUMNS = [
    "User_id",
    "Username",
    "Имя",
    "Фамилия",
    "Телефон",
    "Пол",
    "Премиум",
    "Verified",
    "Последняя активность (UTC)",
    "Время сбора (UTC+1)",
    "Источник группы",
    "ID группы",
    "Тип аккаунта"
]


# Define enhanced states for FSM
class Form(StatesGroup):
    waiting_for_date = State()
    waiting_for_user_ids = State()
    waiting_for_date_range = State()
    waiting_for_group_filter = State()
    waiting_for_export_format = State()
    waiting_for_schedule_time = State()


# Create necessary directories
def ensure_directories():
    """Создание необходимых директорий"""
    directories = ['reply', 'exports', 'backups', 'logs', 'temp']
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)


ensure_directories()


def log_banner(text, symbol="="):
    """Улучшенное логирование с красивым баннером"""
    border = symbol * (len(text) + 4)
    logging.info(f"\n{border}")
    logging.info(f"{symbol} {text} {symbol}")
    logging.info(f"{border}\n")


def create_user_row(user, group_info=None, account_type="Unknown"):
    """Создание расширенной строки пользователя"""
    collected_time = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    # Обработка username - добавляем @ если его нет
    username = getattr(user, 'username', None)
    if username and not username.startswith('@'):
        username = f'@{username}'

    return [
        getattr(user, 'id', user) if hasattr(user, 'id') else user,  # User_id
        username,  # Username с @
        getattr(user, 'first_name', None),  # Имя
        getattr(user, 'last_name', None),  # Фамилия
        getattr(user, 'phone', None),  # Телефон
        None,  # Пол (пока не определяем)
        getattr(user, 'premium', None),  # Премиум
        getattr(user, 'verified', None),  # Verified
        None,  # Последняя активность (заполним позже)
        collected_time,  # Время сбора
        group_info.get('title', 'Unknown') if group_info else None,  # Источник группы
        group_info.get('id', None) if group_info else None,  # ID группы
        account_type  # Тип аккаунта
    ]


class DatabaseManager:
    """Менеджер для работы с базой данных"""

    @staticmethod
    def is_file_locked(filepath):
        """Проверка блокировки файла"""
        if not os.path.exists(filepath):
            return False
        try:
            with open(filepath, 'a'):
                return False
        except (IOError, PermissionError):
            return True

    @staticmethod
    async def wait_for_unlock(filepath, max_retries=10):
        """Ожидание разблокировки файла"""
        retry_count = 0
        while DatabaseManager.is_file_locked(filepath) and retry_count < max_retries:
            await asyncio.sleep(2)
            retry_count += 1
            logging.info(f"Waiting for file unlock: {filepath}, attempt {retry_count}")
        return retry_count < max_retries

    @staticmethod
    async def backup_database():
        """Создание резервной копии базы данных"""
        base_file = 'all_users.xlsx'
        if os.path.exists(base_file):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f'backups/all_users_backup_{timestamp}.xlsx'
            try:
                shutil.copy2(base_file, backup_file)
                logging.info(f"Database backup created: {backup_file}")
                return backup_file
            except Exception as e:
                logging.error(f"Error creating backup: {e}")
        return None

    @staticmethod
    def get_database_stats():
        """Получение расширенной статистики базы данных"""
        base_file = "all_users.xlsx"

        if not os.path.exists(base_file):
            return {"total_users": 0, "error": "Database not found"}

        try:
            df = pd.read_excel(base_file)

            stats = {
                "total_users": len(df),
                "with_username": df["Username"].notna().sum(),
                "premium_users": df["Премиум"].sum() if "Премиум" in df.columns else 0,
                "verified_users": df["Verified"].sum() if "Verified" in df.columns else 0,
            }

            # Статистика по датам
            if "Время сбора (UTC+1)" in df.columns:
                df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
                df_clean = df.dropna(subset=["Время сбора (UTC+1)"])

                if not df_clean.empty:
                    stats["first_record"] = df_clean["Время сбора (UTC+1)"].min()
                    stats["last_record"] = df_clean["Время сбора (UTC+1)"].max()

                    # Статистика по дням
                    daily_stats = df_clean.groupby(df_clean["Время сбора (UTC+1)"].dt.date).size()
                    stats["most_active_day"] = daily_stats.idxmax()
                    stats["most_active_day_count"] = daily_stats.max()

            # Статистика по источникам
            if "Источник группы" in df.columns:
                source_stats = df["Источник группы"].value_counts()
                stats["top_sources"] = source_stats.head(5).to_dict()

            return stats

        except Exception as e:
            logging.error(f"Error getting database stats: {e}")
            return {"error": str(e)}


class ExportManager:
    """Менеджер экспорта данных в различные форматы"""

    @staticmethod
    async def export_to_csv(df, filename):
        """Экспорт в CSV"""
        try:
            csv_path = f'exports/{filename}.csv'
            df.to_csv(csv_path, index=False, encoding='utf-8')
            return csv_path
        except Exception as e:
            logging.error(f"Error exporting to CSV: {e}")
            return None

    @staticmethod
    async def export_to_json(df, filename):
        """Экспорт в JSON"""
        try:
            json_path = f'exports/{filename}.json'
            df.to_json(json_path, orient='records', force_ascii=False, indent=2)
            return json_path
        except Exception as e:
            logging.error(f"Error exporting to JSON: {e}")
            return None

    @staticmethod
    async def create_report(df, filename):
        """Создание детального отчета"""
        try:
            report_path = f'exports/{filename}_report.txt'

            stats = {
                "Общее количество пользователей": len(df),
                "С username": df["Username"].notna().sum(),
                "С именем": df["Имя"].notna().sum(),
                "Premium пользователи": df["Премиум"].sum() if "Премиум" in df.columns else 0,
            }

            with open(report_path, 'w', encoding='utf-8') as f:
                f.write("=== ОТЧЕТ ПО БАЗЕ ДАННЫХ ===\n\n")
                f.write(f"Дата создания отчета: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

                f.write("ОСНОВНАЯ СТАТИСТИКА:\n")
                for key, value in stats.items():
                    f.write(f"• {key}: {value}\n")

                f.write(f"\n• Процент с username: {(stats['С username'] / len(df) * 100):.1f}%\n")

                if "Источник группы" in df.columns:
                    f.write("\nТОП-10 ИСТОЧНИКОВ:\n")
                    source_stats = df["Источник группы"].value_counts().head(10)
                    for source, count in source_stats.items():
                        f.write(f"• {source}: {count} пользователей\n")

                if "Время сбора (UTC+1)" in df.columns:
                    df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
                    df_clean = df.dropna(subset=["Время сбора (UTC+1)"])
                    if not df_clean.empty:
                        daily_stats = df_clean.groupby(df_clean["Время сбора (UTC+1)"].dt.date).size()
                        f.write(f"\nСТАТИСТИКА ПО ДНЯМ:\n")
                        f.write(f"• Период: с {daily_stats.index.min()} по {daily_stats.index.max()}\n")
                        f.write(f"• Самый активный день: {daily_stats.idxmax()} ({daily_stats.max()} пользователей)\n")
                        f.write(f"• Среднее в день: {daily_stats.mean():.1f} пользователей\n")

            return report_path
        except Exception as e:
            logging.error(f"Error creating report: {e}")
            return None


async def find_date_boundaries(client, chat_id: int, target_date: datetime.date) -> Tuple[Optional[int], Optional[int]]:
    """Улучшенный поиск границ сообщений"""
    try:
        start_datetime = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_datetime = datetime.combine(target_date, datetime.max.time()).replace(tzinfo=timezone.utc)

        logging.info(f"Searching messages for {target_date} between {start_datetime} and {end_datetime}")

        # Поиск с расширенным диапазоном
        last_msg = None
        async for msg in client.iter_messages(chat_id, offset_date=end_datetime + timedelta(seconds=1), limit=50):
            if msg.date.date() == target_date:
                last_msg = msg
                break

        if not last_msg:
            return None, None

        first_msg = None
        async for msg in client.iter_messages(chat_id, offset_date=start_datetime, reverse=True, limit=50):
            if msg.date.date() == target_date:
                first_msg = msg
                break

        if first_msg and last_msg:
            logging.info(f"Found boundaries: {first_msg.id} - {last_msg.id}")
            return first_msg.id, last_msg.id

        return None, None

    except FloodWaitError as e:
        logging.warning(f"FloodWaitError: waiting {e.seconds} seconds")
        await asyncio.sleep(e.seconds)
        return await find_date_boundaries(client, chat_id, target_date)
    except Exception as e:
        logging.error(f"Error finding boundaries for {chat_id}: {e}")
        return None, None


async def process_dialog_enhanced(client, dialog, pbar, date_target, account_info):
    """Улучшенная обработка диалога"""
    users = set()
    try:
        if not dialog.is_group:
            pbar.update(1)
            return users

        group_info = {
            'title': dialog.title,
            'id': dialog.id,
            'participants_count': getattr(dialog.entity, 'participants_count', 0)
        }

        logging.info(f"Processing group: {dialog.title} (ID: {dialog.id}, Members: {group_info['participants_count']})")

        min_id, max_id = await find_date_boundaries(client, dialog.id, date_target)

        if not min_id or not max_id:
            logging.info(f"No messages in {dialog.title} for {date_target}")
            pbar.update(1)
            return users

        message_count = 0
        unique_senders = set()

        async for message in client.iter_messages(dialog.id, min_id=min_id - 1, max_id=max_id + 1):
            if message.date.date() == date_target and message.sender:
                try:
                    sender_id = message.sender_id
                    if sender_id not in unique_senders:
                        unique_senders.add(sender_id)

                        # Получаем полную информацию о пользователе
                        user_entity = message.sender
                        last_seen = message.date.strftime("%Y-%m-%d %H:%M:%S")

                        # Обработка username с добавлением @
                        username = getattr(user_entity, 'username', None)
                        if username and not username.startswith('@'):
                            username = f'@{username}'

                        # Создаем расширенную строку пользователя
                        enhanced_user_data = [
                            sender_id,
                            username,  # Username с @
                            getattr(user_entity, 'first_name', None),
                            getattr(user_entity, 'last_name', None),
                            getattr(user_entity, 'phone', None),
                            None,  # Пол
                            getattr(user_entity, 'premium', None),
                            getattr(user_entity, 'verified', None),
                            last_seen,
                            (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
                            dialog.title,
                            dialog.id,
                            account_info['phone_number']
                        ]

                        users.add(tuple(enhanced_user_data))

                    message_count += 1

                except Exception as e:
                    logging.error(f"Error processing message {message.id}: {e}")
                    continue

        logging.info(f"Group {dialog.title}: {len(users)} unique users from {message_count} messages")
        pbar.update(1)

    except Exception as e:
        logging.error(f"Error processing {getattr(dialog, 'title', 'Unknown')}: {e}")
        pbar.update(1)

    return users


async def get_users_from_chats_enhanced(account, date_target, progress_callback=None):
    """Улучшенная функция сбора пользователей"""
    result_message = []
    file_path = None

    if not account.get('api_id') or not account.get('api_hash'):
        error_msg = f"⚠️ Account {account['phone_number']}: Missing API credentials"
        result_message.append(error_msg)
        logging.error(error_msg)
        return result_message, file_path

    log_banner(f"Processing Account {account['phone_number']}")
    result_message.append(f"🔄 Processing account {account['phone_number']}...")

    session_name = f'session_{account["phone_number"]}'
    client = TelegramClient(session_name, account['api_id'], account['api_hash'])

    try:
        await client.start(phone=account['phone_number'])

        # Проверяем авторизацию
        if not await client.is_user_authorized():
            result_message.append(f"❌ Account {account['phone_number']} not authorized")
            return result_message, file_path

        me = await client.get_me()
        logging.info(f"Successfully connected as {me.first_name} ({me.username or 'No username'})")

        dialogs = await client.get_dialogs()

        # Фильтруем группы по различным критериям
        filtered_dialogs = []
        for dialog in dialogs:
            if dialog.is_group and not dialog.archived:
                # Дополнительные проверки
                if hasattr(dialog.entity, 'participants_count'):
                    if dialog.entity.participants_count > 10:  # Только группы с >10 участников
                        filtered_dialogs.append(dialog)
                else:
                    filtered_dialogs.append(dialog)

        result_message.append(f"📌 Found {len(filtered_dialogs)} active groups (filtered from {len(dialogs)} total)")

        if not filtered_dialogs:
            result_message.append(f"⚠️ No suitable groups found")
            return result_message, file_path

        # Обработка с улучшенным прогресс-баром
        with tqdm(total=len(filtered_dialogs), desc=f"Processing {account['phone_number']}", unit="group") as pbar:
            semaphore = asyncio.Semaphore(3)  # Ограничиваем до 3 одновременных задач

            async def bounded_process(dialog):
                async with semaphore:
                    return await process_dialog_enhanced(client, dialog, pbar, date_target, account)

            tasks = [bounded_process(dialog) for dialog in filtered_dialogs]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Собираем результаты
        all_users = set()
        for result in results:
            if isinstance(result, Exception):
                logging.error(f"Dialog processing error: {result}")
                continue
            all_users.update(result)

        logging.info(f"Total unique users collected: {len(all_users)}")

        # Улучшенная обработка базы данных
        if all_users:
            # Создаем бэкап перед изменениями
            await DatabaseManager.backup_database()

            base_file = 'all_users.xlsx'
            await DatabaseManager.wait_for_unlock(base_file)

            try:
                if os.path.exists(base_file):
                    df_existing = pd.read_excel(base_file)
                else:
                    df_existing = pd.DataFrame(columns=COLUMNS)

            except Exception as e:
                logging.error(f"Error reading database: {e}")
                df_existing = pd.DataFrame(columns=COLUMNS)

            existing_ids = set(df_existing["User_id"].values) if not df_existing.empty else set()
            new_users = set()

            for row in all_users:
                if row[0] not in existing_ids:
                    new_users.add(row)

            if new_users:
                df_new = pd.DataFrame(list(new_users), columns=COLUMNS)
                df_updated = pd.concat([df_existing, df_new], ignore_index=True)

                # Сортируем по времени сбора
                df_updated["Время сбора (UTC+1)"] = pd.to_datetime(df_updated["Время сбора (UTC+1)"])
                df_updated = df_updated.sort_values("Время сбора (UTC+1)")

                await DatabaseManager.wait_for_unlock(base_file)

                try:
                    df_updated.to_excel(base_file, index=False)
                    result_message.append(f"✅ Database updated: +{len(new_users)} new users")
                    logging.info(f"Database updated with {len(new_users)} new users")
                except Exception as e:
                    logging.error(f"Error saving database: {e}")
                    result_message.append(f"⚠️ Database save error: {e}")

                # Создаем файл результатов
                date_str = date_target.strftime('%Y-%m-%d')
                file_path = f'reply/reply_{account["phone_number"]}_{date_str}.xlsx'

                try:
                    df_new.to_excel(file_path, index=False)
                    result_message.append(f"💾 Reply file created: {len(new_users)} users")
                    logging.info(f"Reply file saved: {file_path}")
                except Exception as e:
                    logging.error(f"Error saving reply file: {e}")
                    result_message.append(f"⚠️ Reply file error: {e}")
                    file_path = None
            else:
                result_message.append(f"📌 No new users found for {account['phone_number']}")
        else:
            result_message.append(f"📌 No users collected from {account['phone_number']}")

    except SessionPasswordNeededError:
        result_message.append(f"❌ Account {account['phone_number']}: 2FA password required")
    except Exception as e:
        logging.error(f"Error processing account {account['phone_number']}: {e}")
        result_message.append(f"❌ Account {account['phone_number']}: {str(e)}")

    finally:
        try:
            await client.disconnect()
        except:
            pass

    return result_message, file_path


def get_enhanced_main_keyboard():
    """Расширенная главная клавиатура"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 Запустить сбор данных"), KeyboardButton(text="📊 Статистика базы")],
            [KeyboardButton(text="📌 Парсить пропущенные дни"), KeyboardButton(text="📅 Диапазон дат")],
            [KeyboardButton(text="➕ Добавить ID вручную"), KeyboardButton(text="🔍 Поиск пользователей")],
            [KeyboardButton(text="💾 Экспорт данных"), KeyboardButton(text="🔧 Настройки")],
            [KeyboardButton(text="📈 Аналитика"), KeyboardButton(text="🗂 Управление файлами")]
        ],
        resize_keyboard=True
    )
    return keyboard


def get_enhanced_date_keyboard():
    """Улучшенная клавиатура выбора даты"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[])
    today = datetime.now(timezone.utc).date()

    # Быстрые опции
    quick_options = [
        (f"Сегодня ({today.strftime('%d.%m')})", f"date_{today.strftime('%d.%m.%Y')}"),
        (f"Вчера ({(today - timedelta(days=1)).strftime('%d.%m')})",
         f"date_{(today - timedelta(days=1)).strftime('%d.%m.%Y')}"),
        (f"2 дня назад ({(today - timedelta(days=2)).strftime('%d.%m')})",
         f"date_{(today - timedelta(days=2)).strftime('%d.%m.%Y')}")
    ]

    for text, callback_data in quick_options:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=text, callback_data=callback_data)])

    # Дополнительные опции
    keyboard.inline_keyboard.append([
        InlineKeyboardButton(text="📅 Выбрать дату", callback_data="custom_date"),
        InlineKeyboardButton(text="📊 Диапазон дат", callback_data="date_range")
    ])

    return keyboard


def get_export_keyboard():
    """Клавиатура для экспорта"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Excel", callback_data="export_excel"),
            InlineKeyboardButton(text="📝 CSV", callback_data="export_csv")
        ],
        [
            InlineKeyboardButton(text="📋 JSON", callback_data="export_json"),
            InlineKeyboardButton(text="📑 Отчет", callback_data="export_report")
        ],
        [
            InlineKeyboardButton(text="📦 Архив (все форматы)", callback_data="export_all")
        ]
    ])
    return keyboard


def get_last_parsed_date() -> Optional[datetime.date]:
    """Улучшенное определение последней даты парсинга"""
    base_file = "all_users.xlsx"

    if not os.path.exists(base_file):
        logging.info("Database file doesn't exist")
        return None

    try:
        df = pd.read_excel(base_file)

        if "Время сбора (UTC+1)" not in df.columns or df.empty:
            return None

        df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
        df_clean = df.dropna(subset=["Время сбора (UTC+1)"])

        if df_clean.empty:
            return None

        last_datetime = df_clean["Время сбора (UTC+1)"].max()
        return last_datetime.date()

    except Exception as e:
        logging.error(f"Error determining last parsed date: {e}")
        return None


# ===== КОМАНДЫ И ОБРАБОТЧИКИ =====

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Улучшенное главное меню"""
    user_id = message.from_user.id
    user_settings[user_id] = user_settings.get(user_id, {
        'notifications': True,
        'auto_backup': True,
        'export_format': 'excel'
    })

    welcome_text = (
        "🤖 <b>Telegram Parser Bot</b>\n\n"
        "Выберите действие:"
    )

    await message.answer(welcome_text, reply_markup=get_enhanced_main_keyboard(), parse_mode="HTML")


@dp.message(F.text == "📊 Статистика базы")
async def show_enhanced_stats(message: types.Message):
    """Расширенная статистика"""
    stats = DatabaseManager.get_database_stats()

    if 'error' in stats:
        await message.answer(f"❌ Ошибка: {stats['error']}")
        return

    if stats['total_users'] == 0:
        await message.answer("📊 База данных пуста")
        return

    stats_text = f"📊 <b>Статистика базы данных</b>\n\n"
    stats_text += f"👥 <b>Всего пользователей:</b> {stats['total_users']:,}\n"
    stats_text += f"🏷 <b>С username:</b> {stats['with_username']:,} ({stats['with_username'] / stats['total_users'] * 100:.1f}%)\n"

    if stats.get('premium_users', 0) > 0:
        stats_text += f"💎 <b>Premium:</b> {stats['premium_users']:,}\n"

    if stats.get('verified_users', 0) > 0:
        stats_text += f"✅ <b>Verified:</b> {stats['verified_users']:,}\n"

    if 'first_record' in stats and 'last_record' in stats:
        stats_text += f"\n📅 <b>Период сбора:</b>\n"
        stats_text += f"• Первая запись: {stats['first_record'].strftime('%d.%m.%Y')}\n"
        stats_text += f"• Последняя запись: {stats['last_record'].strftime('%d.%m.%Y')}\n"

    if 'most_active_day' in stats:
        stats_text += f"• Самый активный день: {stats['most_active_day']} ({stats['most_active_day_count']} пользователей)\n"

    if 'top_sources' in stats:
        stats_text += f"\n🎯 <b>Топ-5 источников:</b>\n"
        for source, count in list(stats['top_sources'].items())[:5]:
            stats_text += f"• {source}: {count}\n"

    # Добавляем кнопки действий
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📈 Детальная аналитика", callback_data="detailed_analytics"),
            InlineKeyboardButton(text="💾 Экспорт статистики", callback_data="export_stats")
        ],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_stats")]
    ])

    await message.answer(stats_text, reply_markup=keyboard, parse_mode="HTML")


@dp.message(F.text == "🚀 Запустить сбор данных")
async def process_start_command(message: types.Message):
    """Запуск сбора данных"""
    await message.answer("📅 Выберите дату для сбора данных:", reply_markup=get_enhanced_date_keyboard())


@dp.message(F.text == "📈 Аналитика")
async def show_analytics(message: types.Message):
    """Показать аналитику"""
    base_file = "all_users.xlsx"

    if not os.path.exists(base_file):
        await message.answer("📈 Нет данных для аналитики")
        return

    try:
        df = pd.read_excel(base_file)

        if df.empty:
            await message.answer("📈 База данных пуста")
            return

        # Создаем детальную аналитику
        analytics_text = "📈 <b>Детальная аналитика</b>\n\n"

        # Анализ активности по времени
        if "Время сбора (UTC+1)" in df.columns:
            df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
            df_time = df.dropna(subset=["Время сбора (UTC+1)"])

            if not df_time.empty:
                # Статистика по дням недели
                df_time['day_of_week'] = df_time["Время сбора (UTC+1)"].dt.day_name()
                day_stats = df_time['day_of_week'].value_counts()

                analytics_text += "📅 <b>Активность по дням недели:</b>\n"
                for day, count in day_stats.head(3).items():
                    analytics_text += f"• {day}: {count} пользователей\n"

                # Статистика по часам
                df_time['hour'] = df_time["Время сбора (UTC+1)"].dt.hour
                hour_stats = df_time['hour'].value_counts().sort_index()
                peak_hour = hour_stats.idxmax()
                analytics_text += f"\n🕐 <b>Пиковый час активности:</b> {peak_hour}:00 ({hour_stats[peak_hour]} пользователей)\n"

        # Анализ username
        username_stats = {
            'total': len(df),
            'with_username': df["Username"].notna().sum(),
            'without_username': df["Username"].isna().sum()
        }

        analytics_text += f"\n🏷 <b>Анализ username:</b>\n"
        analytics_text += f"• С username: {username_stats['with_username']} ({username_stats['with_username'] / username_stats['total'] * 100:.1f}%)\n"
        analytics_text += f"• Без username: {username_stats['without_username']} ({username_stats['without_username'] / username_stats['total'] * 100:.1f}%)\n"

        # Анализ источников
        if "Источник группы" in df.columns:
            source_stats = df["Источник группы"].value_counts()
            analytics_text += f"\n🎯 <b>Анализ источников:</b>\n"
            analytics_text += f"• Всего уникальных групп: {len(source_stats)}\n"
            analytics_text += f"• Самая продуктивная группа: {source_stats.index[0]} ({source_stats.iloc[0]} пользователей)\n"

        # Прогноз роста
        if "Время сбора (UTC+1)" in df.columns and not df_time.empty:
            daily_growth = df_time.groupby(df_time["Время сбора (UTC+1)"].dt.date).size()
            if len(daily_growth) > 1:
                avg_daily = daily_growth.mean()
                analytics_text += f"\n📊 <b>Прогнозы:</b>\n"
                analytics_text += f"• Среднесуточный рост: {avg_daily:.1f} пользователей\n"
                analytics_text += f"• Прогноз на неделю: +{avg_daily * 7:.0f} пользователей\n"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📊 Создать отчет", callback_data="create_analytics_report"),
                InlineKeyboardButton(text="📈 Графики", callback_data="create_charts")
            ]
        ])

        await message.answer(analytics_text, reply_markup=keyboard, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error in analytics: {e}")
        await message.answer(f"❌ Ошибка при создании аналитики: {e}")


@dp.message(F.text == "💾 Экспорт данных")
async def export_menu(message: types.Message):
    """Меню экспорта данных"""
    base_file = "all_users.xlsx"

    if not os.path.exists(base_file):
        await message.answer("❌ Нет данных для экспорта")
        return

    try:
        df = pd.read_excel(base_file)
        total_users = len(df)

        export_text = f"💾 <b>Экспорт данных</b>\n\n"
        export_text += f"📊 В базе: {total_users:,} пользователей\n"
        export_text += f"📁 Доступные форматы:\n\n"
        export_text += f"• <b>Excel</b> - полная совместимость\n"
        export_text += f"• <b>CSV</b> - универсальный формат\n"
        export_text += f"• <b>JSON</b> - для разработчиков\n"
        export_text += f"• <b>Отчет</b> - детальная аналитика\n"
        export_text += f"• <b>Архив</b> - все форматы сразу"

        await message.answer(export_text, reply_markup=get_export_keyboard(), parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error in export menu: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(F.text == "🔍 Поиск пользователей")
async def search_users(message: types.Message, state: FSMContext):
    """Поиск пользователей в базе"""
    await state.set_state(Form.waiting_for_user_ids)

    search_text = (
        "🔍 <b>Поиск пользователей</b>\n\n"
        "Введите один из вариантов:\n"
        "• <code>ID пользователя</code> (например: 123456789)\n"
        "• <code>@username</code> (например: @john_doe)\n"
        "• <code>Имя пользователя</code> (например: Иван)\n"
        "• <code>Несколько ID</code> через запятую\n\n"
        "Или отправьте /cancel для отмены"
    )

    await message.answer(search_text, parse_mode="HTML")


@dp.message(F.text == "📅 Диапазон дат")
async def date_range_menu(message: types.Message, state: FSMContext):
    """Обработка диапазона дат"""
    await state.set_state(Form.waiting_for_date_range)

    range_text = (
        "📅 <b>Сбор данных за период</b>\n\n"
        "Введите диапазон дат в формате:\n"
        "<code>ДД.ММ.ГГГГ - ДД.ММ.ГГГГ</code>\n\n"
        "Примеры:\n"
        "• <code>01.09.2024 - 05.09.2024</code>\n"
        "• <code>15.08.2024 - 20.08.2024</code>\n\n"
        "⚠️ Большие диапазоны могут занять много времени\n"
        "Или отправьте /cancel для отмены"
    )

    await message.answer(range_text, parse_mode="HTML")


@dp.message(F.text == "🗂 Управление файлами")
async def file_management(message: types.Message):
    """Управление файлами"""
    try:
        # Сканируем директории
        reply_files = list(Path('reply').glob('*.xlsx')) if Path('reply').exists() else []
        backup_files = list(Path('backups').glob('*.xlsx')) if Path('backups').exists() else []
        export_files = list(Path('exports').glob('*')) if Path('exports').exists() else []

        management_text = f"🗂 <b>Управление файлами</b>\n\n"
        management_text += f"📋 <b>Reply файлы:</b> {len(reply_files)}\n"
        management_text += f"💾 <b>Бэкапы:</b> {len(backup_files)}\n"
        management_text += f"📊 <b>Экспорты:</b> {len(export_files)}\n\n"

        # Показываем размеры
        total_size = 0
        for file_list in [reply_files, backup_files, export_files]:
            for file_path in file_list:
                if file_path.exists():
                    total_size += file_path.stat().st_size

        management_text += f"💽 <b>Общий размер:</b> {total_size / (1024 * 1024):.1f} МБ"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📋 Показать Reply", callback_data="list_reply_files"),
                InlineKeyboardButton(text="💾 Показать бэкапы", callback_data="list_backup_files")
            ],
            [
                InlineKeyboardButton(text="🗑 Очистить старые", callback_data="cleanup_old_files"),
                InlineKeyboardButton(text="📦 Создать архив", callback_data="create_archive")
            ]
        ])

        await message.answer(management_text, reply_markup=keyboard, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error in file management: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(F.text == "🔧 Настройки")
async def show_settings(message: types.Message):
    """Показать настройки пользователя"""
    user_id = message.from_user.id
    settings = user_settings.get(user_id, {})

    settings_text = f"🔧 <b>Настройки бота</b>\n\n"
    settings_text += f"🔔 Уведомления: {'✅' if settings.get('notifications', True) else '❌'}\n"
    settings_text += f"💾 Автобэкапы: {'✅' if settings.get('auto_backup', True) else '❌'}\n"
    settings_text += f"📊 Формат экспорта: {settings.get('export_format', 'excel').upper()}\n"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"🔔 {'Выключить' if settings.get('notifications', True) else 'Включить'} уведомления",
                callback_data="toggle_notifications"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"💾 {'Выключить' if settings.get('auto_backup', True) else 'Включить'} автобэкапы",
                callback_data="toggle_backup"
            )
        ],
        [
            InlineKeyboardButton(text="📊 Изменить формат экспорта", callback_data="change_export_format")
        ],
        [
            InlineKeyboardButton(text="🗑 Сбросить настройки", callback_data="reset_settings")
        ]
    ])

    await message.answer(settings_text, reply_markup=keyboard, parse_mode="HTML")


# ===== CALLBACK ОБРАБОТЧИКИ =====

@dp.callback_query(F.data.startswith('date_'))
async def process_date_selection(callback_query: types.CallbackQuery):
    """Обработка выбора даты"""
    await callback_query.answer()
    date_str = callback_query.data.split('_')[1]
    try:
        date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
        await start_processing_enhanced(callback_query.message, date_obj)
    except ValueError:
        await bot.send_message(callback_query.message.chat.id, "⚠️ Неверный формат даты.")


@dp.callback_query(F.data == 'custom_date')
async def process_custom_date(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка кастомной даты"""
    await callback_query.answer()
    await state.set_state(Form.waiting_for_date)
    await bot.send_message(
        callback_query.message.chat.id,
        "📅 Введите дату в формате ДД.ММ.ГГГГ (например: 15.01.2024):"
    )


@dp.callback_query(F.data.startswith('export_'))
async def handle_export(callback_query: types.CallbackQuery):
    """Обработка экспорта данных"""
    await callback_query.answer("Подготавливаю экспорт...")

    export_type = callback_query.data.split('_')[1]
    base_file = "all_users.xlsx"

    if not os.path.exists(base_file):
        await bot.send_message(callback_query.message.chat.id, "❌ Нет данных для экспорта")
        return

    try:
        df = pd.read_excel(base_file)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        status_msg = await bot.send_message(
            callback_query.message.chat.id,
            f"⏳ Создаю экспорт в формате {export_type.upper()}..."
        )

        if export_type == 'excel':
            file_path = f'exports/export_{timestamp}.xlsx'
            df.to_excel(file_path, index=False)
            caption = f"📊 Экспорт Excel ({len(df)} пользователей)"

        elif export_type == 'csv':
            file_path = await ExportManager.export_to_csv(df, f'export_{timestamp}')
            caption = f"📝 Экспорт CSV ({len(df)} пользователей)"

        elif export_type == 'json':
            file_path = await ExportManager.export_to_json(df, f'export_{timestamp}')
            caption = f"📋 Экспорт JSON ({len(df)} пользователей)"

        elif export_type == 'report':
            file_path = await ExportManager.create_report(df, f'export_{timestamp}')
            caption = f"📑 Детальный отчет ({len(df)} пользователей)"

        elif export_type == 'all':
            # Создаем архив со всеми форматами
            with tempfile.TemporaryDirectory() as temp_dir:
                # Создаем все форматы
                excel_path = os.path.join(temp_dir, f'export_{timestamp}.xlsx')
                csv_path = os.path.join(temp_dir, f'export_{timestamp}.csv')
                json_path = os.path.join(temp_dir, f'export_{timestamp}.json')
                report_path = os.path.join(temp_dir, f'report_{timestamp}.txt')

                df.to_excel(excel_path, index=False)
                df.to_csv(csv_path, index=False, encoding='utf-8')
                df.to_json(json_path, orient='records', force_ascii=False, indent=2)

                # Создаем архив
                file_path = f'exports/complete_export_{timestamp}.zip'
                with zipfile.ZipFile(file_path, 'w') as zipf:
                    zipf.write(excel_path, f'export_{timestamp}.xlsx')
                    zipf.write(csv_path, f'export_{timestamp}.csv')
                    zipf.write(json_path, f'export_{timestamp}.json')

                    # Добавляем отчет
                    report_content = f"Экспорт базы данных\n"
                    report_content += f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    report_content += f"Всего пользователей: {len(df)}\n"
                    report_content += f"Форматы: Excel, CSV, JSON\n"

                    with open(report_path, 'w', encoding='utf-8') as f:
                        f.write(report_content)
                    zipf.write(report_path, f'readme_{timestamp}.txt')

            caption = f"📦 Полный экспорт - все форматы ({len(df)} пользователей)"

        await bot.edit_message_text(
            f"✅ Экспорт готов! Отправляю файл...",
            callback_query.message.chat.id,
            status_msg.message_id
        )

        # Отправляем файл
        if file_path and os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                await bot.send_document(
                    callback_query.message.chat.id,
                    FSInputFile(file_path, filename=os.path.basename(file_path)),
                    caption=caption
                )

            await bot.delete_message(callback_query.message.chat.id, status_msg.message_id)
        else:
            await bot.edit_message_text(
                "❌ Ошибка при создании экспорта",
                callback_query.message.chat.id,
                status_msg.message_id
            )

    except Exception as e:
        logging.error(f"Export error: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка экспорта: {e}")


@dp.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback_query: types.CallbackQuery):
    """Переключение уведомлений"""
    user_id = callback_query.from_user.id
    current = user_settings.get(user_id, {}).get('notifications', True)

    if user_id not in user_settings:
        user_settings[user_id] = {}
    user_settings[user_id]['notifications'] = not current

    status = "включены" if not current else "выключены"
    await callback_query.answer(f"Уведомления {status}")
    await show_settings(callback_query.message)


# ===== ОСНОВНЫЕ ФУНКЦИИ ОБРАБОТКИ =====

async def start_processing_enhanced(message, date_target):
    """Улучшенная функция обработки данных"""
    user_id = message.chat.id

    # Проверяем активные задачи
    if user_id in active_tasks:
        await message.answer("⚠️ У вас уже выполняется задача. Дождитесь её завершения.")
        return

    active_tasks[user_id] = True

    try:
        status_message = await message.answer(
            f"🚀 Начинаю расширенный сбор данных за {date_target.strftime('%d.%m.%Y')}...\n"
            f"📊 Включены улучшенные фильтры и аналитика"
        )

        all_results = []
        all_files = []
        processed_accounts = 0
        total_accounts = len(parser_cfg.accounts)

        for i, account in enumerate(parser_cfg.accounts, 1):
            try:
                await bot.edit_message_text(
                    text=f"🚀 Обработка аккаунта {i}/{total_accounts}: {account['phone_number']}\n"
                         f"📊 Применяю умные фильтры...",
                    chat_id=message.chat.id,
                    message_id=status_message.message_id
                )

                result, file_path = await get_users_from_chats_enhanced(account, date_target)
                all_results.extend(result)
                if file_path:
                    all_files.append(file_path)
                processed_accounts += 1

            except Exception as e:
                logging.error(f"Error processing account {account.get('phone_number')}: {e}")
                all_results.append(f"❌ Ошибка в аккаунте {account.get('phone_number')}: {str(e)}")

        # Создаем детальный отчет
        await bot.edit_message_text(
            text=f"✅ Сбор завершен! Обработано: {processed_accounts}/{total_accounts}\n"
                 f"📊 Создаю детальный отчет...",
            chat_id=message.chat.id,
            message_id=status_message.message_id
        )

        # Показываем результаты
        success_count = len([r for r in all_results if "new users added" in r])
        error_count = len([r for r in all_results if "❌" in r])

        summary = f"🎉 <b>Обработка завершена!</b>\n\n"
        summary += f"📊 <b>Статистика:</b>\n"
        summary += f"• Аккаунтов обработано: {processed_accounts}/{total_accounts}\n"
        summary += f"• Успешных операций: {success_count}\n"
        summary += f"• Ошибок: {error_count}\n"
        summary += f"• Файлов создано: {len(all_files)}\n"

        await bot.edit_message_text(
            text=summary,
            chat_id=message.chat.id,
            message_id=status_message.message_id,
            parse_mode="HTML"
        )

        # Отправляем файлы
        for file_path in all_files:
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as file:
                        await bot.send_document(
                            message.chat.id,
                            FSInputFile(file_path, filename=os.path.basename(file_path)),
                            caption=f"📋 {os.path.basename(file_path)}"
                        )
                except Exception as e:
                    logging.error(f"Error sending file: {e}")

        # Отправляем обновленную базу с аналитикой
        await send_enhanced_database(message.chat.id, date_target)

    finally:
        active_tasks.pop(user_id, None)


async def send_enhanced_database(chat_id: int, date_target: datetime.date):
    """Отправка улучшенной базы данных с аналитикой"""
    base_file = 'all_users.xlsx'
    if os.path.exists(base_file):
        try:
            # Создаем аналитику для отправляемого файла
            df = pd.read_excel(base_file)
            stats = DatabaseManager.get_database_stats()

            caption = f"📚 <b>Обновленная база данных</b>\n\n"
            caption += f"📊 Всего пользователей: {len(df):,}\n"
            caption += f"📅 Обработка за: {date_target.strftime('%d.%m.%Y')}\n"

            if stats.get('with_username', 0) > 0:
                caption += f"🏷 С username: {stats['with_username']:,}\n"

            with open(base_file, 'rb') as file:
                await bot.send_document(
                    chat_id,
                    FSInputFile(file.name, filename='all_users.xlsx'),
                    caption=caption,
                    parse_mode="HTML"
                )
        except Exception as e:
            logging.error(f"Error sending enhanced database: {e}")


# ===== ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ =====

@dp.message(Form.waiting_for_date_range)
async def process_date_range(message: types.Message, state: FSMContext):
    """Обработка диапазона дат"""
    try:
        date_range = message.text.strip()

        # Парсим диапазон дат
        if ' - ' in date_range:
            start_str, end_str = date_range.split(' - ')
            start_date = datetime.strptime(start_str.strip(), "%d.%m.%Y").date()
            end_date = datetime.strptime(end_str.strip(), "%d.%m.%Y").date()
        else:
            await message.reply("⚠️ Неверный формат. Используйте: ДД.ММ.ГГГГ - ДД.ММ.ГГГГ")
            return

        if start_date > end_date:
            await message.reply("⚠️ Начальная дата не может быть больше конечной")
            return

        if end_date > datetime.now().date():
            await message.reply("⚠️ Конечная дата не может быть в будущем")
            return

        # Подсчитываем количество дней
        delta = end_date - start_date
        days_count = delta.days + 1

        if days_count > 30:
            await message.reply("⚠️ Максимальный диапазон: 30 дней. Выберите меньший период.")
            return

        await state.clear()

        # Подтверждение
        confirm_text = f"📅 <b>Подтверждение обработки диапазона</b>\n\n"
        confirm_text += f"📊 Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n"
        confirm_text += f"📆 Количество дней: {days_count}\n"
        confirm_text += f"⏱ Примерное время: ~{days_count * 3} минут\n\n"
        confirm_text += f"Продолжить?"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, начать", callback_data=f"process_range_{start_date}_{end_date}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_range")
            ]
        ])

        await message.answer(confirm_text, reply_markup=keyboard, parse_mode="HTML")

    except ValueError:
        await message.reply("⚠️ Неверный формат даты. Используйте ДД.ММ.ГГГГ")
    except Exception as e:
        await message.reply(f"❌ Ошибка: {e}")


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    """Отмена текущего действия"""
    current_state = await state.get_state()
    if current_state is not None:
        await state.clear()
        await message.answer("❌ Действие отменено.", reply_markup=get_enhanced_main_keyboard())
    else:
        await message.answer("Нет активных действий для отмены.")


@dp.message()
async def handle_unknown_message(message: types.Message, state: FSMContext):
    """Улучшенная обработка неизвестных сообщений"""
    current_state = await state.get_state()

    if current_state:
        await message.answer(
            "⚠️ Неожиданный ввод. Используйте /cancel для отмены или следуйте инструкциям."
        )
    else:
        await message.answer(
            "🤔 Команда не распознана.\n"
            "Используйте /start для возврата в главное меню.",
            reply_markup=get_enhanced_main_keyboard()
        )


# ===== ОСНОВНАЯ ФУНКЦИЯ ЗАПУСКА =====

# Также добавим функцию для обновления существующей базы данных
def update_existing_database_usernames():
    """Обновляет существующую базу данных, добавляя @ к username где его нет"""
    base_file = "all_users.xlsx"

    if not os.path.exists(base_file):
        logging.info("Database file doesn't exist, skipping username update")
        return

    try:
        df = pd.read_excel(base_file)

        if 'Username' not in df.columns:
            logging.info("No Username column found")
            return

        # Подсчитываем количество записей до обновления
        initial_count = len(df)

        # Создаем маску для записей с username, которые не начинаются с @
        # Используем правильную обработку NaN значений
        has_username = df['Username'].notna()
        not_starts_with_at = df['Username'].fillna('').astype(str).str.startswith('@') == False

        # Комбинируем условия
        mask = has_username & not_starts_with_at

        # Обновляем username, добавляя @ где его нет
        if mask.any():  # Проверяем, есть ли записи для обновления
            df.loc[mask, 'Username'] = '@' + df.loc[mask, 'Username'].astype(str)

            # Сохраняем обновленную базу
            df.to_excel(base_file, index=False)
            updated_count = mask.sum()
            logging.info(f"Updated {updated_count} usernames with @ prefix out of {initial_count} total records")
        else:
            logging.info("No usernames found that need updating")

    except Exception as e:
        logging.error(f"Error updating existing database usernames: {e}")
        # Подробная диагностика
        try:
            df = pd.read_excel(base_file)
            logging.info(f"Database shape: {df.shape}")
            if 'Username' in df.columns:
                username_info = df['Username'].dtype
                null_count = df['Username'].isna().sum()
                not_null_count = df['Username'].notna().sum()
                logging.info(
                    f"Username column info: dtype={username_info}, null={null_count}, not_null={not_null_count}")

                # Показываем примеры данных
                sample_usernames = df['Username'].dropna().head(5).tolist()
                logging.info(f"Sample usernames: {sample_usernames}")
        except Exception as diag_e:
            logging.error(f"Error in diagnostics: {diag_e}")


# Добавляем эту функцию в main() для автоматического обновления при старте
async def main():
    """Улучшенная основная функция"""
    log_banner("Starting Enhanced Telegram Parser Bot v2.0")

    try:
        # Проверяем конфигурацию
        if not hasattr(parser_cfg, 'BOT_TOKEN') or not parser_cfg.BOT_TOKEN:
            logging.error("BOT_TOKEN not found in parser_cfg")
            return

        if not hasattr(parser_cfg, 'accounts') or not parser_cfg.accounts:
            logging.error("No accounts configured in parser_cfg")
            return

        # Создаем директории
        ensure_directories()

        # Обновляем существующую базу данных (добавляем @ к username)
        try:
            update_existing_database_usernames()
        except Exception as e:
            logging.error(f"Failed to update usernames, continuing: {e}")

        # Логируем конфигурацию
        logging.info(f"Bot configured with {len(parser_cfg.accounts)} account(s)")
        logging.info("Enhanced features enabled:")
        logging.info("- Advanced analytics")
        logging.info("- Multiple export formats")
        logging.info("- Automatic backups")
        logging.info("- Smart filtering")
        logging.info("- File management")
        logging.info("- Username formatting with @")

        # Запуск с улучшенными параметрами
        await dp.start_polling(
            bot,
            skip_updates=True,
            allowed_updates=['message', 'callback_query']
        )

    except Exception as e:
        logging.error(f"Critical error in main: {e}")

    finally:
        try:
            await bot.session.close()
            logging.info("Bot session closed successfully")
        except Exception as e:
            logging.error(f"Error closing bot session: {e}")


# ===== ДОПОЛНИТЕЛЬНЫЕ CALLBACK ОБРАБОТЧИКИ =====

@dp.callback_query(F.data == "detailed_analytics")
async def detailed_analytics(callback_query: types.CallbackQuery):
    """Детальная аналитика"""
    await callback_query.answer("Создаю детальную аналитику...")
    await show_analytics(callback_query.message)


@dp.callback_query(F.data == "refresh_stats")
async def refresh_stats(callback_query: types.CallbackQuery):
    """Обновление статистики"""
    await callback_query.answer("Обновляю статистику...")
    await show_enhanced_stats(callback_query.message)


@dp.callback_query(F.data.startswith("process_range_"))
async def process_date_range_confirmed(callback_query: types.CallbackQuery):
    """Обработка подтвержденного диапазона дат"""
    await callback_query.answer()

    try:
        _, _, start_str, end_str = callback_query.data.split('_')
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

        # Запускаем обработку диапазона
        await process_date_range_task(callback_query.message, start_date, end_date)

    except Exception as e:
        logging.error(f"Error processing date range: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "cancel_range")
async def cancel_range(callback_query: types.CallbackQuery):
    """Отмена обработки диапазона"""
    await callback_query.answer("Отменено")
    await bot.edit_message_text(
        "❌ Обработка диапазона дат отменена",
        callback_query.message.chat.id,
        callback_query.message.message_id
    )


@dp.callback_query(F.data == "list_reply_files")
async def list_reply_files(callback_query: types.CallbackQuery):
    """Список reply файлов"""
    await callback_query.answer()

    try:
        reply_files = list(Path('reply').glob('*.xlsx')) if Path('reply').exists() else []

        if not reply_files:
            await bot.send_message(callback_query.message.chat.id, "📋 Reply файлы не найдены")
            return

        # Сортируем по дате создания
        reply_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        files_text = f"📋 <b>Reply файлы ({len(reply_files)}):</b>\n\n"

        for i, file_path in enumerate(reply_files[:10], 1):  # Показываем только первые 10
            file_size = file_path.stat().st_size / 1024  # KB
            file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
            files_text += f"{i}. <code>{file_path.name}</code>\n"
            files_text += f"   📅 {file_date.strftime('%d.%m.%Y %H:%M')} | 💽 {file_size:.1f} KB\n\n"

        if len(reply_files) > 10:
            files_text += f"... и ещё {len(reply_files) - 10} файлов"

        await bot.send_message(callback_query.message.chat.id, files_text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error listing reply files: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "cleanup_old_files")
async def cleanup_old_files(callback_query: types.CallbackQuery):
    """Очистка старых файлов"""
    await callback_query.answer("Очищаю старые файлы...")

    try:
        cutoff_date = datetime.now() - timedelta(days=30)  # Старше 30 дней
        deleted_count = 0
        total_size_freed = 0

        # Очищаем reply файлы
        if Path('reply').exists():
            for file_path in Path('reply').glob('*.xlsx'):
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_time < cutoff_date:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    deleted_count += 1
                    total_size_freed += file_size

        # Очищаем export файлы (кроме последних)
        if Path('exports').exists():
            export_files = sorted(Path('exports').glob('*'), key=lambda x: x.stat().st_mtime, reverse=True)
            for file_path in export_files[5:]:  # Оставляем только 5 последних
                if file_path.is_file():
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    deleted_count += 1
                    total_size_freed += file_size

        cleanup_text = f"🗑 <b>Очистка завершена</b>\n\n"
        cleanup_text += f"📁 Удалено файлов: {deleted_count}\n"
        cleanup_text += f"💽 Освобождено места: {total_size_freed / (1024 * 1024):.1f} МБ"

        await bot.send_message(callback_query.message.chat.id, cleanup_text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error cleaning up files: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка очистки: {e}")


# ===== ДОПОЛНИТЕЛЬНЫЕ ФУНКЦИИ =====

async def process_date_range_task(message, start_date: datetime.date, end_date: datetime.date):
    """Обработка диапазона дат"""
    user_id = message.chat.id

    if user_id in active_tasks:
        await message.answer("⚠️ У вас уже выполняется задача.")
        return

    active_tasks[user_id] = True

    try:
        delta = end_date - start_date
        total_days = delta.days + 1

        status_msg = await message.answer(
            f"🚀 Начинаю обработку диапазона: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n"
            f"📅 Всего дней: {total_days}\n"
            f"⏱ Примерное время: {total_days * 3} минут"
        )

        processed_days = 0
        all_files = []
        errors = 0

        current_date = start_date
        while current_date <= end_date:
            try:
                # Исправляем передачу параметров для edit_message_text
                await bot.edit_message_text(
                    text=f"📅 Обрабатываю день {processed_days + 1}/{total_days}: {current_date.strftime('%d.%m.%Y')}\n"
                         f"✅ Завершено: {processed_days}\n"
                         f"❌ Ошибок: {errors}",
                    chat_id=message.chat.id,
                    message_id=status_msg.message_id
                )

                # Обрабатываем один день
                success, day_files = await start_processing_single_day_enhanced(message, current_date)

                if success:
                    processed_days += 1
                    all_files.extend(day_files)
                else:
                    errors += 1

                current_date += timedelta(days=1)

                # Пауза между днями
                if current_date <= end_date:
                    await asyncio.sleep(3)

            except Exception as e:
                logging.error(f"Error processing day {current_date}: {e}")
                errors += 1
                current_date += timedelta(days=1)

        # Завершение
        final_text = f"🎉 <b>Диапазон обработан!</b>\n\n"
        final_text += f"📊 <b>Статистика:</b>\n"
        final_text += f"• Всего дней: {total_days}\n"
        final_text += f"• Успешно: {processed_days}\n"
        final_text += f"• Ошибок: {errors}\n"
        final_text += f"• Файлов создано: {len(all_files)}"

        await bot.edit_message_text(
            text=final_text,
            chat_id=message.chat.id,
            message_id=status_msg.message_id,
            parse_mode="HTML"
        )

        # Отправляем файлы
        if all_files:
            await message.answer(f"📋 Отправляю {len(all_files)} файлов результатов...")

            for file_path in all_files:
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'rb') as f:
                            await bot.send_document(
                                message.chat.id,
                                FSInputFile(file_path, filename=os.path.basename(file_path)),
                                caption=f"📋 {os.path.basename(file_path)}"
                            )
                    except Exception as e:
                        logging.error(f"Error sending range file: {e}")

        # Отправляем обновленную базу
        await send_enhanced_database(message.chat.id, end_date)

    finally:
        active_tasks.pop(user_id, None)


async def start_processing_single_day_enhanced(message, date_target) -> tuple[bool, list]:
    """Улучшенная обработка одного дня"""
    try:
        all_results = []
        all_files = []
        success = True

        for account in parser_cfg.accounts:
            try:
                result, file_path = await get_users_from_chats_enhanced(account, date_target)
                all_results.extend(result)
                if file_path and os.path.exists(file_path):
                    all_files.append(file_path)

            except Exception as e:
                logging.error(f"Error processing account {account.get('phone_number')}: {e}")
                success = False

        return success, all_files

    except Exception as e:
        logging.error(f"Error in single day processing: {e}")
        return False, []


# ===== ОБРАБОТЧИКИ FSM СОСТОЯНИЙ =====

@dp.message(Form.waiting_for_date)
async def process_custom_date_input(message: types.Message, state: FSMContext):
    """Обработка ввода кастомной даты"""
    try:
        date_obj = datetime.strptime(message.text.strip(), "%d.%m.%Y").date()
        if date_obj > datetime.now().date():
            await message.reply("⚠️ Дата не может быть в будущем.")
            return
        await state.clear()
        await start_processing_enhanced(message, date_obj)
    except ValueError:
        await message.reply("⚠️ Неверный формат даты. Используйте ДД.ММ.ГГГГ")


@dp.message(Form.waiting_for_user_ids)
async def process_search_input(message: types.Message, state: FSMContext):
    """Обработка поиска пользователей"""
    if message.text.strip().lower() == '/cancel':
        await state.clear()
        await message.answer("❌ Поиск отменен.", reply_markup=get_enhanced_main_keyboard())
        return

    try:
        base_file = "all_users.xlsx"
        if not os.path.exists(base_file):
            await message.answer("❌ База данных не найдена")
            await state.clear()
            return

        df = pd.read_excel(base_file)
        search_term = message.text.strip()

        results = []

        # Поиск по ID
        if search_term.isdigit():
            user_id = int(search_term)
            results = df[df['User_id'] == user_id]

        # Поиск по username
        elif search_term.startswith('@'):
            username = search_term[1:]  # Убираем @
            results = df[df['Username'].str.contains(username, case=False, na=False)]

        # Поиск по имени
        else:
            results = df[df['Имя'].str.contains(search_term, case=False, na=False)]

        if results.empty:
            await message.answer(f"🔍 По запросу '<code>{search_term}</code>' ничего не найдено", parse_mode="HTML")
        else:
            search_text = f"🔍 <b>Результаты поиска:</b> {len(results)} пользователей\n\n"

            for i, (_, user) in enumerate(results.head(10).iterrows(), 1):
                search_text += f"{i}. <b>ID:</b> <code>{user['User_id']}</code>\n"
                if pd.notna(user['Username']):
                    search_text += f"   <b>Username:</b> @{user['Username']}\n"
                if pd.notna(user['Имя']):
                    search_text += f"   <b>Имя:</b> {user['Имя']}\n"
                if pd.notna(user['Источник группы']):
                    search_text += f"   <b>Группа:</b> {user['Источник группы']}\n"
                search_text += "\n"

            if len(results) > 10:
                search_text += f"... и ещё {len(results) - 10} пользователей"

            await message.answer(search_text, parse_mode="HTML")

        await state.clear()

    except Exception as e:
        logging.error(f"Search error: {e}")
        await message.answer(f"❌ Ошибка поиска: {e}")
        await state.clear()


# ===== ОБРАБОТЧИКИ ПРОПУЩЕННЫХ ДНЕЙ (из оригинала) =====

@dp.message(F.text == "📌 Парсить пропущенные дни")
async def process_missed_days(message: types.Message):
    """Улучшенная обработка пропущенных дней"""
    status_msg = await message.answer("🔍 Анализирую пропущенные дни...")

    try:
        last_date = get_last_parsed_date()
        today = datetime.now().date()

        if not last_date:
            await bot.edit_message_text(
                text="⚠️ В базе нет данных о предыдущем парсинге.\n"
                     "Сначала запустите обычный сбор данных.",
                chat_id=message.chat.id,
                message_id=status_msg.message_id
            )
            return

        missed_days = []
        current = last_date + timedelta(days=1)
        while current < today:
            missed_days.append(current)
            current += timedelta(days=1)

        if not missed_days:
            await bot.edit_message_text(
                text="✅ Нет пропущенных дней! База данных актуальна.",
                chat_id=message.chat.id,
                message_id=status_msg.message_id
            )
            return

        # Группируем дни по неделям для лучшего отображения
        display_days = missed_days[:15] if len(missed_days) > 15 else missed_days
        days_text = "\n".join([f"• {d.strftime('%d.%m.%Y (%A)')}" for d in display_days])

        if len(missed_days) > 15:
            days_text += f"\n... и ещё {len(missed_days) - 15} дней"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Обработать все", callback_data="process_all_missed_enhanced")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_missed")]
        ])

        await bot.edit_message_text(
            text=f"📌 <b>Найдено {len(missed_days)} пропущенных дней</b>\n\n{days_text}\n\n"
                 f"⏱ <b>Примерное время:</b> ~{len(missed_days) * 2} минут\n"
                 f"🔧 <b>Режим:</b> Расширенная обработка\n\n"
                 f"Продолжить?",
            chat_id=message.chat.id,
            message_id=status_msg.message_id,
            reply_markup=keyboard,
            parse_mode="HTML"
        )

        global pending_missed_days
        pending_missed_days = missed_days

    except Exception as e:
        logging.error(f"Error in process_missed_days: {e}")
        await bot.edit_message_text(
            text=f"❌ Ошибка при анализе пропущенных дней:\n{str(e)}",
            chat_id=message.chat.id,
            message_id=status_msg.message_id
        )


@dp.callback_query(F.data == "process_all_missed_enhanced")
async def handle_process_missed_enhanced(callback_query: types.CallbackQuery):
    """Улучшенная обработка пропущенных дней"""
    await callback_query.answer()

    global pending_missed_days

    if not pending_missed_days:
        await bot.edit_message_text(
            text="❌ Нет данных о пропущенных днях.",
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id
        )
        return

    user_id = callback_query.message.chat.id
    if user_id in active_tasks:
        await bot.send_message(user_id, "⚠️ У вас уже выполняется задача.")
        return

    active_tasks[user_id] = True

    try:
        total_days = len(pending_missed_days)
        processed_count = 0
        errors_count = 0
        all_reply_files = []

        await bot.edit_message_text(
            text=f"🚀 <b>Расширенная обработка {total_days} дней</b>\n"
                 f"🔧 Режим: Умные фильтры + аналитика\n"
                 f"⏱ Время: до {total_days * 2} минут",
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            parse_mode="HTML"
        )

        for i, day in enumerate(pending_missed_days, 1):
            try:
                if i % 3 == 1 or i == total_days:
                    progress_text = f"📅 <b>День {i}/{total_days}:</b> {day.strftime('%d.%m.%Y')}\n"
                    progress_text += f"✅ <b>Завершено:</b> {processed_count}\n"
                    progress_text += f"❌ <b>Ошибок:</b> {errors_count}\n"
                    progress_text += f"📁 <b>Файлов:</b> {len(all_reply_files)}"

                    await bot.edit_message_text(
                        text=progress_text,
                        chat_id=callback_query.message.chat.id,
                        message_id=callback_query.message.message_id,
                        parse_mode="HTML"
                    )

                success, day_files = await start_processing_single_day_enhanced(callback_query.message, day)

                if success:
                    processed_count += 1
                    all_reply_files.extend(day_files)
                else:
                    errors_count += 1

                if i < total_days:
                    await asyncio.sleep(2)

            except Exception as e:
                logging.error(f"Error processing missed day {day}: {e}")
                errors_count += 1

        # Создаем итоговый отчет
        final_text = f"🎉 <b>Обработка завершена!</b>\n\n"
        final_text += f"📊 <b>Детальная статистика:</b>\n"
        final_text += f"• Всего дней: {total_days}\n"
        final_text += f"• Успешно обработано: {processed_count}\n"
        final_text += f"• Ошибок: {errors_count}\n"
        final_text += f"• Создано reply файлов: {len(all_reply_files)}\n"
        final_text += f"• Эффективность: {processed_count / total_days * 100:.1f}%"

        await bot.edit_message_text(
            text=final_text,
            chat_id=callback_query.message.chat.id,
            message_id=callback_query.message.message_id,
            parse_mode="HTML"
        )

        # Отправляем все файлы
        if all_reply_files:
            await callback_query.message.answer(f"📋 Отправляю {len(all_reply_files)} файлов результатов...")

            for file_path in all_reply_files:
                if os.path.exists(file_path):
                    try:
                        with open(file_path, 'rb') as file:
                            await bot.send_document(
                                callback_query.message.chat.id,
                                FSInputFile(file_path, filename=os.path.basename(file_path)),
                                caption=f"📋 {os.path.basename(file_path)}"
                            )
                    except Exception as e:
                        logging.error(f"Error sending missed day file: {e}")

        await send_enhanced_database(callback_query.message.chat.id, max(pending_missed_days))

    finally:
        active_tasks.pop(user_id, None)
        pending_missed_days = []


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Critical error: {e}")
        raise