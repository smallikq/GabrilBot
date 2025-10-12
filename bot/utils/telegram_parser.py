import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Tuple, Optional, List, Dict, Any, Set
import pandas as pd
from tqdm import tqdm
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError


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

        # Улучшенная обработка базы данных (SQL)
        if all_users:
            from .database import DatabaseManager
            # Создаем бэкап перед изменениями
            await DatabaseManager.backup_database()

            try:
                # Получаем существующие ID пользователей из SQL базы
                existing_ids = DatabaseManager.get_existing_user_ids()
                
                # Фильтруем новых пользователей
                new_users = []
                for row in all_users:
                    if row[0] not in existing_ids:
                        new_users.append(row)
                
                if new_users:
                    # Вставляем новых пользователей в SQL базу
                    inserted_count = DatabaseManager.insert_users(new_users)
                    result_message.append(f"✅ Database updated: +{inserted_count} new users")
                    logging.info(f"Database updated with {inserted_count} new users")
                    
                    # Создаем файл результатов (Excel для совместимости)
                    date_str = date_target.strftime('%Y-%m-%d')
                    file_path = f'bot/data/reply/reply_{account["phone_number"]}_{date_str}.xlsx'
                    
                    try:
                        df_new = pd.DataFrame(list(new_users), columns=COLUMNS)
                        df_new.to_excel(file_path, index=False)
                        result_message.append(f"💾 Reply file created: {len(new_users)} users")
                        logging.info(f"Reply file saved: {file_path}")
                    except Exception as e:
                        logging.error(f"Error saving reply file: {e}")
                        result_message.append(f"⚠️ Reply file error: {e}")
                        file_path = None
                else:
                    result_message.append(f"📌 No new users found for {account['phone_number']}")
                    
            except Exception as e:
                logging.error(f"Error saving to database: {e}")
                result_message.append(f"⚠️ Database save error: {e}")
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

