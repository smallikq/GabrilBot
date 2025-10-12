import asyncio
import os
import logging
from datetime import datetime, timedelta
from aiogram import types, F
from aiogram.types import FSInputFile

from ..utils.file_utils import get_last_parsed_date
from ..utils.telegram_parser import get_users_from_chats_enhanced
from ..keyboards.settings_menu import get_missed_days_keyboard
from ..utils.database import DatabaseManager
from ..aiogram_loader import dp, bot, pending_missed_days, active_tasks


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

        await bot.edit_message_text(
            text=f"📌 <b>Найдено {len(missed_days)} пропущенных дней</b>\n\n{days_text}\n\n"
                 f"⏱ <b>Примерное время:</b> ~{len(missed_days) * 2} минут\n"
                 f"🔧 <b>Режим:</b> Расширенная обработка\n\n"
                 f"Продолжить?",
            chat_id=message.chat.id,
            message_id=status_msg.message_id,
            reply_markup=get_missed_days_keyboard(),
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


async def start_processing_single_day_enhanced(message, date_target) -> tuple[bool, list]:
    """Улучшенная обработка одного дня"""
    try:
        all_results = []
        all_files = []
        success = True

        # Импортируем конфигурацию
        import os
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from bot.data.parser_cfg import accounts

        for account in accounts:
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


async def send_enhanced_database(chat_id: int, date_target: datetime.date):
    """Отправка улучшенной базы данных с аналитикой (экспорт из SQL)"""
    try:
        # Экспортируем базу данных из SQL в Excel
        temp_file = DatabaseManager.export_to_excel()
        
        if temp_file and os.path.exists(temp_file):
            # Получаем статистику
            stats = DatabaseManager.get_database_stats()

            caption = f"📚 <b>Обновленная база данных</b>\n\n"
            caption += f"📊 Всего пользователей: {stats.get('total_users', 0):,}\n"
            caption += f"📅 Обработка за: {date_target.strftime('%d.%m.%Y')}\n"

            if stats.get('with_username', 0) > 0:
                caption += f"🏷 С username: {stats['with_username']:,}\n"
            
            if stats.get('premium_users', 0) > 0:
                caption += f"⭐ Premium: {stats['premium_users']:,}\n"

            await bot.send_document(
                chat_id,
                FSInputFile(temp_file, filename='all_users.xlsx'),
                caption=caption,
                parse_mode="HTML"
            )
            
            # Удаляем временный файл после отправки
            try:
                os.remove(temp_file)
            except:
                pass
        else:
            await bot.send_message(chat_id, "❌ Ошибка экспорта базы данных")
            
    except Exception as e:
        logging.error(f"Error sending enhanced database: {e}")
        await bot.send_message(chat_id, f"❌ Ошибка отправки базы данных: {e}")

