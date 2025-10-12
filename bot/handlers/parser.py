from aiogram import types, F
from aiogram.fsm.context import FSMContext
from datetime import datetime

from ..keyboards.date_selection import get_enhanced_date_keyboard
from ..aiogram_loader import dp
from ..states.form_states import Form


@dp.message(F.text == "🚀 Запустить сбор данных")
async def process_start_command(message: types.Message):
    """Запуск сбора данных"""
    await message.answer("📅 Выберите дату для сбора данных:", reply_markup=get_enhanced_date_keyboard())


@dp.callback_query(F.data.startswith('date_'))
async def process_date_selection(callback_query: types.CallbackQuery):
    """Обработка выбора даты"""
    await callback_query.answer()
    date_str = callback_query.data.split('_')[1]
    try:
        date_obj = datetime.strptime(date_str, "%d.%m.%Y").date()
        await start_processing_enhanced(callback_query.message, date_obj)
    except ValueError:
        from ..aiogram_loader import bot
        await bot.send_message(callback_query.message.chat.id, "⚠️ Неверный формат даты.")


@dp.callback_query(F.data == 'custom_date')
async def process_custom_date(callback_query: types.CallbackQuery, state: FSMContext):
    """Обработка кастомной даты"""
    await callback_query.answer()
    await state.set_state(Form.waiting_for_date)
    from ..aiogram_loader import bot
    await bot.send_message(
        callback_query.message.chat.id,
        "📅 Введите дату в формате ДД.ММ.ГГГГ (например: 15.01.2024):"
    )


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


async def start_processing_enhanced(message, date_target):
    """Улучшенная функция обработки данных"""
    from ..aiogram_loader import bot, active_tasks
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
        
        # Импортируем конфигурацию
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from bot.data.parser_cfg import accounts
        total_accounts = len(accounts)

        from ..utils.telegram_parser import get_users_from_chats_enhanced

        for i, account in enumerate(accounts, 1):
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
                import logging
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
        from aiogram.types import FSInputFile
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
                    import logging
                    logging.error(f"Error sending file: {e}")

        # Отправляем обновленную базу с аналитикой
        await send_enhanced_database(message.chat.id, date_target)

    finally:
        active_tasks.pop(user_id, None)


async def send_enhanced_database(chat_id: int, date_target: datetime.date):
    """Отправка улучшенной базы данных с аналитикой (экспорт из SQL)"""
    from ..aiogram_loader import bot
    from ..utils.database import DatabaseManager
    from aiogram.types import FSInputFile
    import logging
    
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

