import os
import pandas as pd
import logging
from datetime import datetime
from aiogram import types, F
from aiogram.fsm.context import FSMContext

from ..states.form_states import Form
from ..keyboards.main_menu import get_enhanced_main_keyboard
from ..aiogram_loader import dp


@dp.message(F.text == "🔎 Поиск пользователей")
async def search_users(message: types.Message, state: FSMContext):
    """Поиск пользователей в базе"""
    await state.set_state(Form.waiting_for_user_ids)

    search_text = (
        "🔎 <b>Поиск пользователей</b>\n\n"
        "Введите один из вариантов:\n"
        "• <code>ID пользователя</code> (например: 123456789)\n"
        "• <code>@username</code> (например: @john_doe)\n"
        "• <code>Имя пользователя</code> (например: Иван)\n"
        "• <code>Несколько ID</code> через запятую"
    )
    
    from ..keyboards.settings_menu import get_cancel_keyboard
    await message.answer(search_text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")


@dp.callback_query(F.data == "cancel_action")
async def cancel_search_action(callback_query: types.CallbackQuery, state: FSMContext):
    """Отмена текущего действия"""
    from ..aiogram_loader import bot
    await callback_query.answer("Отменено")
    await state.clear()
    await bot.edit_message_text(
        text="❌ Действие отменено",
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id
    )


@dp.message(Form.waiting_for_user_ids)
async def process_search_input(message: types.Message, state: FSMContext):
    """Обработка поиска пользователей"""
    try:
        from ..utils.database import DatabaseManager
        
        search_term = message.text.strip()
        
        # Используем метод поиска из DatabaseManager
        results = DatabaseManager.search_users(search_term)

        if results.empty:
            await message.answer(f"🔎 По запросу '<code>{search_term}</code>' ничего не найдено", parse_mode="HTML")
        else:
            search_text = f"🔎 <b>Результаты поиска:</b> {len(results)} пользователей\n\n"

            for i, (_, user) in enumerate(results.head(10).iterrows(), 1):
                search_text += f"{i}. <b>ID:</b> <code>{user['User_id']}</code>\n"
                if pd.notna(user.get('Username')):
                    search_text += f"   <b>Username:</b> {user['Username']}\n"
                if pd.notna(user.get('Имя')):
                    search_text += f"   <b>Имя:</b> {user['Имя']}\n"
                if pd.notna(user.get('Источник группы')):
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
        "⚠️ Большие диапазоны могут занять много времени"
    )
    
    from ..keyboards.settings_menu import get_cancel_keyboard
    await message.answer(range_text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")


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

        from ..keyboards.settings_menu import get_date_range_confirmation_keyboard
        keyboard = get_date_range_confirmation_keyboard(start_date, end_date)

        await message.answer(confirm_text, reply_markup=keyboard, parse_mode="HTML")

    except ValueError:
        await message.reply("⚠️ Неверный формат даты. Используйте ДД.ММ.ГГГГ")
    except Exception as e:
        await message.reply(f"❌ Ошибка: {e}")


@dp.callback_query(F.data.startswith("process_range_"))
async def handle_process_range(callback_query: types.CallbackQuery):
    """Обработка диапазона дат"""
    await callback_query.answer()
    
    try:
        from ..aiogram_loader import bot, active_tasks
        import asyncio
        
        # Извлекаем даты из callback_data
        parts = callback_query.data.split('_')
        start_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
        end_date = datetime.strptime(parts[3], "%Y-%m-%d").date()
        
        user_id = callback_query.message.chat.id
        if user_id in active_tasks:
            await bot.send_message(user_id, "⚠️ У вас уже выполняется задача.")
            return
        
        active_tasks[user_id] = True
        
        try:
            # Подготовка
            delta = end_date - start_date
            days_count = delta.days + 1
            
            await bot.edit_message_text(
                text=f"🚀 <b>Начинаю обработку {days_count} дней</b>\n"
                     f"📊 Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}",
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                parse_mode="HTML"
            )
            
            processed_count = 0
            errors_count = 0
            all_files = []
            
            # Импортируем функции
            import sys
            sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            from bot.data.parser_cfg import accounts
            from bot.utils.telegram_parser import get_users_from_chats_enhanced
            
            # Обработка каждого дня
            current_date = start_date
            while current_date <= end_date:
                try:
                    progress_text = f"📅 <b>День {processed_count + 1}/{days_count}</b>\n"
                    progress_text += f"📊 Обрабатываю: {current_date.strftime('%d.%m.%Y')}\n"
                    progress_text += f"✅ Завершено: {processed_count}\n"
                    progress_text += f"❌ Ошибок: {errors_count}"
                    
                    if (processed_count + 1) % 3 == 1 or current_date == end_date:
                        await bot.edit_message_text(
                            text=progress_text,
                            chat_id=callback_query.message.chat.id,
                            message_id=callback_query.message.message_id,
                            parse_mode="HTML"
                        )
                    
                    # Обработка для каждого аккаунта
                    for account in accounts:
                        try:
                            result, file_path = await get_users_from_chats_enhanced(account, current_date)
                            if file_path and os.path.exists(file_path):
                                all_files.append(file_path)
                        except Exception as e:
                            logging.error(f"Error processing account {account.get('phone_number')} for {current_date}: {e}")
                    
                    processed_count += 1
                    current_date += pd.Timedelta(days=1)
                    
                    if current_date <= end_date:
                        await asyncio.sleep(2)
                    
                except Exception as e:
                    logging.error(f"Error processing date {current_date}: {e}")
                    errors_count += 1
                    current_date += pd.Timedelta(days=1)
            
            # Создаём объединённый файл за весь диапазон
            combined_file = None
            if all_files:
                try:
                    # Читаем все файлы и объединяем данные
                    all_dataframes = []
                    for file_path in all_files:
                        if os.path.exists(file_path):
                            df = pd.read_excel(file_path)
                            all_dataframes.append(df)

                    if all_dataframes:
                        # Объединяем все DataFrame
                        combined_df = pd.concat(all_dataframes, ignore_index=True)

                        # Удаляем дубликаты по User_id (оставляем последнее вхождение)
                        combined_df = combined_df.drop_duplicates(subset=['User_id'], keep='last')

                        # Создаём объединённый файл
                        date_range_str = f"{start_date.strftime('%Y-%m-%d')}_to_{end_date.strftime('%Y-%m-%d')}"
                        combined_file = f'bot/data/exports/range_{date_range_str}.xlsx'

                        os.makedirs('bot/data/exports', exist_ok=True)
                        combined_df.to_excel(combined_file, index=False)

                        logging.info(f"Combined file created: {combined_file} with {len(combined_df)} unique users")

                except Exception as e:
                    logging.error(f"Error creating combined file: {e}")

            # Итоговый отчет
            final_text = f"🎉 <b>Обработка диапазона завершена!</b>\n\n"
            final_text += f"📊 <b>Статистика:</b>\n"
            final_text += f"• Обработано дней: {processed_count}/{days_count}\n"
            final_text += f"• Ошибок: {errors_count}\n"
            final_text += f"• Создано файлов: {len(all_files)}\n"
            final_text += f"• Эффективность: {processed_count / days_count * 100:.1f}%"

            await bot.edit_message_text(
                text=final_text,
                chat_id=callback_query.message.chat.id,
                message_id=callback_query.message.message_id,
                parse_mode="HTML"
            )

            # Отправляем объединённый файл за диапазон
            if combined_file and os.path.exists(combined_file):
                from aiogram.types import FSInputFile
                try:
                    combined_df_stats = pd.read_excel(combined_file)
                    caption = f"📋 <b>Объединённый файл за период</b>\n\n"
                    caption += f"📅 Период: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}\n"
                    caption += f"📊 Всего уникальных пользователей: {len(combined_df_stats):,}\n"
                    caption += f"📁 Дней обработано: {processed_count}"

                    await bot.send_document(
                        callback_query.message.chat.id,
                        FSInputFile(combined_file, filename=f'range_{start_date.strftime("%d.%m.%Y")}-{end_date.strftime("%d.%m.%Y")}.xlsx'),
                        caption=caption,
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logging.error(f"Error sending combined file: {e}")
                    await bot.send_message(callback_query.message.chat.id, f"⚠️ Ошибка отправки файла: {e}")

            # Отправляем обновлённую базу данных
            from bot.handlers.parser import send_enhanced_database
            await send_enhanced_database(callback_query.message.chat.id, end_date)
            
        finally:
            active_tasks.pop(user_id, None)
    
    except Exception as e:
        logging.error(f"Error in process_range: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка обработки: {e}")


@dp.callback_query(F.data == "cancel_range")
async def cancel_range(callback_query: types.CallbackQuery):
    """Отмена обработки диапазона"""
    from ..aiogram_loader import bot
    await callback_query.answer("Отменено")
    await bot.edit_message_text(
        text="❌ Обработка диапазона отменена",
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id
    )


@dp.callback_query(F.data == "cancel_missed")
async def cancel_missed(callback_query: types.CallbackQuery):
    """Отмена обработки пропущенных дней"""
    from ..aiogram_loader import bot, pending_missed_days
    
    pending_missed_days.clear()
    await callback_query.answer("Отменено")
    await bot.edit_message_text(
        text="❌ Обработка пропущенных дней отменена",
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id
    )

