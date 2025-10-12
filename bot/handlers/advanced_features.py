"""
Расширенные функции бота: бэкапы, планировщик, фильтры
"""

import os
import logging
import shutil
from datetime import datetime
from aiogram import types, F
from aiogram.types import FSInputFile
from aiogram.fsm.context import FSMContext

from ..aiogram_loader import dp, bot
from ..utils.database import DatabaseManager
from ..keyboards.main_menu import get_enhanced_main_keyboard
from ..states.form_states import Form


@dp.message(F.text == "💾 Создать бэкап")
async def create_backup_manual(message: types.Message):
    """Создание ручного бэкапа базы данных"""
    try:
        status_msg = await message.answer("💾 Создаю бэкап базы данных...")
        
        # Создаем директорию для бэкапов
        backup_dir = 'bot/data/backups'
        os.makedirs(backup_dir, exist_ok=True)
        
        # Копируем базу данных
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f'{backup_dir}/backup_{timestamp}.db'
        
        db_path = DatabaseManager.DB_PATH
        if os.path.exists(db_path):
            shutil.copy2(db_path, backup_path)
            
            # Получаем размер файла
            backup_size = os.path.getsize(backup_path) / (1024 * 1024)  # МБ
            
            # Получаем статистику
            stats = DatabaseManager.get_database_stats()
            
            caption = f"💾 <b>Бэкап базы данных</b>\n\n"
            caption += f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            caption += f"💽 Размер: {backup_size:.2f} МБ\n"
            caption += f"👥 Пользователей: {stats.get('total_users', 0):,}\n"
            caption += f"🏷 С username: {stats.get('with_username', 0):,}"
            
            # Отправляем бэкап файл
            await bot.send_document(
                message.chat.id,
                FSInputFile(backup_path, filename=f'backup_{timestamp}.db'),
                caption=caption,
                parse_mode="HTML"
            )
            
            await bot.delete_message(message.chat.id, status_msg.message_id)
            
        else:
            await bot.edit_message_text(
                "❌ База данных не найдена",
                message.chat.id,
                status_msg.message_id
            )
    
    except Exception as e:
        logging.error(f"Error creating backup: {e}")
        await message.answer(f"❌ Ошибка создания бэкапа: {e}")


@dp.message(F.text == "🔍 Расширенный поиск")
async def advanced_search_menu(message: types.Message):
    """Меню расширенного поиска"""
    from ..keyboards.settings_menu import get_advanced_search_keyboard
    
    search_text = (
        "🔍 <b>Расширенный поиск</b>\n\n"
        "Выберите критерий поиска:"
    )
    
    await message.answer(search_text, reply_markup=get_advanced_search_keyboard(), parse_mode="HTML")


@dp.callback_query(F.data == "search_by_premium")
async def search_by_premium(callback_query: types.CallbackQuery):
    """Поиск Premium пользователей"""
    await callback_query.answer("Ищу Premium пользователей...")
    
    try:
        with DatabaseManager.get_connection() as conn:
            query = '''
                SELECT user_id, username, first_name, last_name, source_group
                FROM users
                WHERE is_premium = 1
                ORDER BY collected_at DESC
                LIMIT 50
            '''
            import pandas as pd
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "💎 Premium пользователи не найдены")
            return
        
        result_text = f"💎 <b>Premium пользователи ({len(df)}):</b>\n\n"
        
        for i, (_, user) in enumerate(df.head(20).iterrows(), 1):
            result_text += f"{i}. <b>ID:</b> <code>{user['user_id']}</code>\n"
            if pd.notna(user['username']):
                result_text += f"   <b>Username:</b> {user['username']}\n"
            if pd.notna(user['first_name']):
                result_text += f"   <b>Имя:</b> {user['first_name']}\n"
            result_text += "\n"
        
        if len(df) > 20:
            result_text += f"... и ещё {len(df) - 20} пользователей"
        
        await bot.send_message(callback_query.message.chat.id, result_text, parse_mode="HTML")
    
    except Exception as e:
        logging.error(f"Error searching premium users: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка поиска: {e}")


@dp.callback_query(F.data == "search_by_group")
async def search_by_group(callback_query: types.CallbackQuery, state: FSMContext):
    """Поиск по группе"""
    await callback_query.answer()
    await state.set_state(Form.waiting_for_group_filter)
    
    await bot.send_message(
        callback_query.message.chat.id,
        "🎯 Введите название группы для поиска:\n"
        "Например: <code>Название группы</code>\n\n"
        "Или отправьте /cancel для отмены",
        parse_mode="HTML"
    )


@dp.message(Form.waiting_for_group_filter)
async def process_group_filter(message: types.Message, state: FSMContext):
    """Обработка поиска по группе"""
    if message.text.strip().lower() == '/cancel':
        await state.clear()
        await message.answer("❌ Поиск отменен.", reply_markup=get_enhanced_main_keyboard())
        return
    
    try:
        group_name = message.text.strip()
        
        with DatabaseManager.get_connection() as conn:
            query = '''
                SELECT user_id, username, first_name, last_name, source_group, collected_at
                FROM users
                WHERE source_group LIKE ?
                ORDER BY collected_at DESC
                LIMIT 100
            '''
            import pandas as pd
            df = pd.read_sql_query(query, conn, params=(f'%{group_name}%',))
        
        if df.empty:
            await message.answer(f"🔍 В группе '<code>{group_name}</code>' пользователи не найдены", parse_mode="HTML")
        else:
            result_text = f"🎯 <b>Пользователи из группы '{group_name}':</b>\n"
            result_text += f"<b>Найдено:</b> {len(df)}\n\n"
            
            # Группируем по источникам
            groups = df.groupby('source_group').size()
            result_text += "<b>Распределение по группам:</b>\n"
            for group, count in groups.head(10).items():
                result_text += f"• {group[:40]}: {count}\n"
            
            result_text += f"\n<b>Всего уникальных пользователей:</b> {df['user_id'].nunique()}"
            
            await message.answer(result_text, parse_mode="HTML")
            
            # Предлагаем экспорт
            if len(df) > 0:
                from ..keyboards.settings_menu import get_export_filter_keyboard
                await message.answer(
                    "💾 Хотите экспортировать эти результаты?",
                    reply_markup=get_export_filter_keyboard(group_name)
                )
        
        await state.clear()
    
    except Exception as e:
        logging.error(f"Error filtering by group: {e}")
        await message.answer(f"❌ Ошибка поиска: {e}")
        await state.clear()


@dp.callback_query(F.data.startswith("export_filter_"))
async def export_filtered_results(callback_query: types.CallbackQuery):
    """Экспорт отфильтрованных результатов"""
    await callback_query.answer("Экспортирую...")
    
    try:
        # Извлекаем группу из callback_data
        group_name = callback_query.data.replace("export_filter_", "")
        
        with DatabaseManager.get_connection() as conn:
            query = '''
                SELECT *
                FROM users
                WHERE source_group LIKE ?
                ORDER BY collected_at DESC
            '''
            import pandas as pd
            df = pd.read_sql_query(query, conn, params=(f'%{group_name}%',))
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "❌ Нет данных для экспорта")
            return
        
        # Экспортируем в Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = f'bot/data/exports/filtered_{timestamp}.xlsx'
        os.makedirs('bot/data/exports', exist_ok=True)
        
        df.to_excel(export_path, index=False)
        
        await bot.send_document(
            callback_query.message.chat.id,
            FSInputFile(export_path, filename=f'filtered_{group_name}_{timestamp}.xlsx'),
            caption=f"📊 Экспорт по группе: {group_name}\n👥 Пользователей: {len(df)}"
        )
        
        # Удаляем временный файл
        try:
            os.remove(export_path)
        except:
            pass
    
    except Exception as e:
        logging.error(f"Error exporting filtered results: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка экспорта: {e}")


@dp.callback_query(F.data == "search_by_date")
async def search_by_date(callback_query: types.CallbackQuery, state: FSMContext):
    """Поиск по дате"""
    await callback_query.answer()
    await state.set_state(Form.waiting_for_date)
    
    await bot.send_message(
        callback_query.message.chat.id,
        "📅 Введите дату для поиска в формате ДД.ММ.ГГГГ:\n"
        "Например: <code>15.01.2024</code>\n\n"
        "Или отправьте /cancel для отмены",
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "search_recent")
async def search_recent(callback_query: types.CallbackQuery):
    """Поиск последних добавленных пользователей"""
    await callback_query.answer("Получаю последние записи...")
    
    try:
        with DatabaseManager.get_connection() as conn:
            query = '''
                SELECT user_id, username, first_name, source_group, collected_at
                FROM users
                ORDER BY collected_at DESC
                LIMIT 30
            '''
            import pandas as pd
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "📊 Нет данных")
            return
        
        result_text = f"🕐 <b>Последние {len(df)} добавленных:</b>\n\n"
        
        for i, (_, user) in enumerate(df.iterrows(), 1):
            result_text += f"{i}. <code>{user['user_id']}</code>"
            if pd.notna(user['username']):
                result_text += f" | {user['username']}"
            if pd.notna(user['first_name']):
                result_text += f" | {user['first_name']}"
            
            # Форматируем дату
            try:
                date = pd.to_datetime(user['collected_at'])
                result_text += f"\n   📅 {date.strftime('%d.%m.%Y %H:%M')}"
            except:
                pass
            
            result_text += "\n\n"
        
        await bot.send_message(callback_query.message.chat.id, result_text, parse_mode="HTML")
    
    except Exception as e:
        logging.error(f"Error searching recent: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "search_by_verified")
async def search_by_verified(callback_query: types.CallbackQuery):
    """Поиск Verified пользователей"""
    await callback_query.answer("Ищу Verified пользователей...")
    
    try:
        with DatabaseManager.get_connection() as conn:
            query = '''
                SELECT user_id, username, first_name, last_name, source_group
                FROM users
                WHERE is_verified = 1
                ORDER BY collected_at DESC
                LIMIT 50
            '''
            import pandas as pd
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "✅ Verified пользователи не найдены")
            return
        
        result_text = f"✅ <b>Verified пользователи ({len(df)}):</b>\n\n"
        
        for i, (_, user) in enumerate(df.head(20).iterrows(), 1):
            result_text += f"{i}. <b>ID:</b> <code>{user['user_id']}</code>\n"
            if pd.notna(user['username']):
                result_text += f"   <b>Username:</b> {user['username']}\n"
            if pd.notna(user['first_name']):
                result_text += f"   <b>Имя:</b> {user['first_name']}\n"
            result_text += "\n"
        
        if len(df) > 20:
            result_text += f"... и ещё {len(df) - 20} пользователей"
        
        await bot.send_message(callback_query.message.chat.id, result_text, parse_mode="HTML")
    
    except Exception as e:
        logging.error(f"Error searching verified users: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка поиска: {e}")


@dp.callback_query(F.data == "groups_stats")
async def groups_stats(callback_query: types.CallbackQuery):
    """Статистика по группам"""
    await callback_query.answer("Собираю статистику...")
    
    try:
        with DatabaseManager.get_connection() as conn:
            query = '''
                SELECT 
                    source_group,
                    COUNT(*) as total_users,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN is_premium = 1 THEN 1 ELSE 0 END) as premium_count,
                    SUM(CASE WHEN is_verified = 1 THEN 1 ELSE 0 END) as verified_count,
                    MAX(collected_at) as last_collection
                FROM users
                WHERE source_group IS NOT NULL AND source_group != ''
                GROUP BY source_group
                ORDER BY total_users DESC
                LIMIT 20
            '''
            import pandas as pd
            df = pd.read_sql_query(query, conn)
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "📊 Нет данных по группам")
            return
        
        result_text = f"📊 <b>Статистика по группам (топ-{len(df)}):</b>\n\n"
        
        for i, (_, group) in enumerate(df.iterrows(), 1):
            group_name = group['source_group'][:40]  # Ограничиваем длину
            result_text += f"{i}. <b>{group_name}</b>\n"
            result_text += f"   👥 Всего: {group['total_users']} | Уникальных: {group['unique_users']}\n"
            
            if group['premium_count'] > 0:
                result_text += f"   💎 Premium: {group['premium_count']}"
            if group['verified_count'] > 0:
                result_text += f" | ✅ Verified: {group['verified_count']}"
            
            result_text += "\n\n"
        
        # Общая статистика
        total_groups = len(df)
        total_users = df['total_users'].sum()
        total_unique = df['unique_users'].sum()
        
        result_text += f"<b>Итого:</b>\n"
        result_text += f"📁 Групп: {total_groups}\n"
        result_text += f"👥 Всего записей: {total_users}\n"
        result_text += f"🔢 Уникальных пользователей: {total_unique}"
        
        await bot.send_message(callback_query.message.chat.id, result_text, parse_mode="HTML")
    
    except Exception as e:
        logging.error(f"Error getting groups stats: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка получения статистики: {e}")


@dp.callback_query(F.data == "cancel_export")
async def cancel_export(callback_query: types.CallbackQuery):
    """Отмена экспорта"""
    await callback_query.answer("Отменено")
    await bot.edit_message_text(
        text="❌ Экспорт отменен",
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id
    )

