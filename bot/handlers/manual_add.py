"""
Обработчик ручного добавления пользователей в базу данных
"""

import logging
from datetime import datetime
from aiogram import types, F
from aiogram.fsm.context import FSMContext

from ..aiogram_loader import dp
from ..states.form_states import Form
from ..keyboards.main_menu import get_enhanced_main_keyboard
from ..utils.database import DatabaseManager


@dp.message(F.text == "➕ Добавить ID вручную")
async def manual_add_menu(message: types.Message, state: FSMContext):
    """Меню ручного добавления ID"""
    await state.set_state(Form.waiting_for_manual_ids)
    
    add_text = (
        "➕ <b>Ручное добавление пользователей</b>\n\n"
        "Введите данные пользователей в одном из форматов:\n\n"
        "<b>Формат 1:</b> Только ID\n"
        "<code>123456789</code>\n\n"
        "<b>Формат 2:</b> ID с username\n"
        "<code>123456789 @username</code>\n\n"
        "<b>Формат 3:</b> ID с полными данными\n"
        "<code>123456789 @username Имя Фамилия</code>\n\n"
        "<b>Множественное добавление:</b>\n"
        "Каждый пользователь с новой строки:\n"
        "<code>123456789 @user1\n"
        "987654321 @user2 Иван Петров\n"
        "555555555</code>\n\n"
        "💡 <b>Советы:</b>\n"
        "• Username должен начинаться с @\n"
        "• Имя и фамилия - опционально\n"
        "• Можно добавить до 50 пользователей за раз"
    )
    
    from ..keyboards.settings_menu import get_cancel_keyboard
    await message.answer(add_text, reply_markup=get_cancel_keyboard(), parse_mode="HTML")


@dp.message(Form.waiting_for_manual_ids)
async def process_manual_ids(message: types.Message, state: FSMContext):
    """Обработка ручного добавления ID"""
    try:
        lines = message.text.strip().split('\n')
        
        if len(lines) > 50:
            await message.answer("⚠️ Максимум 50 пользователей за раз. Попробуйте снова.")
            return
        
        added_users = []
        skipped_users = []
        errors = []
        
        status_msg = await message.answer("⏳ Обрабатываю...")
        
        for i, line in enumerate(lines, 1):
            try:
                parts = line.strip().split()
                
                if not parts:
                    continue
                
                # Парсим ID
                try:
                    user_id = int(parts[0])
                except ValueError:
                    errors.append(f"Строка {i}: неверный ID '{parts[0]}'")
                    continue
                
                # Проверяем, существует ли уже
                existing = DatabaseManager.get_user_by_id(user_id)
                if existing is not None and not existing.empty:
                    skipped_users.append(user_id)
                    continue
                
                # Парсим остальные данные
                username = None
                first_name = None
                last_name = None
                
                for part in parts[1:]:
                    if part.startswith('@'):
                        username = part
                    elif first_name is None:
                        first_name = part
                    elif last_name is None:
                        last_name = part
                
                # Добавляем в базу
                user_data = {
                    'user_id': user_id,
                    'username': username,
                    'first_name': first_name,
                    'last_name': last_name,
                    'source_group': 'Ручное добавление',
                    'collection_time': datetime.now(),
                    'is_premium': False,
                    'is_verified': False,
                    'is_bot': False,
                    'is_fake': False,
                    'is_scam': False
                }
                
                success = DatabaseManager.add_user(user_data)
                
                if success:
                    added_users.append(user_id)
                else:
                    errors.append(f"Строка {i}: ошибка добавления ID {user_id}")
                
            except Exception as e:
                logging.error(f"Error processing line {i}: {e}")
                errors.append(f"Строка {i}: {str(e)}")
        
        # Формируем отчет
        report = f"📊 <b>Результаты добавления</b>\n\n"
        
        if added_users:
            report += f"✅ <b>Добавлено:</b> {len(added_users)}\n"
            # Показываем первые 10 добавленных ID
            preview = added_users[:10]
            report += "<code>" + ", ".join(map(str, preview)) + "</code>"
            if len(added_users) > 10:
                report += f"\n... и ещё {len(added_users) - 10}\n"
            else:
                report += "\n"
        
        if skipped_users:
            report += f"\n⏭ <b>Пропущено (уже есть):</b> {len(skipped_users)}\n"
        
        if errors:
            report += f"\n❌ <b>Ошибок:</b> {len(errors)}\n"
            # Показываем первые 5 ошибок
            for error in errors[:5]:
                report += f"• {error}\n"
            if len(errors) > 5:
                report += f"... и ещё {len(errors) - 5} ошибок\n"
        
        # Обновляем статистику БД
        stats = DatabaseManager.get_database_stats()
        report += f"\n📈 <b>Всего в базе:</b> {stats.get('total_users', 0):,} пользователей"
        
        await message.answer(report, parse_mode="HTML", reply_markup=get_enhanced_main_keyboard())
        
        try:
            await status_msg.delete()
        except:
            pass
        
        await state.clear()
    
    except Exception as e:
        logging.error(f"Error in manual add: {e}")
        await message.answer(f"❌ Ошибка обработки: {e}", reply_markup=get_enhanced_main_keyboard())
        await state.clear()

