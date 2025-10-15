from aiogram import types, F

from ..keyboards.settings_menu import get_settings_keyboard
from ..aiogram_loader import dp, user_settings, get_bot


def get_settings_text(user_id: int) -> str:
    """Получить текст настроек для пользователя"""
    settings = user_settings.get(user_id, {})
    
    settings_text = f"⚙️ <b>Настройки бота</b>\n\n"
    settings_text += f"🔔 Уведомления: {'✅' if settings.get('notifications', True) else '❌'}\n"
    settings_text += f"💾 Автобэкапы: {'✅' if settings.get('auto_backup', True) else '❌'}\n"
    settings_text += f"📊 Формат экспорта: {settings.get('export_format', 'excel').upper()}\n"
    
    return settings_text


@dp.message(F.text == "⚙️ Настройки")
async def show_settings(message: types.Message):
    """Показать настройки пользователя"""
    user_id = message.from_user.id
    settings = user_settings.get(user_id, {})
    
    settings_text = get_settings_text(user_id)
    await message.answer(settings_text, reply_markup=get_settings_keyboard(settings), parse_mode="HTML")


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

    # Обновляем сообщение
    bot = get_bot()
    settings = user_settings.get(user_id, {})
    settings_text = get_settings_text(user_id)
    await bot.edit_message_text(
        text=settings_text,
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        reply_markup=get_settings_keyboard(settings),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "toggle_backup")
async def toggle_backup(callback_query: types.CallbackQuery):
    """Переключение автобэкапов"""
    user_id = callback_query.from_user.id
    current = user_settings.get(user_id, {}).get('auto_backup', True)

    if user_id not in user_settings:
        user_settings[user_id] = {}
    user_settings[user_id]['auto_backup'] = not current

    status = "включены" if not current else "выключены"
    await callback_query.answer(f"Автобэкапы {status}")

    # Обновляем сообщение
    bot = get_bot()
    settings = user_settings.get(user_id, {})
    settings_text = get_settings_text(user_id)
    await bot.edit_message_text(
        text=settings_text,
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        reply_markup=get_settings_keyboard(settings),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "change_export_format")
async def change_export_format(callback_query: types.CallbackQuery):
    """Изменение формата экспорта"""
    from ..keyboards.settings_menu import get_export_format_keyboard

    bot = get_bot()
    await callback_query.answer()
    await bot.send_message(
        callback_query.message.chat.id,
        "📊 Выберите формат экспорта по умолчанию:",
        reply_markup=get_export_format_keyboard()
    )


@dp.callback_query(F.data.startswith("set_format_"))
async def set_export_format(callback_query: types.CallbackQuery):
    """Установка формата экспорта"""
    user_id = callback_query.from_user.id
    format_type = callback_query.data.split("_")[-1]
    
    if user_id not in user_settings:
        user_settings[user_id] = {}
    user_settings[user_id]['export_format'] = format_type
    
    await callback_query.answer(f"Формат экспорта: {format_type.upper()}")

    # Отправляем обновленные настройки в главное меню настроек
    bot = get_bot()
    settings = user_settings.get(user_id, {})
    settings_text = get_settings_text(user_id)
    await bot.send_message(
        chat_id=callback_query.message.chat.id,
        text=settings_text,
        reply_markup=get_settings_keyboard(settings),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "reset_settings")
async def reset_settings(callback_query: types.CallbackQuery):
    """Сброс настроек"""
    user_id = callback_query.from_user.id
    
    user_settings[user_id] = {
        'notifications': True,
        'auto_backup': True,
        'export_format': 'excel'
    }
    
    await callback_query.answer("Настройки сброшены")

    # Обновляем сообщение
    bot = get_bot()
    settings = user_settings.get(user_id, {})
    settings_text = get_settings_text(user_id)
    await bot.edit_message_text(
        text=settings_text,
        chat_id=callback_query.message.chat.id,
        message_id=callback_query.message.message_id,
        reply_markup=get_settings_keyboard(settings),
        parse_mode="HTML"
    )
