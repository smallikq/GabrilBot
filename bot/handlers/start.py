from aiogram import types
from aiogram.filters import Command

from ..keyboards.main_menu import get_enhanced_main_keyboard
from ..aiogram_loader import dp, user_settings


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

