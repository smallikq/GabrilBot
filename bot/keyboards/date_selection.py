from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timezone, timedelta


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

