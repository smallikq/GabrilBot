from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


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

