from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def get_enhanced_main_keyboard():
    """Расширенная главная клавиатура"""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚀 Запустить сбор данных"), KeyboardButton(text="📊 Статистика и Аналитика")],
            [KeyboardButton(text="📌 Парсить пропущенные дни"), KeyboardButton(text="📅 Диапазон дат")],
            [KeyboardButton(text="➕ Добавить ID вручную"), KeyboardButton(text="💾 Создать резервную копию")],
            [KeyboardButton(text="🗂 Управление файлами"), KeyboardButton(text="⚙️ Настройки")]
        ],
        resize_keyboard=True
    )
    return keyboard

