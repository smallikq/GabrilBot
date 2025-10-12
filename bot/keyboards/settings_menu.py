from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


def get_settings_keyboard(settings):
    """Клавиатура настроек"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text=f"🔔 {'Выключить' if settings.get('notifications', True) else 'Включить'} уведомления",
                callback_data="toggle_notifications"
            )
        ],
        [
            InlineKeyboardButton(
                text=f"💾 {'Выключить' if settings.get('auto_backup', True) else 'Включить'} автобэкапы",
                callback_data="toggle_backup"
            )
        ],
        [
            InlineKeyboardButton(text="📊 Изменить формат экспорта", callback_data="change_export_format")
        ],
        [
            InlineKeyboardButton(text="🗑 Сбросить настройки", callback_data="reset_settings")
        ]
    ])
    return keyboard


def get_file_management_keyboard():
    """Клавиатура управления файлами"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 Показать Reply", callback_data="list_reply_files"),
            InlineKeyboardButton(text="💾 Показать бэкапы", callback_data="list_backup_files")
        ],
        [
            InlineKeyboardButton(text="🗑 Очистить старые", callback_data="cleanup_old_files"),
            InlineKeyboardButton(text="📦 Создать архив", callback_data="create_archive")
        ]
    ])
    return keyboard


def get_combined_stats_keyboard():
    """Объединенная клавиатура статистики и аналитики"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Создать отчет", callback_data="create_analytics_report"),
            InlineKeyboardButton(text="📈 Графики", callback_data="create_charts")
        ],
        [
            InlineKeyboardButton(text="💾 Экспорт статистики", callback_data="export_stats"),
            InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_stats")
        ]
    ])
    return keyboard


def get_missed_days_keyboard():
    """Клавиатура для пропущенных дней"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Обработать все", callback_data="process_all_missed_enhanced")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_missed")]
    ])
    return keyboard


def get_date_range_confirmation_keyboard(start_date, end_date):
    """Клавиатура подтверждения диапазона дат"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, начать", callback_data=f"process_range_{start_date}_{end_date}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_range")
        ]
    ])
    return keyboard


def get_export_format_keyboard():
    """Клавиатура выбора формата экспорта"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📊 Excel", callback_data="set_format_excel"),
            InlineKeyboardButton(text="📝 CSV", callback_data="set_format_csv")
        ],
        [
            InlineKeyboardButton(text="📋 JSON", callback_data="set_format_json"),
            InlineKeyboardButton(text="📄 TXT", callback_data="set_format_txt")
        ]
    ])
    return keyboard


def get_advanced_search_keyboard():
    """Клавиатура расширенного поиска"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💎 Premium пользователи", callback_data="search_by_premium"),
            InlineKeyboardButton(text="✅ Verified", callback_data="search_by_verified")
        ],
        [
            InlineKeyboardButton(text="🎯 По группе", callback_data="search_by_group"),
            InlineKeyboardButton(text="📅 По дате", callback_data="search_by_date")
        ],
        [
            InlineKeyboardButton(text="🕐 Последние добавленные", callback_data="search_recent"),
            InlineKeyboardButton(text="📊 Статистика групп", callback_data="groups_stats")
        ]
    ])
    return keyboard


def get_export_filter_keyboard(filter_name: str):
    """Клавиатура экспорта отфильтрованных результатов"""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💾 Экспортировать", callback_data=f"export_filter_{filter_name}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_export")
        ]
    ])
    return keyboard