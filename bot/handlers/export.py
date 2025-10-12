import os
import logging
from aiogram import types, F
from aiogram.types import FSInputFile
from datetime import datetime
import pandas as pd

from ..utils.export_manager import ExportManager
from ..keyboards.export_menu import get_export_keyboard
from ..aiogram_loader import dp, bot


@dp.message(F.text == "📤 Экспорт данных")
async def export_menu(message: types.Message):
    """Меню экспорта данных (из SQL)"""
    try:
        from ..utils.database import DatabaseManager
        
        # Получаем статистику из SQL базы
        stats = DatabaseManager.get_database_stats()
        total_users = stats.get('total_users', 0)
        
        if total_users == 0:
            await message.answer("❌ Нет данных для экспорта")
            return

        export_text = f"📤 <b>Экспорт данных</b>\n\n"
        export_text += f"📊 В базе: {total_users:,} пользователей\n"
        export_text += f"📁 Доступные форматы:\n\n"
        export_text += f"• <b>Excel</b> - полная совместимость\n"
        export_text += f"• <b>CSV</b> - универсальный формат\n"
        export_text += f"• <b>JSON</b> - для разработчиков\n"
        export_text += f"• <b>Отчет</b> - детальная аналитика\n"
        export_text += f"• <b>Архив</b> - все форматы сразу"

        await message.answer(export_text, reply_markup=get_export_keyboard(), parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error in export menu: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@dp.callback_query(F.data.startswith('export_'))
async def handle_export(callback_query: types.CallbackQuery):
    """Обработка экспорта данных (из SQL)"""
    await callback_query.answer("Подготавливаю экспорт...")

    export_type = callback_query.data.split('_')[1]

    try:
        from ..utils.database import DatabaseManager
        
        # Получаем данные из SQL базы
        df = DatabaseManager.get_all_users()
        
        if df.empty:
            await bot.send_message(callback_query.message.chat.id, "❌ Нет данных для экспорта")
            return
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        status_msg = await bot.send_message(
            callback_query.message.chat.id,
            f"⏳ Создаю экспорт в формате {export_type.upper()}..."
        )

        if export_type == 'excel':
            file_path = f'bot/data/exports/export_{timestamp}.xlsx'
            df.to_excel(file_path, index=False)
            caption = f"📊 Экспорт Excel ({len(df)} пользователей)"

        elif export_type == 'csv':
            file_path = await ExportManager.export_to_csv(df, f'export_{timestamp}')
            caption = f"📝 Экспорт CSV ({len(df)} пользователей)"

        elif export_type == 'json':
            file_path = await ExportManager.export_to_json(df, f'export_{timestamp}')
            caption = f"📋 Экспорт JSON ({len(df)} пользователей)"

        elif export_type == 'report':
            file_path = await ExportManager.create_report(df, f'export_{timestamp}')
            caption = f"📑 Детальный отчет ({len(df)} пользователей)"

        elif export_type == 'all':
            file_path = await ExportManager.create_complete_export(df, f'export_{timestamp}')
            caption = f"📦 Полный экспорт - все форматы ({len(df)} пользователей)"

        await bot.edit_message_text(
            f"✅ Экспорт готов! Отправляю файл...",
            callback_query.message.chat.id,
            status_msg.message_id
        )

        # Отправляем файл
        if file_path and os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                await bot.send_document(
                    callback_query.message.chat.id,
                    FSInputFile(file_path, filename=os.path.basename(file_path)),
                    caption=caption
                )

            await bot.delete_message(callback_query.message.chat.id, status_msg.message_id)
        else:
            await bot.edit_message_text(
                "❌ Ошибка при создании экспорта",
                callback_query.message.chat.id,
                status_msg.message_id
            )

    except Exception as e:
        logging.error(f"Export error: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка экспорта: {e}")

