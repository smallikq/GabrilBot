import logging
from aiogram import types, F
from pathlib import Path
from datetime import datetime

from ..utils.file_utils import get_file_management_stats, list_reply_files, cleanup_old_files
from ..keyboards.settings_menu import get_file_management_keyboard
from ..aiogram_loader import dp, bot


@dp.message(F.text == "🗂 Управление файлами")
async def file_management(message: types.Message):
    """Управление файлами"""
    try:
        stats = get_file_management_stats()
        
        if "error" in stats:
            await message.answer(f"❌ Ошибка: {stats['error']}")
            return

        management_text = f"🗂 <b>Управление файлами</b>\n\n"
        management_text += f"📋 <b>Reply файлы:</b> {stats['reply_files']}\n"
        management_text += f"💾 <b>Бэкапы:</b> {stats['backup_files']}\n"
        management_text += f"📊 <b>Экспорты:</b> {stats['export_files']}\n\n"
        management_text += f"💽 <b>Общий размер:</b> {stats['total_size_mb']:.1f} МБ"

        await message.answer(management_text, reply_markup=get_file_management_keyboard(), parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error in file management: {e}")
        await message.answer(f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "list_reply_files")
async def list_reply_files_callback(callback_query: types.CallbackQuery):
    """Список reply файлов"""
    await callback_query.answer()

    try:
        files_info = list_reply_files(limit=10)

        if not files_info:
            await bot.send_message(callback_query.message.chat.id, "📋 Reply файлы не найдены")
            return

        files_text = f"📋 <b>Reply файлы ({len(files_info)}):</b>\n\n"

        for i, file_info in enumerate(files_info, 1):
            files_text += f"{i}. <code>{file_info['name']}</code>\n"
            files_text += f"   📅 {file_info['date'].strftime('%d.%m.%Y %H:%M')} | 💽 {file_info['size_kb']:.1f} KB\n\n"

        await bot.send_message(callback_query.message.chat.id, files_text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error listing reply files: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "cleanup_old_files")
async def cleanup_old_files_callback(callback_query: types.CallbackQuery):
    """Очистка старых файлов"""
    await callback_query.answer("Очищаю старые файлы...")

    try:
        cleanup_result = cleanup_old_files(days_old=30)
        
        if "error" in cleanup_result:
            await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка очистки: {cleanup_result['error']}")
            return

        cleanup_text = f"🗑 <b>Очистка завершена</b>\n\n"
        cleanup_text += f"📁 Удалено файлов: {cleanup_result['deleted_count']}\n"
        cleanup_text += f"💽 Освобождено места: {cleanup_result['size_freed'] / (1024 * 1024):.1f} МБ"

        await bot.send_message(callback_query.message.chat.id, cleanup_text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error cleaning up files: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка очистки: {e}")


@dp.callback_query(F.data == "list_backup_files")
async def list_backup_files_callback(callback_query: types.CallbackQuery):
    """Список backup файлов"""
    await callback_query.answer()

    try:
        backup_dir = Path('bot/data/backups')
        
        if not backup_dir.exists():
            await bot.send_message(callback_query.message.chat.id, "📦 Backup файлы не найдены")
            return

        backup_files = sorted(backup_dir.glob('*.db'), key=lambda x: x.stat().st_mtime, reverse=True)[:10]
        
        if not backup_files:
            await bot.send_message(callback_query.message.chat.id, "📦 Backup файлы не найдены")
            return

        files_text = f"📦 <b>Backup файлы ({len(backup_files)}):</b>\n\n"

        for i, file_path in enumerate(backup_files, 1):
            stat = file_path.stat()
            size_mb = stat.st_size / (1024 * 1024)
            mod_time = datetime.fromtimestamp(stat.st_mtime)
            
            files_text += f"{i}. <code>{file_path.name}</code>\n"
            files_text += f"   📅 {mod_time.strftime('%d.%m.%Y %H:%M')} | 💽 {size_mb:.2f} МБ\n\n"

        await bot.send_message(callback_query.message.chat.id, files_text, parse_mode="HTML")

    except Exception as e:
        logging.error(f"Error listing backup files: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка: {e}")


@dp.callback_query(F.data == "create_archive")
async def create_archive_callback(callback_query: types.CallbackQuery):
    """Создание архива всех данных"""
    await callback_query.answer("Создаю архив...")

    try:
        from aiogram.types import FSInputFile
        import zipfile
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_path = f'bot/data/exports/archive_{timestamp}.zip'
        os.makedirs('bot/data/exports', exist_ok=True)

        status_msg = await bot.send_message(
            callback_query.message.chat.id,
            "📦 Создаю архив всех данных..."
        )

        with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Добавляем базу данных
            db_path = 'bot/data/all_users.db'
            if os.path.exists(db_path):
                zipf.write(db_path, 'all_users.db')
            
            # Добавляем reply файлы
            reply_dir = Path('bot/data/reply')
            if reply_dir.exists():
                for file in reply_dir.glob('*.txt'):
                    zipf.write(file, f'reply/{file.name}')
            
            # Добавляем экспорты
            exports_dir = Path('bot/data/exports')
            if exports_dir.exists():
                for file in list(exports_dir.glob('*.xlsx'))[:5]:  # Последние 5 экспортов
                    zipf.write(file, f'exports/{file.name}')
            
            # Добавляем логи
            logs_dir = Path('bot/data/logs')
            if logs_dir.exists():
                for file in logs_dir.glob('*.log'):
                    zipf.write(file, f'logs/{file.name}')

        archive_size = os.path.getsize(archive_path) / (1024 * 1024)

        await bot.edit_message_text(
            f"✅ Архив создан ({archive_size:.1f} МБ). Отправляю...",
            callback_query.message.chat.id,
            status_msg.message_id
        )

        await bot.send_document(
            callback_query.message.chat.id,
            FSInputFile(archive_path, filename=f'archive_{timestamp}.zip'),
            caption=f"📦 Полный архив данных ({archive_size:.1f} МБ)"
        )

        await bot.delete_message(callback_query.message.chat.id, status_msg.message_id)

        # Удаляем временный архив
        try:
            os.remove(archive_path)
        except:
            pass

    except Exception as e:
        logging.error(f"Error creating archive: {e}")
        await bot.send_message(callback_query.message.chat.id, f"❌ Ошибка создания архива: {e}")
