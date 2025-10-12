import os
import logging
from datetime import datetime, timedelta
from pathlib import Path


def ensure_directories():
    """Создание необходимых директорий"""
    directories = [
        'bot/data/reply', 
        'bot/data/exports', 
        'bot/data/backups', 
        'bot/data/logs', 
        'bot/data/temp'
    ]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)


def get_last_parsed_date() -> datetime.date:
    """Улучшенное определение последней даты парсинга"""
    base_file = "bot/data/all_users.xlsx"

    if not os.path.exists(base_file):
        logging.info("Database file doesn't exist")
        return None

    try:
        import pandas as pd
        df = pd.read_excel(base_file)

        if "Время сбора (UTC+1)" not in df.columns or df.empty:
            return None

        df["Время сбора (UTC+1)"] = pd.to_datetime(df["Время сбора (UTC+1)"], errors="coerce")
        df_clean = df.dropna(subset=["Время сбора (UTC+1)"])

        if df_clean.empty:
            return None

        last_datetime = df_clean["Время сбора (UTC+1)"].max()
        return last_datetime.date()

    except Exception as e:
        logging.error(f"Error determining last parsed date: {e}")
        return None


def cleanup_old_files(days_old=30):
    """Очистка старых файлов"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        deleted_count = 0
        total_size_freed = 0

        # Очищаем reply файлы
        if Path('bot/data/reply').exists():
            for file_path in Path('bot/data/reply').glob('*.xlsx'):
                file_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_time < cutoff_date:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    deleted_count += 1
                    total_size_freed += file_size

        # Очищаем export файлы (кроме последних)
        if Path('bot/data/exports').exists():
            export_files = sorted(Path('bot/data/exports').glob('*'), key=lambda x: x.stat().st_mtime, reverse=True)
            for file_path in export_files[5:]:  # Оставляем только 5 последних
                if file_path.is_file():
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    deleted_count += 1
                    total_size_freed += file_size

        return {
            "deleted_count": deleted_count,
            "size_freed": total_size_freed
        }

    except Exception as e:
        logging.error(f"Error cleaning up files: {e}")
        return {"error": str(e)}


def get_file_management_stats():
    """Получение статистики по файлам"""
    try:
        # Сканируем директории
        reply_files = list(Path('bot/data/reply').glob('*.xlsx')) if Path('bot/data/reply').exists() else []
        backup_files = list(Path('bot/data/backups').glob('*.xlsx')) if Path('bot/data/backups').exists() else []
        export_files = list(Path('bot/data/exports').glob('*')) if Path('bot/data/exports').exists() else []

        # Показываем размеры
        total_size = 0
        for file_list in [reply_files, backup_files, export_files]:
            for file_path in file_list:
                if file_path.exists():
                    total_size += file_path.stat().st_size

        return {
            "reply_files": len(reply_files),
            "backup_files": len(backup_files),
            "export_files": len(export_files),
            "total_size_mb": total_size / (1024 * 1024)
        }

    except Exception as e:
        logging.error(f"Error getting file management stats: {e}")
        return {"error": str(e)}


def list_reply_files(limit=10):
    """Список reply файлов"""
    try:
        reply_files = list(Path('bot/data/reply').glob('*.xlsx')) if Path('bot/data/reply').exists() else []

        if not reply_files:
            return []

        # Сортируем по дате создания
        reply_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        files_info = []
        for file_path in reply_files[:limit]:
            file_size = file_path.stat().st_size / 1024  # KB
            file_date = datetime.fromtimestamp(file_path.stat().st_mtime)
            files_info.append({
                "name": file_path.name,
                "size_kb": file_size,
                "date": file_date
            })

        return files_info

    except Exception as e:
        logging.error(f"Error listing reply files: {e}")
        return []

