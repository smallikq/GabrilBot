"""
Главный модуль бота с улучшенной инициализацией и мониторингом
"""

import asyncio
import logging
import os
import sys

# Добавляем путь к модулям
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from .aiogram_loader import initialize_bot, dp
from .utils.logging_utils import setup_logging, log_banner, log_section
from .utils.database import DatabaseManager
from .utils.file_utils import ensure_directories
from .utils.config_manager import config
from .utils.metrics import metrics
from .utils.error_handler import ErrorHandler

# Импортируем все обработчики
from .handlers import (
    start, stats, export, parser, search, settings, file_manager, 
    missed_days
)


async def initialize_system() -> bool:
    """
    Инициализация системы с валидацией
    
    Returns:
        True если инициализация успешна, False иначе
    """
    try:
        # Валидация окружения
        is_valid, issues = config.validate_environment()
        if not is_valid:
            logging.error("Environment validation failed:")
            for issue in issues:
                logging.error(f"  - {issue}")
            return False
        
        # Создаем необходимые директории
        ensure_directories()
        
        # Инициализация базы данных
        DatabaseManager.init_database()
        logging.info("Database initialized successfully")
        
        # Обновляем существующую базу данных (добавляем @ к username)
        try:
            updated_count = DatabaseManager.update_existing_database_usernames()
            if updated_count > 0:
                logging.info(f"Updated {updated_count} usernames with @ prefix")
        except Exception as e:
            logging.warning(f"Failed to update usernames: {e}")
        
        return True
        
    except Exception as e:
        logging.error(f"System initialization failed: {e}", exc_info=True)
        return False


async def main():
    """Улучшенная основная функция с метриками и обработкой ошибок"""
    
    # Настройка расширенного логирования
    setup_logging(level=logging.INFO)
    
    log_banner("Starting Enhanced Telegram Parser Bot v2.2.0")
    
    try:
        # Загрузка и валидация конфигурации
        if not config.load_config():
            logging.error("Failed to load configuration")
            return
        
        # Логируем сводку конфигурации
        print(config.get_config_summary())
        
        # Проверяем токен и аккаунты
        bot_token = config.get_bot_token()
        accounts = config.get_accounts()
        
        if not bot_token:
            logging.error("BOT_TOKEN not found in configuration")
            return
        
        if not accounts:
            logging.error("No accounts configured")
            return
        
        # Инициализация системы
        log_banner("Initializing system components", symbol="-")
        if not await initialize_system():
            logging.error("System initialization failed")
            return
        
        # Инициализация бота
        bot = initialize_bot(bot_token)
        logging.info(f"Bot initialized successfully")
        
        # Логируем включенные возможности
        features = {
            "Database": "SQLite with connection pooling",
            "Accounts": f"{len(accounts)} configured",
            "Export Formats": "Excel, CSV, JSON, Markdown, HTML, TXT",
            "Error Handling": "Retry with exponential backoff",
            "Metrics": "Performance monitoring enabled",
            "Validation": "Data validation and SQL injection prevention",
            "Logging": "Rotating logs with error tracking"
        }
        
        log_section("ENABLED FEATURES", features)
        
        # Получаем начальную статистику БД
        db_stats = DatabaseManager.get_database_stats()
        if 'error' not in db_stats:
            stats_info = {
                "Total Users": f"{db_stats.get('total_users', 0):,}",
                "With Username": f"{db_stats.get('with_username', 0):,}",
                "Premium Users": f"{db_stats.get('premium_users', 0):,}",
                "Unique Users": f"{db_stats.get('unique_users', 0):,}"
            }
            log_section("DATABASE STATISTICS", stats_info)
        
        # Запуск бота
        log_banner("Starting bot polling", symbol="-")
        metrics.increment_counter("bot_starts")
        
        await dp.start_polling(
            bot,
            skip_updates=True,
            allowed_updates=['message', 'callback_query']
        )
        
    except KeyboardInterrupt:
        logging.info("Bot stopped by user (Ctrl+C)")
        metrics.increment_counter("manual_stops")
        
    except Exception as e:
        logging.error(f"Critical error in main: {e}", exc_info=True)
        metrics.record_error(
            error_type=type(e).__name__,
            error_message=str(e),
            context="main"
        )
        metrics.increment_counter("critical_errors")
        
    finally:
        try:
            # Логируем финальные метрики
            log_banner("Shutdown - Performance Report", symbol="=")
            print(metrics.format_metrics_report())
            
            # Закрываем соединение с ботом
            if 'bot' in locals():
                await bot.session.close()
                logging.info("Bot session closed successfully")
            
            # Очищаем пул соединений БД
            DatabaseManager.cleanup_pool()
            logging.info("Database connection pool cleaned up")
            
            log_banner("Bot shutdown completed", symbol="=")
            
        except Exception as e:
            logging.error(f"Error during shutdown: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Critical error: {e}")
        raise

