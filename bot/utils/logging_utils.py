"""
Модуль для расширенного логирования с ротацией и форматированием
"""

import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime
from typing import Optional


class ColoredFormatter(logging.Formatter):
    """Форматтер с цветным выводом для консоли"""
    
    # ANSI цветовые коды
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        # Добавляем цвет к уровню логирования
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.COLORS['RESET']}"
        return super().format(record)


def setup_logging(
    level: int = logging.INFO,
    log_dir: str = 'bot/data/logs',
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    colored_console: bool = True
) -> None:
    """
    Расширенная настройка логирования с ротацией
    
    Args:
        level: Уровень логирования
        log_dir: Директория для логов
        max_bytes: Максимальный размер файла лога перед ротацией
        backup_count: Количество резервных копий логов
        colored_console: Использовать цветной вывод в консоль
    """
    # Создаем директорию для логов
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Очищаем существующие обработчики
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    
    # Формат для файла (детальный)
    file_format = logging.Formatter(
        '%(asctime)s | %(name)-25s | %(levelname)-8s | %(funcName)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Формат для консоли (компактный)
    if colored_console and sys.platform != 'win32':
        console_format = ColoredFormatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
    else:
        console_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%H:%M:%S'
        )
    
    # Обработчик для основного лога с ротацией
    main_log_file = log_path / 'bot_logs.log'
    file_handler = RotatingFileHandler(
        main_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(file_format)
    root_logger.addHandler(file_handler)
    
    # Обработчик для ошибок (отдельный файл)
    error_log_file = log_path / 'errors.log'
    error_handler = RotatingFileHandler(
        error_log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_format)
    root_logger.addHandler(error_handler)
    
    # Обработчик для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(console_format)
    root_logger.addHandler(console_handler)
    
    # Создаем логгер производительности (опционально)
    perf_logger = logging.getLogger('performance')
    perf_log_file = log_path / 'performance.log'
    perf_handler = TimedRotatingFileHandler(
        perf_log_file,
        when='midnight',
        interval=1,
        backupCount=7,
        encoding='utf-8'
    )
    perf_handler.setFormatter(file_format)
    perf_logger.addHandler(perf_handler)
    perf_logger.setLevel(logging.INFO)
    perf_logger.propagate = False
    
    logging.info("Logging system initialized")
    logging.info(f"Log directory: {log_path.absolute()}")


def log_banner(text: str, symbol: str = "=", level: int = logging.INFO) -> None:
    """
    Улучшенное логирование с красивым баннером
    
    Args:
        text: Текст для отображения
        symbol: Символ для границы
        level: Уровень логирования
    """
    border = symbol * (len(text) + 4)
    logger = logging.getLogger()
    
    logger.log(level, "")
    logger.log(level, border)
    logger.log(level, f"{symbol} {text} {symbol}")
    logger.log(level, border)
    logger.log(level, "")


def log_section(title: str, content: dict, level: int = logging.INFO) -> None:
    """
    Логирование секции с данными
    
    Args:
        title: Заголовок секции
        content: Словарь с данными для отображения
        level: Уровень логирования
    """
    logger = logging.getLogger()
    
    logger.log(level, f"\n{'='*60}")
    logger.log(level, f"{title}")
    logger.log(level, f"{'-'*60}")
    
    for key, value in content.items():
        logger.log(level, f"  {key:<30} {value}")
    
    logger.log(level, f"{'='*60}\n")


def log_performance(operation: str, duration: float, details: Optional[dict] = None) -> None:
    """
    Логирование производительности операции
    
    Args:
        operation: Название операции
        duration: Длительность в секундах
        details: Дополнительные детали
    """
    perf_logger = logging.getLogger('performance')
    
    message = f"{operation} completed in {duration:.3f}s"
    
    if details:
        details_str = " | ".join([f"{k}={v}" for k, v in details.items()])
        message += f" | {details_str}"
    
    perf_logger.info(message)


def cleanup_old_logs(log_dir: str = 'bot/data/logs', days: int = 30) -> int:
    """
    Очистка старых логов
    
    Args:
        log_dir: Директория с логами
        days: Количество дней для хранения
    
    Returns:
        Количество удаленных файлов
    """
    from datetime import timedelta
    
    log_path = Path(log_dir)
    if not log_path.exists():
        return 0
    
    cutoff_date = datetime.now() - timedelta(days=days)
    deleted_count = 0
    
    for log_file in log_path.glob('*.log*'):
        if log_file.is_file():
            file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            if file_time < cutoff_date:
                try:
                    log_file.unlink()
                    deleted_count += 1
                    logging.info(f"Deleted old log: {log_file.name}")
                except Exception as e:
                    logging.error(f"Failed to delete {log_file.name}: {e}")
    
    return deleted_count

