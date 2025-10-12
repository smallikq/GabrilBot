"""
Модуль для централизованной обработки ошибок и retry механизмов
"""

import logging
import asyncio
import functools
from typing import Callable, Any, Optional, Type, Tuple
from telethon.errors import FloodWaitError, AuthKeyError, ServerError, TimedOutError


class ErrorHandler:
    """Класс для обработки ошибок с retry механизмами"""
    
    @staticmethod
    def retry_on_error(
        max_retries: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        """
        Декоратор для повторной попытки выполнения функции при ошибке
        
        Args:
            max_retries: Максимальное количество попыток
            delay: Начальная задержка между попытками (секунды)
            backoff: Множитель увеличения задержки
            exceptions: Кортеж исключений, при которых делать retry
        
        Returns:
            Декорированная функция
        """
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs) -> Any:
                current_delay = delay
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return await func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        
                        if attempt < max_retries - 1:
                            logging.warning(
                                f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                                f"Retrying in {current_delay:.1f}s..."
                            )
                            await asyncio.sleep(current_delay)
                            current_delay *= backoff
                        else:
                            logging.error(
                                f"All {max_retries} attempts failed for {func.__name__}: {e}",
                                exc_info=True
                            )
                
                raise last_exception
            
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs) -> Any:
                import time
                current_delay = delay
                last_exception = None
                
                for attempt in range(max_retries):
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        
                        if attempt < max_retries - 1:
                            logging.warning(
                                f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                                f"Retrying in {current_delay:.1f}s..."
                            )
                            time.sleep(current_delay)
                            current_delay *= backoff
                        else:
                            logging.error(
                                f"All {max_retries} attempts failed for {func.__name__}: {e}",
                                exc_info=True
                            )
                
                raise last_exception
            
            # Определяем, асинхронная ли функция
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            else:
                return sync_wrapper
        
        return decorator
    
    @staticmethod
    async def handle_telegram_error(error: Exception, context: str = "") -> Optional[int]:
        """
        Обработка специфичных ошибок Telegram
        
        Args:
            error: Исключение для обработки
            context: Контекст, где произошла ошибка
        
        Returns:
            Рекомендуемое время ожидания в секундах или None
        """
        if isinstance(error, FloodWaitError):
            wait_seconds = error.seconds
            logging.warning(
                f"FloodWait error in {context}: waiting {wait_seconds} seconds"
            )
            return wait_seconds
        
        elif isinstance(error, AuthKeyError):
            logging.error(
                f"Authentication error in {context}: {error}. "
                "Session may need to be recreated."
            )
            return None
        
        elif isinstance(error, ServerError):
            logging.error(f"Telegram server error in {context}: {error}")
            return 30  # Подождать 30 секунд
        
        elif isinstance(error, TimedOutError):
            logging.warning(f"Timeout error in {context}: {error}")
            return 5  # Подождать 5 секунд
        
        else:
            logging.error(f"Unexpected error in {context}: {error}", exc_info=True)
            return None
    
    @staticmethod
    def safe_execute(func: Callable, default_return: Any = None, 
                     log_errors: bool = True) -> Callable:
        """
        Декоратор для безопасного выполнения функции с обработкой всех ошибок
        
        Args:
            func: Функция для выполнения
            default_return: Значение по умолчанию при ошибке
            log_errors: Логировать ли ошибки
        
        Returns:
            Декорированная функция
        """
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                if log_errors:
                    logging.error(
                        f"Error in {func.__name__}: {e}",
                        exc_info=True
                    )
                return default_return
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_errors:
                    logging.error(
                        f"Error in {func.__name__}: {e}",
                        exc_info=True
                    )
                return default_return
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    @staticmethod
    def log_performance(func: Callable) -> Callable:
        """
        Декоратор для логирования времени выполнения функции
        
        Args:
            func: Функция для мониторинга
        
        Returns:
            Декорированная функция
        """
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Any:
            import time
            start_time = time.time()
            
            try:
                result = await func(*args, **kwargs)
                elapsed = time.time() - start_time
                logging.info(f"{func.__name__} completed in {elapsed:.2f}s")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logging.error(f"{func.__name__} failed after {elapsed:.2f}s: {e}")
                raise
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> Any:
            import time
            start_time = time.time()
            
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start_time
                logging.info(f"{func.__name__} completed in {elapsed:.2f}s")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logging.error(f"{func.__name__} failed after {elapsed:.2f}s: {e}")
                raise
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper


# Глобальный обработчик для некритичных ошибок
def handle_non_critical_error(error: Exception, context: str = "") -> None:
    """
    Обработка некритичных ошибок с логированием
    
    Args:
        error: Исключение
        context: Контекст ошибки
    """
    logging.warning(f"Non-critical error in {context}: {error}")


# Глобальный обработчик для критичных ошибок
def handle_critical_error(error: Exception, context: str = "") -> None:
    """
    Обработка критичных ошибок с детальным логированием
    
    Args:
        error: Исключение
        context: Контекст ошибки
    """
    logging.critical(
        f"Critical error in {context}: {error}",
        exc_info=True,
        stack_info=True
    )

