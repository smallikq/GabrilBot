"""
Модуль сбора метрик и мониторинга производительности
"""

import time
import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict, deque
from threading import Lock


class PerformanceMetrics:
    """Класс для сбора и анализа метрик производительности"""
    
    _instance = None
    _lock = Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(PerformanceMetrics, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._metrics: Dict[str, List[float]] = defaultdict(list)
            self._counters: Dict[str, int] = defaultdict(int)
            self._timers: Dict[str, float] = {}
            self._errors: deque = deque(maxlen=100)  # Хранить последние 100 ошибок
            self._start_time = time.time()
            self._initialized = True
    
    def record_execution_time(self, operation: str, duration: float) -> None:
        """
        Записать время выполнения операции
        
        Args:
            operation: Название операции
            duration: Длительность в секундах
        """
        with self._lock:
            self._metrics[f"{operation}_time"].append(duration)
            # Храним только последние 1000 записей
            if len(self._metrics[f"{operation}_time"]) > 1000:
                self._metrics[f"{operation}_time"] = self._metrics[f"{operation}_time"][-1000:]
    
    def increment_counter(self, counter_name: str, value: int = 1) -> None:
        """
        Увеличить счетчик
        
        Args:
            counter_name: Название счетчика
            value: Значение для увеличения
        """
        with self._lock:
            self._counters[counter_name] += value
    
    def record_error(self, error_type: str, error_message: str, context: str = "") -> None:
        """
        Записать ошибку
        
        Args:
            error_type: Тип ошибки
            error_message: Сообщение об ошибке
            context: Контекст ошибки
        """
        with self._lock:
            error_record = {
                'timestamp': datetime.now().isoformat(),
                'type': error_type,
                'message': error_message,
                'context': context
            }
            self._errors.append(error_record)
    
    def start_timer(self, timer_name: str) -> None:
        """
        Запустить таймер
        
        Args:
            timer_name: Название таймера
        """
        self._timers[timer_name] = time.time()
    
    def stop_timer(self, timer_name: str) -> Optional[float]:
        """
        Остановить таймер и записать результат
        
        Args:
            timer_name: Название таймера
        
        Returns:
            Длительность в секундах или None
        """
        if timer_name in self._timers:
            duration = time.time() - self._timers[timer_name]
            del self._timers[timer_name]
            self.record_execution_time(timer_name, duration)
            return duration
        return None
    
    def get_average_time(self, operation: str) -> Optional[float]:
        """
        Получить среднее время выполнения операции
        
        Args:
            operation: Название операции
        
        Returns:
            Среднее время в секундах или None
        """
        times = self._metrics.get(f"{operation}_time", [])
        if times:
            return sum(times) / len(times)
        return None
    
    def get_percentile(self, operation: str, percentile: float = 95) -> Optional[float]:
        """
        Получить перцентиль времени выполнения
        
        Args:
            operation: Название операции
            percentile: Перцентиль (0-100)
        
        Returns:
            Значение перцентиля или None
        """
        times = self._metrics.get(f"{operation}_time", [])
        if times:
            sorted_times = sorted(times)
            index = int(len(sorted_times) * (percentile / 100))
            return sorted_times[min(index, len(sorted_times) - 1)]
        return None
    
    def get_counter(self, counter_name: str) -> int:
        """
        Получить значение счетчика
        
        Args:
            counter_name: Название счетчика
        
        Returns:
            Значение счетчика
        """
        return self._counters.get(counter_name, 0)
    
    def get_uptime(self) -> float:
        """
        Получить время работы в секундах
        
        Returns:
            Время работы
        """
        return time.time() - self._start_time
    
    def get_recent_errors(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Получить последние ошибки
        
        Args:
            limit: Максимальное количество ошибок
        
        Returns:
            Список словарей с информацией об ошибках
        """
        with self._lock:
            return list(self._errors)[-limit:]
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """
        Получить сводку всех метрик
        
        Returns:
            Словарь с метриками
        """
        summary = {
            'uptime_seconds': self.get_uptime(),
            'counters': dict(self._counters),
            'operations': {}
        }
        
        # Добавляем статистику по операциям
        for key in self._metrics:
            if key.endswith('_time'):
                operation = key[:-5]
                times = self._metrics[key]
                if times:
                    summary['operations'][operation] = {
                        'count': len(times),
                        'avg_time': sum(times) / len(times),
                        'min_time': min(times),
                        'max_time': max(times),
                        'p95_time': self.get_percentile(operation, 95),
                        'p99_time': self.get_percentile(operation, 99)
                    }
        
        # Добавляем статистику ошибок
        summary['errors'] = {
            'total_recorded': len(self._errors),
            'recent': self.get_recent_errors(5)
        }
        
        return summary
    
    def format_metrics_report(self) -> str:
        """
        Форматировать отчет с метриками
        
        Returns:
            Текстовый отчет
        """
        summary = self.get_metrics_summary()
        
        lines = []
        lines.append("=" * 70)
        lines.append("PERFORMANCE METRICS REPORT")
        lines.append("=" * 70)
        
        # Uptime
        uptime = summary['uptime_seconds']
        hours, remainder = divmod(uptime, 3600)
        minutes, seconds = divmod(remainder, 60)
        lines.append(f"\nUptime: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        
        # Counters
        if summary['counters']:
            lines.append("\nCOUNTERS:")
            lines.append("-" * 70)
            for counter, value in sorted(summary['counters'].items()):
                lines.append(f"  {counter:<40} {value:>20,}")
        
        # Operations
        if summary['operations']:
            lines.append("\nOPERATIONS TIMING:")
            lines.append("-" * 70)
            lines.append(f"{'Operation':<30} {'Count':>10} {'Avg':>10} {'P95':>10} {'Max':>10}")
            lines.append("-" * 70)
            
            for op, stats in sorted(summary['operations'].items()):
                lines.append(
                    f"{op:<30} "
                    f"{stats['count']:>10,} "
                    f"{stats['avg_time']:>9.2f}s "
                    f"{stats.get('p95_time', 0):>9.2f}s "
                    f"{stats['max_time']:>9.2f}s"
                )
        
        # Errors
        if summary['errors']['total_recorded'] > 0:
            lines.append(f"\nERRORS: {summary['errors']['total_recorded']} recorded")
            if summary['errors']['recent']:
                lines.append("\nRecent errors:")
                for error in summary['errors']['recent']:
                    lines.append(f"  [{error['timestamp']}] {error['type']}: {error['message']}")
        
        lines.append("\n" + "=" * 70)
        
        return "\n".join(lines)
    
    def reset_metrics(self) -> None:
        """Сброс всех метрик"""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._errors.clear()
            self._start_time = time.time()
            logging.info("Metrics reset")


# Декоратор для автоматического сбора метрик
def track_performance(operation_name: Optional[str] = None):
    """
    Декоратор для отслеживания производительности функций
    
    Args:
        operation_name: Название операции (по умолчанию - имя функции)
    
    Returns:
        Декорированная функция
    """
    def decorator(func):
        import functools
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            metrics = PerformanceMetrics()
            op_name = operation_name or func.__name__
            
            metrics.start_timer(op_name)
            metrics.increment_counter(f"{op_name}_calls")
            
            try:
                result = await func(*args, **kwargs)
                metrics.increment_counter(f"{op_name}_success")
                return result
            except Exception as e:
                metrics.increment_counter(f"{op_name}_errors")
                metrics.record_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    context=op_name
                )
                raise
            finally:
                duration = metrics.stop_timer(op_name)
                if duration:
                    logging.debug(f"{op_name} completed in {duration:.3f}s")
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            metrics = PerformanceMetrics()
            op_name = operation_name or func.__name__
            
            metrics.start_timer(op_name)
            metrics.increment_counter(f"{op_name}_calls")
            
            try:
                result = func(*args, **kwargs)
                metrics.increment_counter(f"{op_name}_success")
                return result
            except Exception as e:
                metrics.increment_counter(f"{op_name}_errors")
                metrics.record_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    context=op_name
                )
                raise
            finally:
                duration = metrics.stop_timer(op_name)
                if duration:
                    logging.debug(f"{op_name} completed in {duration:.3f}s")
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Глобальный экземпляр метрик
metrics = PerformanceMetrics()

