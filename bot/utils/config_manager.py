"""
Модуль управления конфигурацией с валидацией и безопасностью
"""

import os
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path


class ConfigManager:
    """Менеджер конфигурации с валидацией"""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._config = {}
            self.load_config()
    
    def load_config(self) -> bool:
        """
        Загрузка конфигурации из файла с валидацией
        
        Returns:
            True если загрузка успешна, False иначе
        """
        try:
            # Импортируем конфигурацию
            from ..data import parser_cfg
            
            self._config = {
                'BOT_TOKEN': getattr(parser_cfg, 'BOT_TOKEN', None),
                'accounts': getattr(parser_cfg, 'accounts', [])
            }
            
            # Валидируем конфигурацию
            from .validators import DataValidator
            is_valid, errors = DataValidator.validate_config(self._config)
            
            if not is_valid:
                for error in errors:
                    logging.error(f"Configuration error: {error}")
                return False
            
            logging.info("Configuration loaded and validated successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}", exc_info=True)
            return False
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Получение значения из конфигурации
        
        Args:
            key: Ключ конфигурации
            default: Значение по умолчанию
        
        Returns:
            Значение конфигурации или default
        """
        return self._config.get(key, default)
    
    def get_bot_token(self) -> Optional[str]:
        """Получение токена бота"""
        return self._config.get('BOT_TOKEN')
    
    def get_accounts(self) -> List[Dict[str, str]]:
        """Получение списка аккаунтов"""
        return self._config.get('accounts', [])
    
    def get_account_count(self) -> int:
        """Получение количества настроенных аккаунтов"""
        return len(self.get_accounts())
    
    @staticmethod
    def get_db_path() -> str:
        """Получение пути к базе данных"""
        return os.path.join('bot', 'data', 'all_users.db')
    
    @staticmethod
    def get_exports_dir() -> str:
        """Получение директории экспортов"""
        path = os.path.join('bot', 'data', 'exports')
        os.makedirs(path, exist_ok=True)
        return path
    
    @staticmethod
    def get_backups_dir() -> str:
        """Получение директории бэкапов"""
        path = os.path.join('bot', 'data', 'backups')
        os.makedirs(path, exist_ok=True)
        return path
    
    @staticmethod
    def get_logs_dir() -> str:
        """Получение директории логов"""
        path = os.path.join('bot', 'data', 'logs')
        os.makedirs(path, exist_ok=True)
        return path
    
    @staticmethod
    def get_temp_dir() -> str:
        """Получение директории временных файлов"""
        path = os.path.join('bot', 'data', 'temp')
        os.makedirs(path, exist_ok=True)
        return path
    
    def validate_environment(self) -> tuple[bool, List[str]]:
        """
        Валидация окружения и необходимых директорий
        
        Returns:
            Кортеж (валидность, список проблем)
        """
        issues = []
        
        # Проверка наличия конфигурации
        config_path = Path('bot/data/parser_cfg.py')
        if not config_path.exists():
            issues.append("Configuration file not found: bot/data/parser_cfg.py")
        
        # Проверка директорий
        required_dirs = [
            'bot/data',
            'bot/data/logs',
            'bot/data/backups',
            'bot/data/exports',
            'bot/data/temp',
            'bot/data/reply'
        ]
        
        for dir_path in required_dirs:
            if not os.path.exists(dir_path):
                try:
                    os.makedirs(dir_path, exist_ok=True)
                    logging.info(f"Created directory: {dir_path}")
                except Exception as e:
                    issues.append(f"Cannot create directory {dir_path}: {e}")
        
        # Проверка прав на запись
        test_dirs = ['bot/data', 'bot/data/exports', 'bot/data/logs']
        for dir_path in test_dirs:
            test_file = os.path.join(dir_path, '.write_test')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
            except Exception as e:
                issues.append(f"No write permission for {dir_path}: {e}")
        
        # Проверка свободного места (минимум 100MB)
        try:
            import shutil
            stat = shutil.disk_usage('.')
            free_mb = stat.free / (1024 * 1024)
            if free_mb < 100:
                issues.append(f"Low disk space: {free_mb:.1f}MB free (minimum 100MB required)")
        except Exception as e:
            logging.warning(f"Cannot check disk space: {e}")
        
        return len(issues) == 0, issues
    
    def get_config_summary(self) -> str:
        """
        Получение сводки конфигурации для логов
        
        Returns:
            Строка со сводкой
        """
        summary = []
        summary.append("=" * 60)
        summary.append("CONFIGURATION SUMMARY")
        summary.append("=" * 60)
        
        # Маскируем токен
        token = self.get_bot_token()
        if token:
            masked_token = f"{token[:10]}...{token[-5:]}"
            summary.append(f"Bot Token: {masked_token}")
        else:
            summary.append("Bot Token: NOT CONFIGURED")
        
        # Информация об аккаунтах
        accounts = self.get_accounts()
        summary.append(f"\nConfigured Accounts: {len(accounts)}")
        
        for idx, account in enumerate(accounts, 1):
            phone = account.get('phone_number', 'Unknown')
            # Маскируем номер телефона
            if phone and len(phone) > 4:
                masked_phone = f"{phone[:3]}***{phone[-2:]}"
            else:
                masked_phone = "***"
            summary.append(f"  {idx}. {masked_phone}")
        
        # Пути
        summary.append(f"\nDatabase Path: {self.get_db_path()}")
        summary.append(f"Exports Directory: {self.get_exports_dir()}")
        summary.append(f"Backups Directory: {self.get_backups_dir()}")
        summary.append(f"Logs Directory: {self.get_logs_dir()}")
        
        summary.append("=" * 60)
        
        return "\n".join(summary)


# Создаем глобальный экземпляр
config = ConfigManager()

