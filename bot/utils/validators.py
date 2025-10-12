"""
Модуль для валидации данных и защиты от некорректного ввода
"""

import re
import logging
from typing import Optional, Any, List, Dict
from datetime import datetime


class DataValidator:
    """Класс для валидации различных типов данных"""
    
    # Регулярные выражения для валидации
    USERNAME_PATTERN = re.compile(r'^@?[a-zA-Z0-9_]{5,32}$')
    PHONE_PATTERN = re.compile(r'^\+?[1-9]\d{1,14}$')
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    @staticmethod
    def validate_user_id(user_id: Any) -> bool:
        """
        Валидация Telegram User ID
        
        Args:
            user_id: ID пользователя для проверки
        
        Returns:
            True если ID валиден, False иначе
        """
        try:
            uid = int(user_id)
            # Telegram user ID должен быть положительным и в разумных пределах
            return 0 < uid < 10**15
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_username(username: Optional[str]) -> bool:
        """
        Валидация username
        
        Args:
            username: Username для проверки
        
        Returns:
            True если username валиден или пустой, False иначе
        """
        if not username:
            return True  # Username может быть пустым
        
        # Убираем @ если есть
        clean_username = username.lstrip('@')
        return bool(DataValidator.USERNAME_PATTERN.match(clean_username))
    
    @staticmethod
    def validate_phone(phone: Optional[str]) -> bool:
        """
        Валидация номера телефона
        
        Args:
            phone: Номер телефона для проверки
        
        Returns:
            True если номер валиден или пустой, False иначе
        """
        if not phone:
            return True  # Телефон может быть пустым
        
        # Убираем все пробелы и дефисы
        clean_phone = re.sub(r'[\s\-()]', '', phone)
        return bool(DataValidator.PHONE_PATTERN.match(clean_phone))
    
    @staticmethod
    def validate_date(date_str: str) -> bool:
        """
        Валидация даты в формате YYYY-MM-DD
        
        Args:
            date_str: Строка с датой
        
        Returns:
            True если дата валидна, False иначе
        """
        if not DataValidator.DATE_PATTERN.match(date_str):
            return False
        
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_search_term(search_term: str, max_length: int = 100) -> bool:
        """
        Валидация поискового запроса
        
        Args:
            search_term: Поисковый запрос
            max_length: Максимальная длина запроса
        
        Returns:
            True если запрос валиден, False иначе
        """
        if not search_term or not search_term.strip():
            return False
        
        if len(search_term) > max_length:
            logging.warning(f"Search term too long: {len(search_term)} chars")
            return False
        
        # Проверка на потенциально опасные символы для SQL
        dangerous_chars = [';', '--', '/*', '*/', 'xp_', 'sp_', 'EXEC', 'DROP', 'DELETE', 'INSERT', 'UPDATE']
        search_upper = search_term.upper()
        
        for dangerous in dangerous_chars:
            if dangerous.upper() in search_upper:
                logging.warning(f"Dangerous pattern detected in search: {dangerous}")
                return False
        
        return True
    
    @staticmethod
    def sanitize_string(text: Optional[str], max_length: int = 500) -> str:
        """
        Очистка и санитизация строки
        
        Args:
            text: Текст для очистки
            max_length: Максимальная длина
        
        Returns:
            Очищенная строка
        """
        if not text:
            return ""
        
        # Убираем опасные символы
        sanitized = str(text)[:max_length]
        
        # Убираем управляющие символы
        sanitized = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', sanitized)
        
        return sanitized.strip()
    
    @staticmethod
    def validate_user_data(user_data: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Комплексная валидация данных пользователя
        
        Args:
            user_data: Словарь с данными пользователя
        
        Returns:
            Кортеж (валидность, список ошибок)
        """
        errors = []
        
        # Проверка обязательных полей
        if 'user_id' not in user_data:
            errors.append("Missing required field: user_id")
        elif not DataValidator.validate_user_id(user_data['user_id']):
            errors.append("Invalid user_id")
        
        # Проверка опциональных полей
        if 'username' in user_data and not DataValidator.validate_username(user_data['username']):
            errors.append("Invalid username format")
        
        if 'phone' in user_data and not DataValidator.validate_phone(user_data['phone']):
            errors.append("Invalid phone format")
        
        # Проверка типов данных
        if 'is_premium' in user_data and not isinstance(user_data['is_premium'], (bool, int)):
            errors.append("Invalid is_premium type")
        
        if 'is_verified' in user_data and not isinstance(user_data['is_verified'], (bool, int)):
            errors.append("Invalid is_verified type")
        
        # Валидация длины строковых полей
        string_fields = {
            'first_name': 100,
            'last_name': 100,
            'source_group': 200
        }
        
        for field, max_len in string_fields.items():
            if field in user_data and user_data[field]:
                if len(str(user_data[field])) > max_len:
                    errors.append(f"{field} exceeds maximum length of {max_len}")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def validate_config(config: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Валидация конфигурации бота
        
        Args:
            config: Словарь конфигурации
        
        Returns:
            Кортеж (валидность, список ошибок)
        """
        errors = []
        
        # Проверка BOT_TOKEN
        if 'BOT_TOKEN' not in config or not config['BOT_TOKEN']:
            errors.append("Missing or empty BOT_TOKEN")
        elif not re.match(r'^\d+:[A-Za-z0-9_-]{35}$', config['BOT_TOKEN']):
            errors.append("Invalid BOT_TOKEN format")
        
        # Проверка accounts
        if 'accounts' not in config or not config['accounts']:
            errors.append("No accounts configured")
        elif not isinstance(config['accounts'], list):
            errors.append("Accounts must be a list")
        else:
            for idx, account in enumerate(config['accounts']):
                if not isinstance(account, dict):
                    errors.append(f"Account {idx} is not a dictionary")
                    continue
                
                # Проверка обязательных полей аккаунта
                required_fields = ['phone_number', 'api_id', 'api_hash']
                for field in required_fields:
                    if field not in account or not account[field]:
                        errors.append(f"Account {idx}: missing {field}")
                
                # Валидация phone_number
                if 'phone_number' in account:
                    if not DataValidator.validate_phone(account['phone_number']):
                        errors.append(f"Account {idx}: invalid phone_number format")
                
                # Валидация api_id
                if 'api_id' in account:
                    try:
                        int(account['api_id'])
                    except (ValueError, TypeError):
                        errors.append(f"Account {idx}: api_id must be numeric")
        
        return len(errors) == 0, errors


class SQLInjectionPreventer:
    """Класс для предотвращения SQL инъекций"""
    
    @staticmethod
    def is_safe_query(query: str) -> bool:
        """
        Проверка безопасности SQL запроса
        
        Args:
            query: SQL запрос для проверки
        
        Returns:
            True если запрос безопасен, False иначе
        """
        # Список опасных SQL команд
        dangerous_patterns = [
            r'\bDROP\b',
            r'\bDELETE\b(?!\s+FROM\s+users\s+WHERE)',  # DELETE разрешен только с WHERE
            r'\bTRUNCATE\b',
            r'\bEXEC\b',
            r'\bEXECUTE\b',
            r'\bxp_\w+',
            r'\bsp_\w+',
            r';.*DROP',
            r';.*DELETE',
            r'--',
            r'/\*.*\*/',
            r'\bUNION\b.*\bSELECT\b',
        ]
        
        query_upper = query.upper()
        
        for pattern in dangerous_patterns:
            if re.search(pattern, query_upper, re.IGNORECASE):
                logging.critical(f"Dangerous SQL pattern detected: {pattern}")
                return False
        
        return True
    
    @staticmethod
    def escape_like_pattern(pattern: str) -> str:
        """
        Экранирование спецсимволов для LIKE запросов
        
        Args:
            pattern: Паттерн для экранирования
        
        Returns:
            Экранированный паттерн
        """
        # Экранируем специальные символы SQL LIKE
        escape_chars = ['%', '_', '[', ']']
        result = pattern
        
        for char in escape_chars:
            result = result.replace(char, f'\\{char}')
        
        return result

