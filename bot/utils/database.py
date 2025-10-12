import os
import asyncio
import shutil
import logging
import sqlite3
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Union
from contextlib import contextmanager
from threading import Lock
import pandas as pd


class DatabaseManager:
    """Менеджер для работы с базой данных SQL с поддержкой connection pooling"""
    
    DB_PATH = 'bot/data/all_users.db'
    _connection_pool: List[sqlite3.Connection] = []
    _pool_size: int = 5
    _lock = Lock()
    
    @classmethod
    def _get_pool_connection(cls) -> sqlite3.Connection:
        """Получение соединения из пула"""
        with cls._lock:
            if cls._connection_pool:
                return cls._connection_pool.pop()
            else:
                return cls._create_connection()
    
    @classmethod
    def _return_to_pool(cls, conn: sqlite3.Connection) -> None:
        """Возврат соединения в пул"""
        with cls._lock:
            if len(cls._connection_pool) < cls._pool_size:
                cls._connection_pool.append(conn)
            else:
                conn.close()
    
    @staticmethod
    def _create_connection() -> sqlite3.Connection:
        """Создание нового соединения с оптимизацией"""
        conn = sqlite3.connect(
            DatabaseManager.DB_PATH,
            timeout=30.0,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        # Оптимизация для производительности
        conn.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging
        conn.execute('PRAGMA synchronous=NORMAL')
        conn.execute('PRAGMA cache_size=10000')
        conn.execute('PRAGMA temp_store=MEMORY')
        return conn
    
    @classmethod
    @contextmanager
    def get_connection(cls):
        """Context manager для безопасной работы с соединением"""
        conn = cls._get_pool_connection()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.error(f"Database error: {e}", exc_info=True)
            raise
        finally:
            cls._return_to_pool(conn)
    
    @classmethod
    def init_database(cls) -> None:
        """Инициализация базы данных с улучшенной схемой и индексами"""
        if not os.path.exists(cls.DB_PATH):
            os.makedirs(os.path.dirname(cls.DB_PATH), exist_ok=True)
        
        with cls.get_connection() as conn:
            cursor = conn.cursor()
            
            # Создаем таблицу users с улучшенной схемой
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    phone TEXT,
                    gender TEXT,
                    is_premium INTEGER DEFAULT 0,
                    is_verified INTEGER DEFAULT 0,
                    is_bot INTEGER DEFAULT 0,
                    last_activity_utc TEXT,
                    collected_at TEXT NOT NULL,
                    source_group TEXT,
                    group_id TEXT,
                    account_type TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_user_collection UNIQUE (user_id, collected_at, source_group)
                )
            ''')
            
            # Миграция: добавляем is_bot если его нет (для существующих БД)
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'is_bot' not in columns:
                try:
                    cursor.execute('ALTER TABLE users ADD COLUMN is_bot INTEGER DEFAULT 0')
                    logging.info("Migration: Added is_bot column to existing database")
                except Exception as e:
                    logging.warning(f"Could not add is_bot column (may already exist): {e}")
            
            # Создаем оптимизированные индексы
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON users(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_username ON users(username)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_collected_at ON users(collected_at DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_group ON users(source_group)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_premium ON users(is_premium) WHERE is_premium = 1')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_verified ON users(is_verified) WHERE is_verified = 1')
            
            # Создаем таблицу для метаданных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                INSERT OR REPLACE INTO metadata (key, value) 
                VALUES ('db_version', '2.2.0')
            ''')
            
            logging.info("Database initialized successfully with optimized schema")
    
    @staticmethod
    def is_file_locked(filepath):
        """Проверка блокировки файла (для обратной совместимости)"""
        if not os.path.exists(filepath):
            return False
        try:
            # Для SQLite проверяем возможность подключения
            if filepath.endswith('.db'):
                conn = sqlite3.connect(filepath, timeout=1.0)
                conn.close()
                return False
            else:
                with open(filepath, 'a'):
                    return False
        except (IOError, PermissionError, sqlite3.OperationalError):
            return True
    
    @staticmethod
    async def wait_for_unlock(filepath, max_retries=10):
        """Ожидание разблокировки файла"""
        retry_count = 0
        while DatabaseManager.is_file_locked(filepath) and retry_count < max_retries:
            await asyncio.sleep(2)
            retry_count += 1
            logging.info(f"Waiting for file unlock: {filepath}, attempt {retry_count}")
        return retry_count < max_retries
    
    @staticmethod
    async def backup_database():
        """Создание резервной копии базы данных"""
        base_file = DatabaseManager.DB_PATH
        if os.path.exists(base_file):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f'bot/data/backups/all_users_backup_{timestamp}.db'
            try:
                # Создаем директорию для бэкапов, если её нет
                os.makedirs('bot/data/backups', exist_ok=True)
                shutil.copy2(base_file, backup_file)
                logging.info(f"Database backup created: {backup_file}")
                return backup_file
            except Exception as e:
                logging.error(f"Error creating backup: {e}")
        return None
    
    @classmethod
    def get_database_stats(cls) -> Dict[str, Any]:
        """Получение расширенной статистики базы данных с улучшенной производительностью"""
        cls.init_database()
        
        try:
            with cls.get_connection() as conn:
                cursor = conn.cursor()
                
                # Используем одиночный запрос для базовой статистики (оптимизация)
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total_users,
                        COUNT(CASE WHEN username IS NOT NULL AND username != "" THEN 1 END) as with_username,
                        COUNT(CASE WHEN is_premium = 1 THEN 1 END) as premium_users,
                        COUNT(CASE WHEN is_verified = 1 THEN 1 END) as verified_users,
                        COUNT(CASE WHEN is_bot = 1 THEN 1 END) as bot_accounts
                    FROM users
                ''')
                
                row = cursor.fetchone()
                stats = {
                    "total_users": row[0],
                    "with_username": row[1],
                    "premium_users": row[2],
                    "verified_users": row[3],
                    "bot_accounts": row[4]
                }
                
                # Статистика по датам
                cursor.execute('''
                    SELECT 
                        MIN(collected_at) as first_record,
                        MAX(collected_at) as last_record
                    FROM users
                    WHERE collected_at IS NOT NULL
                ''')
                date_stats = cursor.fetchone()
                
                if date_stats and date_stats[0]:
                    stats["first_record"] = datetime.fromisoformat(date_stats[0])
                    stats["last_record"] = datetime.fromisoformat(date_stats[1])
                    
                    # Самый активный день
                    cursor.execute('''
                        SELECT DATE(collected_at) as date, COUNT(*) as count
                        FROM users
                        WHERE collected_at IS NOT NULL
                        GROUP BY DATE(collected_at)
                        ORDER BY count DESC
                        LIMIT 1
                    ''')
                    most_active = cursor.fetchone()
                    if most_active:
                        stats["most_active_day"] = most_active[0]
                        stats["most_active_day_count"] = most_active[1]
                
                # Статистика по источникам
                cursor.execute('''
                    SELECT source_group, COUNT(*) as count
                    FROM users
                    WHERE source_group IS NOT NULL AND source_group != ""
                    GROUP BY source_group
                    ORDER BY count DESC
                    LIMIT 5
                ''')
                top_sources = cursor.fetchall()
                stats["top_sources"] = {row[0]: row[1] for row in top_sources}
                
                # Дополнительная статистика
                cursor.execute('SELECT COUNT(DISTINCT user_id) FROM users')
                stats["unique_users"] = cursor.fetchone()[0]
                
                return stats
                
        except Exception as e:
            logging.error(f"Error getting database stats: {e}", exc_info=True)
            return {"error": str(e)}
    
    @classmethod
    def insert_users(cls, users_data: List[Tuple], batch_size: int = 1000) -> int:
        """
        Вставка пользователей в базу данных с batch операциями для производительности
        
        Args:
            users_data: Список кортежей с данными пользователей
                       (user_id, username, first_name, last_name, phone, gender,
                        is_premium, is_verified, last_activity, collected_at,
                        source_group, group_id, account_type)
            batch_size: Размер batch для вставки (по умолчанию 1000)
        
        Returns:
            Количество вставленных записей
        """
        cls.init_database()
        
        if not users_data:
            logging.warning("No users data provided for insertion")
            return 0
        
        try:
            with cls.get_connection() as conn:
                cursor = conn.cursor()
                inserted_count = 0
                
                # Обрабатываем данные батчами для производительности
                for i in range(0, len(users_data), batch_size):
                    batch = users_data[i:i + batch_size]
                    
                    try:
                        # Используем executemany для batch вставки
                        cursor.executemany('''
                            INSERT OR IGNORE INTO users (
                                user_id, username, first_name, last_name, phone,
                                gender, is_premium, is_verified, last_activity_utc,
                                collected_at, source_group, group_id, account_type
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', batch)
                        
                        inserted_count += cursor.rowcount
                        
                    except sqlite3.IntegrityError as e:
                        logging.warning(f"Integrity error in batch {i//batch_size + 1}: {e}")
                        continue
                
                logging.info(f"Inserted {inserted_count} new users into database (total processed: {len(users_data)})")
                return inserted_count
                
        except Exception as e:
            logging.error(f"Error inserting users: {e}", exc_info=True)
            return 0
    
    @classmethod
    def search_users(cls, search_term: str, limit: int = 100) -> pd.DataFrame:
        """
        Поиск пользователей по различным критериям с оптимизацией
        
        Args:
            search_term: Строка поиска (ID, username или имя)
            limit: Максимальное количество результатов (по умолчанию 100)
        
        Returns:
            DataFrame с результатами поиска
        """
        cls.init_database()
        
        if not search_term or not search_term.strip():
            logging.warning("Empty search term provided")
            return pd.DataFrame()
        
        try:
            with cls.get_connection() as conn:
                search_term = search_term.strip()
                
                # Поиск по ID
                if search_term.isdigit():
                    user_id = int(search_term)
                    query = f'SELECT * FROM users WHERE user_id = ? LIMIT {limit}'
                    df = pd.read_sql_query(query, conn, params=(user_id,))
                
                # Поиск по username
                elif search_term.startswith('@'):
                    username = search_term
                    query = f'SELECT * FROM users WHERE username LIKE ? ORDER BY collected_at DESC LIMIT {limit}'
                    df = pd.read_sql_query(query, conn, params=(f'%{username}%',))
                
                # Поиск по имени
                else:
                    query = f'''
                        SELECT * FROM users 
                        WHERE first_name LIKE ? OR last_name LIKE ? OR username LIKE ?
                        ORDER BY collected_at DESC 
                        LIMIT {limit}
                    '''
                    search_param = f'%{search_term}%'
                    df = pd.read_sql_query(query, conn, params=(search_param, search_param, search_param))
                
                # Переименовываем колонки для консистентности
                if not df.empty:
                    column_mapping = {
                        'user_id': 'User_id',
                        'username': 'Username',
                        'first_name': 'Имя',
                        'last_name': 'Фамилия',
                        'phone': 'Телефон',
                        'gender': 'Пол',
                        'is_premium': 'Премиум',
                        'is_verified': 'Verified',
                        'is_bot': 'Бот',
                        'last_activity_utc': 'Последняя активность (UTC)',
                        'collected_at': 'Время сбора (UTC+1)',
                        'source_group': 'Источник группы',
                        'group_id': 'ID группы',
                        'account_type': 'Тип аккаунта'
                    }
                    df = df.rename(columns=column_mapping)
                
                logging.info(f"Search completed: found {len(df)} results for '{search_term}'")
                return df
                
        except Exception as e:
            logging.error(f"Error searching users: {e}", exc_info=True)
            return pd.DataFrame()
    
    @classmethod
    def get_all_users(cls, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Получение всех пользователей из базы данных с опциональным лимитом
        
        Args:
            limit: Максимальное количество записей (None = все записи)
        
        Returns:
            DataFrame с пользователями (с переименованными колонками для совместимости)
        """
        cls.init_database()
        
        try:
            with cls.get_connection() as conn:
                query = 'SELECT * FROM users ORDER BY collected_at DESC'
                if limit:
                    query += f' LIMIT {limit}'
                df = pd.read_sql_query(query, conn)
                
                # Переименовываем колонки для совместимости с аналитикой и экспортом
                if not df.empty:
                    column_mapping = {
                        'user_id': 'User_id',
                        'username': 'Username',
                        'first_name': 'Имя',
                        'last_name': 'Фамилия',
                        'phone': 'Телефон',
                        'gender': 'Пол',
                        'is_premium': 'Премиум',
                        'is_verified': 'Verified',
                        'is_bot': 'Бот',
                        'last_activity_utc': 'Последняя активность (UTC)',
                        'collected_at': 'Время сбора (UTC+1)',
                        'source_group': 'Источник группы',
                        'group_id': 'ID группы',
                        'account_type': 'Тип аккаунта'
                    }
                    df = df.rename(columns=column_mapping)
                
                logging.info(f"Retrieved {len(df)} users from database")
                return df
        except Exception as e:
            logging.error(f"Error getting all users: {e}", exc_info=True)
            return pd.DataFrame()
    
    @classmethod
    def get_existing_user_ids(cls) -> set:
        """
        Получение множества существующих user_id с оптимизацией
        
        Returns:
            Множество user_id
        """
        cls.init_database()
        
        try:
            with cls.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT DISTINCT user_id FROM users')
                user_ids = {row[0] for row in cursor.fetchall()}
                logging.info(f"Retrieved {len(user_ids)} unique user IDs")
                return user_ids
        except Exception as e:
            logging.error(f"Error getting existing user IDs: {e}", exc_info=True)
            return set()
    
    @classmethod
    def export_to_excel(cls, output_path: Optional[str] = None) -> Optional[str]:
        """
        Экспорт базы данных в Excel файл с оптимизацией
        
        Args:
            output_path: Путь для сохранения файла. Если не указан, используется временный файл
        
        Returns:
            Путь к созданному файлу или None в случае ошибки
        """
        cls.init_database()
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f'bot/data/exports/all_users_{timestamp}.xlsx'
        
        try:
            # Создаем директорию для экспорта, если её нет
            dir_path = os.path.dirname(output_path)
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            
            with cls.get_connection() as conn:
                # Получаем данные с переименованными колонками для совместимости
                query = '''
                    SELECT 
                        user_id as "User_id",
                        username as "Username",
                        first_name as "Имя",
                        last_name as "Фамилия",
                        phone as "Телефон",
                        gender as "Пол",
                        is_premium as "Премиум",
                        is_verified as "Verified",
                        last_activity_utc as "Последняя активность (UTC)",
                        collected_at as "Время сбора (UTC+1)",
                        source_group as "Источник группы",
                        group_id as "ID группы",
                        account_type as "Тип аккаунта"
                    FROM users
                    ORDER BY collected_at DESC
                '''
                
                df = pd.read_sql_query(query, conn)
            
            # Сохраняем в Excel с оптимизацией
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Users')
                
                # Автоматическая настройка ширины столбцов
                worksheet = writer.sheets['Users']
                for idx, col in enumerate(df.columns, 1):
                    max_length = max(
                        df[col].astype(str).apply(len).max(),
                        len(str(col))
                    )
                    worksheet.column_dimensions[chr(64 + idx)].width = min(max_length + 2, 50)
            
            logging.info(f"Database exported to Excel: {output_path} ({len(df)} records)")
            return output_path
            
        except Exception as e:
            logging.error(f"Error exporting to Excel: {e}", exc_info=True)
            return None
    
    @classmethod
    def update_existing_database_usernames(cls) -> int:
        """
        Обновляет существующую базу данных, добавляя @ к username где его нет
        
        Returns:
            Количество обновленных записей
        """
        cls.init_database()
        
        try:
            with cls.get_connection() as conn:
                cursor = conn.cursor()
                
                # Обновляем username, добавляя @ где его нет
                cursor.execute('''
                    UPDATE users
                    SET username = '@' || username,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE username IS NOT NULL 
                    AND username != ''
                    AND username NOT LIKE '@%'
                ''')
                
                updated_count = cursor.rowcount
                logging.info(f"Updated {updated_count} usernames with @ prefix")
                return updated_count
                
        except Exception as e:
            logging.error(f"Error updating database usernames: {e}", exc_info=True)
            return 0
    
    @classmethod
    def get_user_by_id(cls, user_id: int) -> Optional[pd.DataFrame]:
        """
        Получение пользователя по ID
        
        Args:
            user_id: ID пользователя
        
        Returns:
            DataFrame с данными пользователя или None
        """
        cls.init_database()
        
        try:
            with cls.get_connection() as conn:
                query = 'SELECT * FROM users WHERE user_id = ? LIMIT 1'
                df = pd.read_sql_query(query, conn, params=(user_id,))
                return df if not df.empty else None
        except Exception as e:
            logging.error(f"Error getting user by ID {user_id}: {e}", exc_info=True)
            return None
    
    @classmethod
    def add_user(cls, user_data: Dict[str, Any]) -> bool:
        """
        Добавление одного пользователя в базу данных
        
        Args:
            user_data: Словарь с данными пользователя
        
        Returns:
            True если успешно добавлен, False иначе
        """
        cls.init_database()
        
        try:
            with cls.get_connection() as conn:
                cursor = conn.cursor()
                
                # Преобразуем datetime в строку если нужно
                collected_at = user_data.get('collection_time')
                if isinstance(collected_at, datetime):
                    collected_at = collected_at.isoformat()
                elif collected_at is None:
                    collected_at = datetime.now().isoformat()
                
                cursor.execute('''
                    INSERT OR IGNORE INTO users (
                        user_id, username, first_name, last_name, phone,
                        gender, is_premium, is_verified, is_bot,
                        collected_at, source_group, group_id, account_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    user_data.get('user_id'),
                    user_data.get('username'),
                    user_data.get('first_name'),
                    user_data.get('last_name'),
                    user_data.get('phone'),
                    user_data.get('gender'),
                    1 if user_data.get('is_premium') else 0,
                    1 if user_data.get('is_verified') else 0,
                    1 if user_data.get('is_bot') else 0,
                    collected_at,
                    user_data.get('source_group'),
                    user_data.get('group_id'),
                    user_data.get('account_type', 'Regular')
                ))
                
                success = cursor.rowcount > 0
                if success:
                    logging.info(f"Added user {user_data.get('user_id')} to database")
                return success
                
        except Exception as e:
            logging.error(f"Error adding user: {e}", exc_info=True)
            return False
    
    @classmethod
    def cleanup_pool(cls) -> None:
        """Очистка пула соединений при завершении работы"""
        with cls._lock:
            while cls._connection_pool:
                conn = cls._connection_pool.pop()
                try:
                    conn.close()
                except Exception as e:
                    logging.error(f"Error closing connection: {e}")
            logging.info("Connection pool cleaned up")
