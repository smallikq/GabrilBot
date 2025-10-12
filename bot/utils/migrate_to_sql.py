"""
Скрипт для миграции базы данных из Excel в SQL
"""
import os
import sys
import sqlite3
import logging
from datetime import datetime
import pandas as pd

# Устанавливаем UTF-8 для Windows консоли
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_sql_schema(conn):
    """Создание SQL схемы базы данных"""
    cursor = conn.cursor()
    
    # Создаем таблицу users
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
            last_activity_utc TEXT,
            collected_at TEXT NOT NULL,
            source_group TEXT,
            group_id TEXT,
            account_type TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Создаем индексы для быстрого поиска
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON users(user_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_username ON users(username)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_collected_at ON users(collected_at)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_source_group ON users(source_group)')
    cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_user_unique ON users(user_id, collected_at, source_group)')
    
    # Создаем таблицу для метаданных миграции
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS migration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_date TEXT NOT NULL,
            records_migrated INTEGER NOT NULL,
            source_file TEXT NOT NULL,
            status TEXT NOT NULL,
            notes TEXT
        )
    ''')
    
    conn.commit()
    logging.info("SQL schema created successfully")


def migrate_excel_to_sql(excel_path, db_path):
    """Миграция данных из Excel в SQLite"""
    
    if not os.path.exists(excel_path):
        logging.error(f"Excel file not found: {excel_path}")
        return False
    
    try:
        # Читаем Excel файл
        logging.info(f"Reading Excel file: {excel_path}")
        df = pd.read_excel(excel_path)
        logging.info(f"Found {len(df)} records in Excel file")
        
        # Подключаемся к SQLite базе
        conn = sqlite3.connect(db_path)
        
        # Создаем схему
        create_sql_schema(conn)
        
        # Подготавливаем данные для вставки
        cursor = conn.cursor()
        records_inserted = 0
        records_skipped = 0
        
        for idx, row in df.iterrows():
            try:
                # Преобразуем данные
                user_id = int(row.get('User_id', 0)) if pd.notna(row.get('User_id')) else 0
                username = str(row.get('Username', '')) if pd.notna(row.get('Username')) else None
                first_name = str(row.get('Имя', '')) if pd.notna(row.get('Имя')) else None
                last_name = str(row.get('Фамилия', '')) if pd.notna(row.get('Фамилия')) else None
                phone = str(row.get('Телефон', '')) if pd.notna(row.get('Телефон')) else None
                gender = str(row.get('Пол', '')) if pd.notna(row.get('Пол')) else None
                # Обработка is_premium с поддержкой текста "Да"/"Нет"
                premium_value = row.get('Премиум')
                if pd.notna(premium_value):
                    if isinstance(premium_value, str):
                        is_premium = 1 if premium_value.lower() in ['да', 'yes', 'true', '1'] else 0
                    else:
                        is_premium = int(premium_value) if premium_value else 0
                else:
                    is_premium = 0
                
                # Обработка is_verified с поддержкой текста "Да"/"Нет"
                verified_value = row.get('Verified')
                if pd.notna(verified_value):
                    if isinstance(verified_value, str):
                        is_verified = 1 if verified_value.lower() in ['да', 'yes', 'true', '1'] else 0
                    else:
                        is_verified = int(verified_value) if verified_value else 0
                else:
                    is_verified = 0
                last_activity = str(row.get('Последняя активность (UTC)', '')) if pd.notna(row.get('Последняя активность (UTC)')) else None
                collected_at = str(row.get('Время сбора (UTC+1)', '')) if pd.notna(row.get('Время сбора (UTC+1)')) else datetime.now().isoformat()
                source_group = str(row.get('Источник группы', '')) if pd.notna(row.get('Источник группы')) else None
                group_id = str(row.get('ID группы', '')) if pd.notna(row.get('ID группы')) else None
                account_type = str(row.get('Тип аккаунта', 'Unknown')) if pd.notna(row.get('Тип аккаунта')) else 'Unknown'
                
                # Пропускаем записи с нулевым user_id
                if user_id == 0:
                    records_skipped += 1
                    continue
                
                # Вставляем или игнорируем дубликаты
                cursor.execute('''
                    INSERT OR IGNORE INTO users (
                        user_id, username, first_name, last_name, phone, 
                        gender, is_premium, is_verified, last_activity_utc,
                        collected_at, source_group, group_id, account_type
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    user_id, username, first_name, last_name, phone,
                    gender, is_premium, is_verified, last_activity,
                    collected_at, source_group, group_id, account_type
                ))
                
                records_inserted += cursor.rowcount
                
            except Exception as e:
                logging.error(f"Error processing row {idx}: {e}")
                records_skipped += 1
                continue
        
        # Сохраняем изменения
        conn.commit()
        
        # Записываем информацию о миграции
        cursor.execute('''
            INSERT INTO migration_history (
                migration_date, records_migrated, source_file, status, notes
            ) VALUES (?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            records_inserted,
            excel_path,
            'SUCCESS',
            f'Successfully migrated {records_inserted} records, skipped {records_skipped} records'
        ))
        
        conn.commit()
        
        # Получаем статистику
        cursor.execute('SELECT COUNT(*) FROM users')
        total_records = cursor.fetchone()[0]
        
        logging.info(f"Migration completed successfully!")
        logging.info(f"Records inserted: {records_inserted}")
        logging.info(f"Records skipped: {records_skipped}")
        logging.info(f"Total records in database: {total_records}")
        
        conn.close()
        return True
        
    except Exception as e:
        logging.error(f"Migration failed: {e}")
        return False


def main():
    """Главная функция миграции"""
    # Пути к файлам
    excel_path = 'bot/data/all_users.xlsx'
    db_path = 'bot/data/all_users.db'
    
    # Проверяем существование Excel файла
    if not os.path.exists(excel_path):
        # Ищем в корне проекта
        excel_path = 'all_users.xlsx'
        if not os.path.exists(excel_path):
            logging.error("Excel file not found!")
            print("\n⚠️  Excel файл не найден!")
            print(f"Ожидается: bot/data/all_users.xlsx или all_users.xlsx")
            return False
    
    # Создаем резервную копию существующей SQL базы
    if os.path.exists(db_path):
        backup_path = f"{db_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        import shutil
        shutil.copy2(db_path, backup_path)
        logging.info(f"Created backup: {backup_path}")
    
    # Выполняем миграцию
    print("\n🚀 Начало миграции из Excel в SQL...")
    print(f"📄 Исходный файл: {excel_path}")
    print(f"🗄️  База данных: {db_path}")
    print("-" * 60)
    
    success = migrate_excel_to_sql(excel_path, db_path)
    
    if success:
        print("-" * 60)
        print("✅ Миграция завершена успешно!")
        print(f"🗄️  База данных создана: {db_path}")
        
        # Создаем резервную копию Excel файла
        if os.path.exists(excel_path):
            excel_backup = f"{excel_path}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            import shutil
            shutil.copy2(excel_path, excel_backup)
            print(f"📋 Создана резервная копия Excel: {excel_backup}")
    else:
        print("-" * 60)
        print("❌ Миграция не удалась!")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

