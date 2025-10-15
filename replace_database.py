#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для замены базы данных из Excel файла
Очищает текущую БД и загружает данные из all_users.xlsx
"""

import os
import sys
import pandas as pd
import shutil
from datetime import datetime

# Настройка кодировки для Windows консоли
if sys.platform == 'win32':
    import codecs
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot.utils.database import DatabaseManager


def replace_database_from_excel(excel_file: str):
    """
    Заменяет базу данных данными из Excel файла

    Args:
        excel_file: Путь к Excel файлу с новой базой
    """

    print("=" * 60)
    print("Замена базы данных из Excel файла")
    print("=" * 60)

    # Проверяем существование Excel файла
    if not os.path.exists(excel_file):
        print(f"❌ Ошибка: файл {excel_file} не найден")
        return False

    print(f"\n📂 Файл источника: {excel_file}")

    try:
        # Читаем Excel файл
        print("\n📊 Читаю Excel файл...")
        df = pd.read_excel(excel_file)
        print(f"✅ Прочитано {len(df)} записей")

        # Показываем структуру данных
        print(f"\n📋 Колонки в файле:")
        for col in df.columns:
            print(f"   • {col}")

        # Создаём бэкап текущей базы
        db_path = DatabaseManager.DB_PATH
        if os.path.exists(db_path):
            backup_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f'bot/data/backups/all_users_before_replace_{backup_time}.db'
            os.makedirs('bot/data/backups', exist_ok=True)

            print(f"\n💾 Создаю бэкап текущей базы...")
            shutil.copy2(db_path, backup_path)
            print(f"✅ Бэкап создан: {backup_path}")

            # Удаляем текущую базу
            print(f"\n🗑️ Удаляю старую базу данных...")
            os.remove(db_path)
            print(f"✅ Старая база удалена")
        else:
            print(f"\n⚠️ Текущая база данных не найдена, создаю новую")

        # Инициализируем новую базу
        print(f"\n🔧 Инициализирую новую базу данных...")
        DatabaseManager.init_database()
        print(f"✅ База данных инициализирована")

        # Подготавливаем данные для вставки
        print(f"\n📝 Подготавливаю данные для вставки...")
        users_data = []

        # Маппинг колонок из Excel в формат БД
        column_mapping = {
            'User_id': 'user_id',
            'Username': 'username',
            'Имя': 'first_name',
            'Фамилия': 'last_name',
            'Телефон': 'phone',
            'Пол': 'gender',
            'Премиум': 'is_premium',
            'Verified': 'is_verified',
            'Последняя активность (UTC)': 'last_activity_utc',
            'Время сбора (UTC+1)': 'collected_at',
            'Источник группы': 'source_group',
            'ID группы': 'group_id',
            'Тип аккаунта': 'account_type'
        }

        skipped_count = 0
        for idx, row in df.iterrows():
            try:
                # Проверяем и конвертируем User_id
                user_id_raw = row.get('User_id', 0)

                # Пропускаем строки с некорректным User_id
                if pd.isna(user_id_raw) or user_id_raw == '' or user_id_raw == 0:
                    skipped_count += 1
                    continue

                # Пытаемся преобразовать в int
                try:
                    user_id = int(user_id_raw)
                except (ValueError, TypeError):
                    # Если это строка, пробуем удалить нечисловые символы
                    user_id_str = str(user_id_raw).strip()
                    # Пропускаем строки со специальными символами в начале (например заголовки)
                    if not user_id_str or not user_id_str[0].isdigit():
                        skipped_count += 1
                        continue
                    user_id = int(''.join(filter(str.isdigit, user_id_str)))

                if user_id <= 0:
                    skipped_count += 1
                    continue

                user_tuple = (
                    user_id,  # user_id
                    row.get('Username', None),  # username
                    row.get('Имя', None),  # first_name
                    row.get('Фамилия', None),  # last_name
                    row.get('Телефон', None),  # phone
                    row.get('Пол', None),  # gender
                    1 if row.get('Премиум') else 0,  # is_premium
                    1 if row.get('Verified') else 0,  # is_verified
                    row.get('Последняя активность (UTC)', None),  # last_activity_utc
                    row.get('Время сбора (UTC+1)', None),  # collected_at
                    row.get('Источник группы', None),  # source_group
                    str(row.get('ID группы', '')) if pd.notna(row.get('ID группы')) else None,  # group_id
                    row.get('Тип аккаунта', 'Regular')  # account_type
                )
                users_data.append(user_tuple)
            except Exception as e:
                skipped_count += 1
                if skipped_count <= 5:  # Показываем только первые 5 ошибок
                    print(f"   ⚠️ Пропущена строка {idx}: {e}")
                continue

        print(f"✅ Подготовлено {len(users_data)} записей")
        if skipped_count > 0:
            print(f"⚠️ Пропущено {skipped_count} некорректных записей")

        # Вставляем данные в базу
        print(f"\n💿 Загружаю данные в базу (батчами по 1000)...")
        inserted_count = DatabaseManager.insert_users(users_data, batch_size=1000)
        print(f"✅ Вставлено {inserted_count} записей")

        # Проверяем результат
        print(f"\n📊 Проверяю новую базу данных...")
        stats = DatabaseManager.get_database_stats()

        print(f"\n{'=' * 60}")
        print(f"✅ БАЗА ДАННЫХ УСПЕШНО ЗАМЕНЕНА")
        print(f"{'=' * 60}")
        print(f"\n📊 Статистика новой базы:")
        print(f"   • Всего пользователей: {stats.get('total_users', 0):,}")
        print(f"   • Уникальных пользователей: {stats.get('unique_users', 0):,}")
        print(f"   • С username: {stats.get('with_username', 0):,}")
        print(f"   • Premium: {stats.get('premium_users', 0):,}")
        print(f"   • Verified: {stats.get('verified_users', 0):,}")

        if stats.get('top_sources'):
            print(f"\n   📌 Топ источников:")
            for source, count in list(stats['top_sources'].items())[:5]:
                print(f"      • {source}: {count:,}")

        print(f"\n{'=' * 60}\n")

        return True

    except Exception as e:
        print(f"\n❌ Ошибка при замене базы данных: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    excel_file = "all_users.xlsx"

    if len(sys.argv) > 1:
        excel_file = sys.argv[1]

    print(f"\n⚠️  ВНИМАНИЕ: Эта операция заменит текущую базу данных!")
    print(f"   Текущая база будет сохранена в резервную копию.")

    response = input(f"\nПродолжить? (yes/no): ").strip().lower()

    if response in ['yes', 'y', 'да']:
        success = replace_database_from_excel(excel_file)
        if success:
            print("✅ Операция завершена успешно!")
            sys.exit(0)
        else:
            print("❌ Операция завершена с ошибками!")
            sys.exit(1)
    else:
        print("❌ Операция отменена пользователем")
        sys.exit(0)
