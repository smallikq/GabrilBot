#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Parser Bot - Точка входа
Запуск бота для сбора данных из Telegram групп
"""

import sys
import os

# Настройка кодировки для Windows консоли
if sys.platform == 'win32':
    import codecs
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    # Альтернативный способ для старых версий Python
    try:
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    except:
        pass

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Импортируем и запускаем основную функцию
from bot.main import main
import asyncio

if __name__ == "__main__":
    try:
        print("=" * 60)
        print("Запуск Telegram Parser Bot v2.1")
        print("=" * 60)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nБот остановлен пользователем")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

