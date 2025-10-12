"""
Пример конфигурации для Telegram Parser Bot
Скопируйте этот файл в parser_cfg.py и заполните своими данными
"""

# ⚠️ ВАЖНО: НЕ ПУБЛИКУЙТЕ ФАЙЛ parser_cfg.py С РЕАЛЬНЫМИ ДАННЫМИ!
# Добавьте parser_cfg.py в .gitignore

# Токен бота от @BotFather в Telegram
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"

# Список аккаунтов для парсинга
# Можно добавить несколько аккаунтов для параллельной работы
accounts = [
    {
        "phone_number": "+1234567890",  # Ваш номер телефона с кодом страны
        "api_id": "YOUR_API_ID",        # Получите на https://my.telegram.org
        "api_hash": "YOUR_API_HASH"     # Получите на https://my.telegram.org
    },
    # Можно добавить больше аккаунтов:
    # {
    #     "phone_number": "+0987654321",
    #     "api_id": "222222",
    #     "api_hash": "bbbbbbbbbbbbbbbb"
    # }
]

# Как получить API данные:
# 1. Перейдите на https://my.telegram.org
# 2. Войдите в свой Telegram аккаунт
# 3. Перейдите в "API development tools"
# 4. Создайте новое приложение
# 5. Скопируйте api_id и api_hash

