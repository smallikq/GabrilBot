import os
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

# Initialize bot and dispatcher (токен будет установлен в main.py)
bot = None
dp = Dispatcher(storage=MemoryStorage())

# Global variables
active_tasks = {}  # Для отслеживания активных задач
user_settings = {}  # Настройки пользователей
pending_missed_days = []  # Пропущенные дни


def initialize_bot(token):
    """Инициализация бота с токеном"""
    global bot
    bot = Bot(token=token)
    return bot


def get_bot():
    """Получить экземпляр бота"""
    return bot


def get_dispatcher():
    """Получить экземпляр диспетчера"""
    return dp

