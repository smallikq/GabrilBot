from aiogram.fsm.state import State, StatesGroup


class Form(StatesGroup):
    """FSM состояния для форм"""
    waiting_for_date = State()
    waiting_for_user_ids = State()
    waiting_for_date_range = State()
    waiting_for_group_filter = State()
    waiting_for_export_format = State()
    waiting_for_schedule_time = State()
    waiting_for_manual_ids = State()

