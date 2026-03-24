from aiogram.fsm.state import State, StatesGroup


class AdminScanStates(StatesGroup):
    waiting_cups_count = State()


class OwnerStates(StatesGroup):
    waiting_broadcast_text = State()
    waiting_add_admin_id = State()
    waiting_remove_admin_id = State()