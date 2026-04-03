from aiogram.fsm.state import State, StatesGroup


class AdminScanStates(StatesGroup):
    waiting_cups_count = State()


class OwnerStates(StatesGroup):
    waiting_broadcast_text = State()
    waiting_add_admin_id = State()
    waiting_remove_admin_id = State()


class SuperAdminStates(StatesGroup):
    waiting_shop_name = State()
    waiting_shop_city = State()
    waiting_shop_address = State()
    waiting_shop_owner_id = State()
    waiting_delete_shop_id = State()
    waiting_extend_shop_id = State()
    waiting_extend_days = State()
