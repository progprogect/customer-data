"""
Keyboards
Утилиты для создания inline клавиатур
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from typing import List, Dict, Any, Optional


def create_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Создание главного меню"""
    keyboard = [
        [
            InlineKeyboardButton("👥 Сегментация", callback_data="seg|main"),
            InlineKeyboardButton("🛒 Вероятность покупки", callback_data="prob|main")
        ],
        [
            InlineKeyboardButton("🔎 Поиск пользователя", callback_data="user|search")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_back_to_main_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с кнопкой 'Назад' к главному меню"""
    keyboard = [
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_segmentation_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для сегментации"""
    keyboard = [
        [
            InlineKeyboardButton("📊 Снапшот", callback_data="seg|snapshot")
        ],
        [
            InlineKeyboardButton("🏠 Главное меню", callback_data="main")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_probability_keyboard(page: int = 1, threshold: float = 0.7, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура для вероятности покупки"""
    keyboard = [
        [
            InlineKeyboardButton("Порог 0.6", callback_data=f"prob|th|0.6"),
            InlineKeyboardButton("Порог 0.7", callback_data=f"prob|th|0.7"),
            InlineKeyboardButton("Порог 0.8", callback_data=f"prob|th|0.8")
        ],
        []
    ]
    
    # Пагинация
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("⏪", callback_data=f"prob|page|{page-1}"))
        
        nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="noop"))
        
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton("⏩", callback_data=f"prob|page|{page+1}"))
        
        keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main")])
    
    return InlineKeyboardMarkup(keyboard)






def create_user_actions_keyboard(user_id: int) -> InlineKeyboardMarkup:
    """Клавиатура действий с пользователем"""
    keyboard = [
        [
            InlineKeyboardButton("🏠 Главное меню", callback_data="main")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_pagination_keyboard(
    current_page: int, 
    total_pages: int, 
    callback_prefix: str,
    additional_params: str = ""
) -> List[List[InlineKeyboardButton]]:
    """Создание клавиатуры пагинации"""
    if total_pages <= 1:
        return []
    
    nav_buttons = []
    
    if current_page > 1:
        nav_buttons.append(
            InlineKeyboardButton("⏪", callback_data=f"{callback_prefix}|page|{current_page-1}|{additional_params}")
        )
    
    nav_buttons.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="noop"))
    
    if current_page < total_pages:
        nav_buttons.append(
            InlineKeyboardButton("⏩", callback_data=f"{callback_prefix}|page|{current_page+1}|{additional_params}")
        )
    
    return [nav_buttons]


def create_user_search_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для поиска пользователя"""
    keyboard = [
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_error_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для ошибок"""
    keyboard = [
        [InlineKeyboardButton("🔄 Попробовать снова", callback_data="retry")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main")]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_noop_keyboard() -> InlineKeyboardMarkup:
    """Пустая клавиатура для callback'ов которые ничего не делают"""
    return InlineKeyboardMarkup([])