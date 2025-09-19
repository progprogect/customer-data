"""
Keyboard Utils
Утилиты для создания клавиатур
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup


def create_main_menu():
    """Создание главного меню"""
    keyboard = [
        [
            InlineKeyboardButton("📊 Аналитика", callback_data="analytics_main"),
            InlineKeyboardButton("🎯 Рекомендации", callback_data="recommendations_main")
        ],
        [
            InlineKeyboardButton("👥 Пользователи", callback_data="users_main"),
            InlineKeyboardButton("📦 Товары", callback_data="products_main")
        ],
        [
            InlineKeyboardButton("ℹ️ Помощь", callback_data="help")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_back_button(callback_data: str):
    """Создание кнопки 'Назад'"""
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data=callback_data)]
    ]
    return InlineKeyboardMarkup(keyboard)


def create_pagination_buttons(current_page: int, total_pages: int, callback_prefix: str):
    """Создание кнопок пагинации"""
    keyboard = []
    
    if total_pages > 1:
        row = []
        
        # Кнопка "Предыдущая"
        if current_page > 1:
            row.append(InlineKeyboardButton("◀️", callback_data=f"{callback_prefix}_page_{current_page - 1}"))
        
        # Номер страницы
        row.append(InlineKeyboardButton(f"{current_page}/{total_pages}", callback_data="noop"))
        
        # Кнопка "Следующая"
        if current_page < total_pages:
            row.append(InlineKeyboardButton("▶️", callback_data=f"{callback_prefix}_page_{current_page + 1}"))
        
        keyboard.append(row)
    
    return InlineKeyboardMarkup(keyboard) if keyboard else None


def create_yes_no_buttons(yes_callback: str, no_callback: str):
    """Создание кнопок Да/Нет"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Да", callback_data=yes_callback),
            InlineKeyboardButton("❌ Нет", callback_data=no_callback)
        ]
    ]
    return InlineKeyboardMarkup(keyboard)
