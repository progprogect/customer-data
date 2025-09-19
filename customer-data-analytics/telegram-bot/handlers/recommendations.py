"""
Recommendations Handler
Обработчик команд рекомендаций
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging
import httpx

logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8000"


async def recommendations_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /recommendations"""
    await recommendations_callback(update, context)


async def recommendations_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback'ов рекомендаций"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "recommendations_main":
        await show_recommendations_main(query)
    elif query.data == "recommendations_get":
        await show_get_recommendations(query)
    elif query.data == "recommendations_similar":
        await show_similar_products(query)
    elif query.data == "recommendations_train":
        await show_train_model(query)
    elif query.data == "recommendations_back":
        await show_recommendations_main(query)


async def show_recommendations_main(query):
    """Показать главное меню рекомендаций"""
    keyboard = [
        [
            InlineKeyboardButton("🎯 Получить рекомендации", callback_data="recommendations_get"),
            InlineKeyboardButton("🔍 Похожие товары", callback_data="recommendations_similar")
        ],
        [
            InlineKeyboardButton("🤖 Обучение модели", callback_data="recommendations_train"),
            InlineKeyboardButton("📊 Статус модели", callback_data="recommendations_status")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="start")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = """
🎯 <b>Рекомендации</b>

Система персонализированных рекомендаций товаров.

<b>Функции:</b>
• <b>🎯 Получить рекомендации</b> - рекомендации для конкретного пользователя
• <b>🔍 Похожие товары</b> - поиск похожих товаров
• <b>🤖 Обучение модели</b> - переобучение рекомендательной системы
• <b>📊 Статус модели</b> - информация о состоянии модели
    """
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_get_recommendations(query):
    """Показать получение рекомендаций"""
    text = """
🎯 <b>Получить рекомендации</b>

Для получения рекомендаций введи ID пользователя.

<b>Формат:</b> <code>user_id:123</code>

<b>Примеры:</b>
• <code>user_id:1</code> - рекомендации для пользователя 1
• <code>user_id:42</code> - рекомендации для пользователя 42

<b>Методы рекомендаций:</b>
• Collaborative Filtering - на основе похожих пользователей
• Content-Based - на основе характеристик товаров
• Hybrid - комбинированный подход
    """
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="recommendations_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_similar_products(query):
    """Показать похожие товары"""
    text = """
🔍 <b>Похожие товары</b>

Для поиска похожих товаров введи ID товара.

<b>Формат:</b> <code>product_id:123</code>

<b>Примеры:</b>
• <code>product_id:1</code> - похожие на товар 1
• <code>product_id:42</code> - похожие на товар 42

Система найдет товары с похожими характеристиками, описанием и поведением пользователей.
    """
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="recommendations_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_train_model(query):
    """Показать обучение модели"""
    text = """
🤖 <b>Обучение модели рекомендаций</b>

Переобучение рекомендательной системы на новых данных.

<b>Методы обучения:</b>
• Collaborative Filtering
• Content-Based Filtering
• Hybrid (рекомендуется)

<b>Внимание:</b> Обучение может занять несколько минут.
    """
    
    keyboard = [
        [
            InlineKeyboardButton("🔄 Collaborative", callback_data="recommendations_train_collaborative"),
            InlineKeyboardButton("🔄 Content-Based", callback_data="recommendations_train_content")
        ],
        [
            InlineKeyboardButton("🔄 Hybrid", callback_data="recommendations_train_hybrid")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="recommendations_main")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )
