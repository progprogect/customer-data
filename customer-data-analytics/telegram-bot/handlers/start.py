"""
Start Handler
Обработчик команды /start
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging

logger = logging.getLogger(__name__)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    # Создаем клавиатуру с основными разделами
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
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_text = f"""
🤖 <b>Customer Data Analytics Bot</b>

Привет, {user.first_name}! 👋

Я помогу тебе работать с системой аналитики и персонализированных рекомендаций.

<b>Доступные функции:</b>
• 📊 <b>Аналитика</b> - сегментация пользователей, предсказание оттока, анализ цен
• 🎯 <b>Рекомендации</b> - персонализированные рекомендации товаров
• 👥 <b>Пользователи</b> - управление пользователями и их профилями
• 📦 <b>Товары</b> - каталог товаров и их аналитика

Выбери раздел для начала работы:
    """
    
    await update.message.reply_text(
        welcome_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )
    
    logger.info(f"User {user.id} started the bot")

