"""
Start Handler
Обработчик команды /start и главного меню
"""

from telegram import Update
from telegram.ext import ContextTypes
import logging
from datetime import datetime

from utils.keyboards import create_main_menu_keyboard, create_user_search_keyboard
from utils.formatters import format_current_time

logger = logging.getLogger(__name__)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    welcome_text = f"""
🤖 <b>Telegram Marketing Assistant</b>

Привет, {user.first_name}! 👋

Я твой карманный дашборд для маркетинга с быстрым доступом к ключевым метрикам.

<b>Доступные разделы:</b>
• 👥 <b>Сегментация</b> - анализ кластеров пользователей
• 🛒 <b>Вероятность покупки</b> - топ клиентов с высоким потенциалом
• ⚠️ <b>Риск оттока</b> - анализ клиентов на грани ухода
• 🎯 <b>Рекомендации</b> - персонализированные предложения
• 🚨 <b>Аномалии</b> - необычные паттерны поведения
• 💰 <b>Эластичность цен</b> - анализ чувствительности к ценам
• 🔎 <b>Поиск пользователя</b> - детальная сводка по ID

📅 {format_current_time()}

Выбери раздел для начала работы:
    """
    
    reply_markup = create_main_menu_keyboard()
    
    await update.message.reply_text(
        welcome_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )
    
    logger.info(f"User {user.id} started the Marketing Assistant")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help"""
    help_text = """
📚 <b>Справка по командам</b>

<b>Основные команды:</b>
/start - главное меню
/help - эта справка
/user &lt;ID&gt; - быстрая сводка пользователя
/today - ключевые цифры за сегодня

<b>Примеры использования:</b>
• /user 123 - показать сводку пользователя #123
• /today - мини-дайджест за сегодня

<b>Навигация:</b>
Используй кнопки для перехода между разделами. Все данные в реальном времени из системы аналитики.

<b>Поддержка:</b>
При возникновении проблем попробуй перезапустить бота командой /start
    """
    
    reply_markup = create_main_menu_keyboard()
    
    await update.message.reply_text(
        help_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /user <ID>"""
    user = update.effective_user
    args = context.args
    
    if not args:
        await update.message.reply_text(
            "❌ Укажите ID пользователя\n\nПример: /user 123",
            reply_markup=create_user_search_keyboard()
        )
        return
    
    try:
        user_id = int(args[0])
        # Импортируем и вызываем функцию поиска пользователя
        from .user_handler import get_user_summary
        await get_user_summary(update, context, user_id)
        
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат ID пользователя\n\nID должен быть числом. Пример: /user 123",
            reply_markup=create_user_search_keyboard()
        )


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /today"""
    today_text = """
📊 <b>Ключевые цифры за сегодня</b>

🕐 {current_time}

Данная функция будет реализована в следующих шагах.

Пока что используй главное меню для доступа к разделам аналитики.
    """.format(current_time=format_current_time())
    
    reply_markup = create_main_menu_keyboard()
    
    await update.message.reply_text(
        today_text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

