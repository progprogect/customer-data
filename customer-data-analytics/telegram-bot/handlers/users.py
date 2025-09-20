"""
Users Handler
Обработчик команд пользователей
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging
import httpx

logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8000"


async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /users"""
    await users_callback(update, context)


async def users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback'ов пользователей"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "users_main":
        await show_users_main(query)
    elif query.data == "users_list":
        await show_users_list(query)
    elif query.data == "users_profile":
        await show_user_profile(query)
    elif query.data == "users_ltv":
        await show_user_ltv(query)
    elif query.data == "users_segments":
        await show_user_segments(query)
    elif query.data == "users_back":
        await show_users_main(query)


async def show_users_main(query):
    """Показать главное меню пользователей"""
    keyboard = [
        [
            InlineKeyboardButton("👥 Список пользователей", callback_data="users_list"),
            InlineKeyboardButton("👤 Профиль пользователя", callback_data="users_profile")
        ],
        [
            InlineKeyboardButton("💰 LTV анализ", callback_data="users_ltv"),
            InlineKeyboardButton("🎯 Сегменты", callback_data="users_segments")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="start")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = """
👥 <b>Пользователи</b>

Управление пользователями и анализ их поведения.

<b>Функции:</b>
• <b>👥 Список пользователей</b> - просмотр всех пользователей
• <b>👤 Профиль пользователя</b> - детальная информация о пользователе
• <b>💰 LTV анализ</b> - расчет пожизненной ценности клиента
• <b>🎯 Сегменты</b> - просмотр сегментов пользователей
    """
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_users_list(query):
    """Показать список пользователей"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/api/v1/users/?limit=10")
            users = response.json()
        
        if not users:
            text = "👥 <b>Список пользователей</b>\n\nПользователи не найдены."
        else:
            text = "👥 <b>Список пользователей</b>\n\n"
            for user in users[:10]:  # Показываем только первых 10
                text += f"• <b>ID {user.get('user_id', 'N/A')}</b> - {user.get('email', 'N/A')}\n"
                text += f"  📍 {user.get('country', 'N/A')}, {user.get('city', 'N/A')}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="users_list")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error fetching users list: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки списка пользователей",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="users_main")]
            ])
        )


async def show_user_profile(query):
    """Показать профиль пользователя"""
    text = """
👤 <b>Профиль пользователя</b>

Для просмотра профиля введи ID пользователя.

<b>Формат:</b> <code>user_id:123</code>

<b>Примеры:</b>
• <code>user_id:1</code> - профиль пользователя 1
• <code>user_id:42</code> - профиль пользователя 42

<b>Информация в профиле:</b>
• Основные данные пользователя
• История заказов
• События поведения
• Аналитические метрики
    """
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="users_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_user_ltv(query):
    """Показать LTV анализ"""
    text = """
💰 <b>LTV анализ</b>

Расчет пожизненной ценности клиента (Lifetime Value).

<b>Для расчета LTV введи ID пользователя:</b>
<b>Формат:</b> <code>user_id:123</code>

<b>LTV включает:</b>
• Общую сумму покупок
• Частоту заказов
• Время с последней покупки
• Прогнозируемую ценность
    """
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="users_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_user_segments(query):
    """Показать сегменты пользователей"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/api/v1/users/segments/")
            segments = response.json()
        
        if not segments:
            text = "🎯 <b>Сегменты пользователей</b>\n\nСегменты не найдены. Запустите сегментацию в разделе Аналитика."
        else:
            text = "🎯 <b>Сегменты пользователей</b>\n\n"
            for i, segment in enumerate(segments):
                text += f"• <b>Сегмент {i+1}</b>\n"
                text += f"  👥 Пользователей: {segment.get('size', 'N/A')}\n"
                text += f"  📊 Характеристики: {segment.get('description', 'N/A')}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="users_segments")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="users_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error fetching user segments: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки сегментов пользователей",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="users_main")]
            ])
        )

