"""
Callback Handler
Главный обработчик callback'ов для Telegram Marketing Assistant
"""

from telegram import Update
from telegram.ext import ContextTypes
import logging
from typing import Dict, Any

from utils.api_client import api_client, APIClientError
from utils.formatters import *
from utils.keyboards import *
from utils.config import get_config

logger = logging.getLogger(__name__)


def filter_anomalies_by_type(anomalies: list, filter_type: str) -> list:
    """Фильтрация аномалий по типу"""
    if filter_type == "all":
        return anomalies
    
    filtered = []
    for anomaly in anomalies:
        triggers = anomaly.get('triggers', [])
        triggers_str = " ".join(triggers).lower()
        
        if filter_type == "spike" and any(word in triggers_str for word in ['spike', 'всплеск', 'рост']):
            filtered.append(anomaly)
        elif filter_type == "drop" and any(word in triggers_str for word in ['drop', 'провал', 'падение']):
            filtered.append(anomaly)
        elif filter_type == "spending" and any(word in triggers_str for word in ['spending', 'траты', 'расходы']):
            filtered.append(anomaly)
        elif filter_type == "orders" and any(word in triggers_str for word in ['orders', 'заказы', 'покупки']):
            filtered.append(anomaly)
    
    return filtered


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главный обработчик callback'ов"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    callback_data = query.data
    
    logger.info(f"Callback from user {user_id}: {callback_data}")
    
    try:
        # Парсим callback_data
        parts = callback_data.split('|')
        action = parts[0] if parts else "main"
        
        # Маршрутизация по действиям
        if action == "main":
            await show_main_menu(query)
        elif action == "seg":
            await handle_segmentation(query, parts, user_id)
        elif action == "prob":
            await handle_probability(query, parts, user_id)
        elif action == "user":
            await handle_user_actions(query, parts, user_id)
        elif action == "noop":
            # Ничего не делаем для noop
            pass
        elif action == "retry":
            await show_main_menu(query)
        else:
            await query.edit_message_text(
                "❌ Неизвестное действие. Возвращаемся в главное меню.",
                reply_markup=create_main_menu_keyboard()
            )
            
    except APIClientError as e:
        logger.error(f"API error for user {user_id}: {e}")
        await query.edit_message_text(
            f"❌ Ошибка API: {str(e)}",
            reply_markup=create_error_keyboard()
        )
    except Exception as e:
        logger.error(f"Unexpected error for user {user_id}: {e}")
        await query.edit_message_text(
            "❌ Произошла неожиданная ошибка. Попробуйте позже.",
            reply_markup=create_error_keyboard()
        )


async def show_main_menu(query):
    """Показать главное меню"""
    from .start import start_command
    
    # Создаем фиктивный update для start_command
    class FakeUpdate:
        def __init__(self, query):
            self.effective_user = query.from_user
            self.message = query.message
    
    fake_update = FakeUpdate(query)
    await start_command(fake_update, None)


async def handle_segmentation(query, parts: list, user_id: int):
    """Обработка сегментации"""
    if len(parts) < 2:
        await show_segmentation_menu(query)
        return
    
    subaction = parts[1]
    
    if subaction == "main":
        await show_segmentation_menu(query)
    elif subaction == "snapshot":
        await show_segmentation_snapshot(query, user_id)


async def show_segmentation_menu(query):
    """Показать меню сегментации"""
    text = """
👥 <b>Сегментация пользователей</b>

Текущее распределение пользователей по кластерам:

• <b>📊 Снапшот</b> - актуальное состояние сегментации
    """
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=create_segmentation_keyboard()
    )


async def show_segmentation_snapshot(query, user_id: int):
    """Показать снапшот сегментации"""
    try:
        # Получаем данные сегментации
        data = await api_client.get_segments_distribution(user_id)
        
        if not data:
            await query.edit_message_text(
                "❌ Данные сегментации недоступны\n\n"
                "Возможно, сегментация еще не была выполнена.",
                reply_markup=create_back_to_main_keyboard()
            )
            return
        
        # Форматируем данные
        segments = data.get('segments', [])
        date_str = data.get('date', format_current_time())
        
        if not segments:
            await query.edit_message_text(
                "❌ Сегменты не найдены\n\n"
                "Возможно, сегментация еще не была выполнена.",
                reply_markup=create_back_to_main_keyboard()
            )
            return
        
        text = f"📊 <b>Снапшот сегментации</b>\n"
        text += f"📅 Дата: {date_str}\n\n"
        text += format_segments_table(segments)
        
        # Добавляем кнопки
        keyboard = [
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main")]
        ]
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
    except Exception as e:
        logger.error(f"Error showing segmentation snapshot: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки данных сегментации",
            reply_markup=create_error_keyboard()
        )




async def handle_probability(query, parts: list, user_id: int):
    """Обработка вероятности покупки"""
    if len(parts) < 2:
        await show_probability_menu(query, user_id)
        return
    
    subaction = parts[1]
    
    if subaction == "main":
        await show_probability_menu(query, user_id)
    elif subaction == "th":
        # Смена порога
        threshold = float(parts[2]) if len(parts) > 2 else 0.7
        await show_probability_table(query, user_id, threshold=threshold)
    elif subaction == "page":
        # Пагинация
        page = int(parts[2]) if len(parts) > 2 else 1
        threshold = float(parts[3]) if len(parts) > 3 else 0.7
        await show_probability_table(query, user_id, page=page, threshold=threshold)


async def show_probability_menu(query, user_id: int):
    """Показать меню вероятности покупки"""
    await show_probability_table(query, user_id)


async def show_probability_table(query, user_id: int, page: int = 1, threshold: float = 0.7):
    """Показать таблицу вероятности покупки"""
    try:
        config = get_config()
        # Получаем топ пользователей по вероятности покупки
        users_data = await api_client.get_top_purchase_probability_users(user_id, threshold, config.PAGE_SIZE)
        
        if not users_data:
            text = f"🛒 <b>Вероятность покупки (30д)</b>\n"
            text += f"Порог: <b>{format_percentage(threshold)}</b>\n\n"
            text += "❌ Пользователи с вероятностью покупки не найдены"
        else:
            text = f"🛒 <b>Вероятность покупки (30д)</b>\n"
            text += f"Порог: <b>{format_percentage(threshold)}</b>\n\n"
            text += format_probability_table(users_data, threshold)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=create_probability_keyboard(page, threshold)
        )
        
    except Exception as e:
        logger.error(f"Error showing probability table: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки данных вероятности покупки",
            reply_markup=create_error_keyboard()
        )





async def handle_user_actions(query, parts: list, user_id: int):
    """Обработка действий с пользователями"""
    if len(parts) < 2:
        await show_user_search(query)
        return
    
    subaction = parts[1]
    
    if subaction == "search":
        await show_user_search(query)


async def show_user_search(query):
    """Показать поиск пользователя"""
    text = """
🔎 <b>Поиск пользователя</b>

Введите ID пользователя в чат для получения детальной сводки.

Пример: 123

Или используйте команду: /user 123
    """
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=create_user_search_keyboard()
    )


