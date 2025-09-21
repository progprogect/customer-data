"""
User Handler
Обработчик для работы с пользователями и их данными
"""

from telegram import Update
from telegram.ext import ContextTypes
import logging
from typing import Dict, Any

from utils.api_client import api_client, APIClientError
from utils.formatters import format_user_summary, format_days_ago, format_percentage
from utils.keyboards import create_user_actions_keyboard, create_back_to_main_keyboard, create_error_keyboard

logger = logging.getLogger(__name__)


async def get_user_summary(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
    """Получение полной сводки пользователя"""
    user_id = update.effective_user.id
    
    try:
        # Получаем данные пользователя из разных источников
        user_data = await fetch_user_data(user_id, target_user_id)
        
        if not user_data:
            await update.message.reply_text(
                f"❌ Пользователь #{target_user_id} не найден",
                reply_markup=create_back_to_main_keyboard()
            )
            return
        
        # Форматируем сводку
        summary_text = format_user_summary(user_data)
        
        # Создаем клавиатуру с действиями
        reply_markup = create_user_actions_keyboard(target_user_id)
        
        await update.message.reply_text(
            summary_text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
        logger.info(f"User {user_id} viewed summary for user {target_user_id}")
        
    except APIClientError as e:
        logger.error(f"API error getting user summary: {e}")
        await update.message.reply_text(
            f"❌ Ошибка получения данных пользователя: {str(e)}",
            reply_markup=create_error_keyboard()
        )
    except Exception as e:
        logger.error(f"Error getting user summary: {e}")
        await update.message.reply_text(
            "❌ Произошла ошибка при получении данных пользователя",
            reply_markup=create_error_keyboard()
        )


async def fetch_user_data(user_id: int, target_user_id: int) -> Dict[str, Any]:
    """Получение всех данных пользователя из нового Telegram API"""
    try:
        # Используем новый API endpoint для получения всех данных сразу
        summary_data = await api_client.get_telegram_user_summary(user_id, target_user_id)
        
        if summary_data:
            # Преобразуем данные в нужный формат
            user_data = {
                "user_id": summary_data.get("user_id", target_user_id),
                "segment": summary_data.get("segment", "Новый"),
                "ltv_12m": summary_data.get("ltv_12m", 0),
                "last_order_days": summary_data.get("last_order_days"),
                "prob_purchase": summary_data.get("prob_purchase_30d", 0.5),
                "prob_churn": summary_data.get("prob_churn_60d", 0.3),
                "recommendations": summary_data.get("recommendations", []),
                "anomalies": summary_data.get("anomalies", [])
            }
            
            logger.info(f"Successfully fetched user summary for user {target_user_id} from Telegram API")
            return user_data
        else:
            logger.warning(f"No data returned from Telegram API for user {target_user_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error fetching user data from Telegram API for user {target_user_id}: {e}")
        return None
    


def analyze_user_segment(events: list) -> str:
    """Простой анализ сегмента пользователя на основе событий"""
    if not events:
        return "Новый"
    
    # Подсчитываем типы событий
    event_types = {}
    for event in events:
        event_type = event.get('event_type', '')
        event_types[event_type] = event_types.get(event_type, 0) + 1
    
    # Простая логика определения сегмента
    purchase_events = event_types.get('purchase', 0)
    view_events = event_types.get('view_product', 0)
    
    if purchase_events >= 10:
        return "VIP"
    elif purchase_events >= 5:
        return "Высокий"
    elif purchase_events >= 2:
        return "Средний"
    elif purchase_events >= 1:
        return "Низкий"
    elif view_events >= 10:
        return "Заинтересованный"
    else:
        return "Новый"


def analyze_user_segment_by_ltv(ltv: float) -> str:
    """Анализ сегмента пользователя на основе LTV"""
    if ltv >= 5000:
        return "VIP"
    elif ltv >= 2000:
        return "Высокий"
    elif ltv >= 500:
        return "Средний"
    elif ltv >= 100:
        return "Низкий"
    else:
        return "Новый"


def analyze_user_segment_by_features(frequency: int, aov: float, ltv: float) -> str:
    """Анализ сегмента пользователя на основе features"""
    # Комплексный анализ на основе частоты, AOV и LTV
    
    # VIP: высокая частота + высокий AOV + высокий LTV
    if frequency >= 8 and aov >= 500 and ltv >= 3000:
        return "VIP"
    
    # Высокий: средняя-высокая частота + средний-высокий AOV
    elif frequency >= 5 and aov >= 300:
        return "Высокий"
    
    # Средний: средняя частота или средний AOV
    elif frequency >= 3 or aov >= 200:
        return "Средний"
    
    # Низкий: низкая частота но есть активность
    elif frequency >= 1:
        return "Низкий"
    
    # Новый: нет активности
    else:
        return "Новый"




