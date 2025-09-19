"""
Analytics Handler
Обработчик команд аналитики
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging
import httpx

logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8000"


async def analytics_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /analytics"""
    await analytics_callback(update, context)


async def analytics_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback'ов аналитики"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "analytics_main":
        await show_analytics_main(query)
    elif query.data == "analytics_dashboard":
        await show_dashboard(query)
    elif query.data == "analytics_segmentation":
        await show_segmentation(query)
    elif query.data == "analytics_churn":
        await show_churn_prediction(query)
    elif query.data == "analytics_anomalies":
        await show_anomaly_detection(query)
    elif query.data == "analytics_back":
        await show_analytics_main(query)


async def show_analytics_main(query):
    """Показать главное меню аналитики"""
    keyboard = [
        [
            InlineKeyboardButton("📈 Дашборд", callback_data="analytics_dashboard"),
            InlineKeyboardButton("👥 Сегментация", callback_data="analytics_segmentation")
        ],
        [
            InlineKeyboardButton("⚠️ Предсказание оттока", callback_data="analytics_churn"),
            InlineKeyboardButton("🔍 Детекция аномалий", callback_data="analytics_anomalies")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="start")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = """
📊 <b>Аналитика</b>

Выбери тип анализа:

• <b>📈 Дашборд</b> - основные метрики и графики
• <b>👥 Сегментация</b> - группировка пользователей по поведению
• <b>⚠️ Предсказание оттока</b> - анализ риска потери клиентов
• <b>🔍 Детекция аномалий</b> - поиск необычных паттернов
    """
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_dashboard(query):
    """Показать дашборд"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/api/v1/analytics/dashboard")
            data = response.json()
        
        text = f"""
📈 <b>Дашборд</b>

<b>Основные метрики:</b>
• 👥 Всего пользователей: {data.get('total_users', 'N/A')}
• 📦 Всего заказов: {data.get('total_orders', 'N/A')}
• 💰 Общая выручка: ${data.get('total_revenue', 0):,.2f}
• 🟢 Активные пользователи: {data.get('active_users', 'N/A')}
• 📈 Конверсия: {data.get('conversion_rate', 0) * 100:.1f}%
• 💳 Средний чек: ${data.get('avg_order_value', 0):.2f}
        """
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="analytics_dashboard")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="analytics_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error fetching dashboard data: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки данных дашборда",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="analytics_main")]
            ])
        )


async def show_segmentation(query):
    """Показать сегментацию пользователей"""
    text = """
👥 <b>Сегментация пользователей</b>

Для запуска сегментации выбери параметры:

<b>Алгоритмы:</b>
• K-Means - классическая кластеризация
• DBSCAN - кластеризация по плотности

<b>Количество кластеров:</b> 3-10 (для K-Means)
    """
    
    keyboard = [
        [
            InlineKeyboardButton("🔵 K-Means (5 кластеров)", callback_data="analytics_segmentation_kmeans_5"),
            InlineKeyboardButton("🔵 K-Means (3 кластера)", callback_data="analytics_segmentation_kmeans_3")
        ],
        [
            InlineKeyboardButton("🟢 DBSCAN", callback_data="analytics_segmentation_dbscan")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="analytics_main")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_churn_prediction(query):
    """Показать предсказание оттока"""
    text = """
⚠️ <b>Предсказание оттока клиентов</b>

Анализ риска потери клиентов на основе их поведения и характеристик.

<b>Функции:</b>
• Анализ пользователей с высоким риском оттока
• Рекомендации по удержанию клиентов
• Мониторинг изменений в поведении
    """
    
    keyboard = [
        [
            InlineKeyboardButton("🔍 Анализ рисков", callback_data="analytics_churn_analyze"),
            InlineKeyboardButton("📊 Статистика", callback_data="analytics_churn_stats")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="analytics_main")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_anomaly_detection(query):
    """Показать детекцию аномалий"""
    text = """
🔍 <b>Детекция аномалий</b>

Поиск необычных паттернов в поведении пользователей и данных.

<b>Типы аномалий:</b>
• Необычные покупки
• Подозрительная активность
• Аномальные паттерны просмотров
• Нестандартное использование системы
    """
    
    keyboard = [
        [
            InlineKeyboardButton("🔍 Найти аномалии", callback_data="analytics_anomalies_detect"),
            InlineKeyboardButton("📊 Отчет", callback_data="analytics_anomalies_report")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="analytics_main")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )
