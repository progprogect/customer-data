"""
Products Handler
Обработчик команд товаров
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging
import httpx

logger = logging.getLogger(__name__)

API_BASE_URL = "http://localhost:8000"


async def products_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /products"""
    await products_callback(update, context)


async def products_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback'ов товаров"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "products_main":
        await show_products_main(query)
    elif query.data == "products_list":
        await show_products_list(query)
    elif query.data == "products_categories":
        await show_categories(query)
    elif query.data == "products_popular":
        await show_popular_products(query)
    elif query.data == "products_search":
        await show_product_search(query)
    elif query.data == "products_back":
        await show_products_main(query)


async def show_products_main(query):
    """Показать главное меню товаров"""
    keyboard = [
        [
            InlineKeyboardButton("📦 Список товаров", callback_data="products_list"),
            InlineKeyboardButton("🏷️ Категории", callback_data="products_categories")
        ],
        [
            InlineKeyboardButton("🔥 Популярные", callback_data="products_popular"),
            InlineKeyboardButton("🔍 Поиск", callback_data="products_search")
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data="start")
        ]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = """
📦 <b>Товары</b>

Управление каталогом товаров и их аналитика.

<b>Функции:</b>
• <b>📦 Список товаров</b> - просмотр каталога товаров
• <b>🏷️ Категории</b> - просмотр категорий товаров
• <b>🔥 Популярные</b> - самые популярные товары
• <b>🔍 Поиск</b> - поиск товаров по названию
    """
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )


async def show_products_list(query):
    """Показать список товаров"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/api/v1/products/?limit=10")
            products = response.json()
        
        if not products:
            text = "📦 <b>Список товаров</b>\n\nТовары не найдены."
        else:
            text = "📦 <b>Список товаров</b>\n\n"
            for product in products[:10]:  # Показываем только первых 10
                text += f"• <b>{product.get('title', 'N/A')}</b>\n"
                text += f"  🏷️ {product.get('category', 'N/A')} | 💰 ${product.get('price', 0):.2f}\n"
                text += f"  🏪 {product.get('brand', 'N/A')}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="products_list")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error fetching products list: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки списка товаров",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
            ])
        )


async def show_categories(query):
    """Показать категории товаров"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/api/v1/products/categories/")
            categories = response.json()
        
        if not categories:
            text = "🏷️ <b>Категории товаров</b>\n\nКатегории не найдены."
        else:
            text = "🏷️ <b>Категории товаров</b>\n\n"
            for category in categories:
                text += f"• {category}\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="products_categories")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error fetching categories: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки категорий",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
            ])
        )


async def show_popular_products(query):
    """Показать популярные товары"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_BASE_URL}/api/v1/products/popular/?limit=10")
            products = response.json()
        
        if not products:
            text = "🔥 <b>Популярные товары</b>\n\nПопулярные товары не найдены."
        else:
            text = "🔥 <b>Популярные товары</b>\n\n"
            for i, product in enumerate(products, 1):
                text += f"{i}. <b>{product.get('title', 'N/A')}</b>\n"
                text += f"   💰 ${product.get('price', 0):.2f} | 🏪 {product.get('brand', 'N/A')}\n\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="products_popular")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(
            text,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        logger.error(f"Error fetching popular products: {e}")
        await query.edit_message_text(
            "❌ Ошибка загрузки популярных товаров",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
            ])
        )


async def show_product_search(query):
    """Показать поиск товаров"""
    text = """
🔍 <b>Поиск товаров</b>

Для поиска товаров введи поисковый запрос.

<b>Формат:</b> <code>search:запрос</code>

<b>Примеры:</b>
• <code>search:iphone</code> - поиск iPhone
• <code>search:кроссовки</code> - поиск кроссовок
• <code>search:nike</code> - поиск товаров Nike

<b>Поиск работает по:</b>
• Названию товара
• Описанию
• Бренду
• Категории
    """
    
    keyboard = [
        [InlineKeyboardButton("⬅️ Назад", callback_data="products_main")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

