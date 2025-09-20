"""
Telegram Bot Main
Главный файл Telegram бота
"""

import asyncio
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler
from dotenv import load_dotenv
import os

from handlers.start import start_command
from handlers.analytics import analytics_command, analytics_callback
from handlers.recommendations import recommendations_command, recommendations_callback
from handlers.users import users_command, users_callback
from handlers.products import products_command, products_callback
from utils.logging import setup_logging
from utils.config import get_config

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
setup_logging()

# Получаем конфигурацию
config = get_config()

# Создаем приложение
application = Application.builder().token(config.BOT_TOKEN).build()

# Добавляем обработчики команд
application.add_handler(CommandHandler("start", start_command))
application.add_handler(CommandHandler("help", start_command))
application.add_handler(CommandHandler("analytics", analytics_command))
application.add_handler(CommandHandler("recommendations", recommendations_command))
application.add_handler(CommandHandler("users", users_command))
application.add_handler(CommandHandler("products", products_command))

# Добавляем обработчики callback'ов
application.add_handler(CallbackQueryHandler(analytics_callback, pattern="^analytics_"))
application.add_handler(CallbackQueryHandler(recommendations_callback, pattern="^recommendations_"))
application.add_handler(CallbackQueryHandler(users_callback, pattern="^users_"))
application.add_handler(CallbackQueryHandler(products_callback, pattern="^products_"))

# Добавляем обработчик текстовых сообщений
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda update, context: update.message.reply_text("Используйте команды для навигации по боту")))


async def main():
    """Главная функция"""
    logging.info("Запуск Telegram бота...")
    
    # Запускаем бота
    await application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True
    )


if __name__ == "__main__":
    asyncio.run(main())

