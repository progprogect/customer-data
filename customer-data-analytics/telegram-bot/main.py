"""
Telegram Marketing Assistant Main
Главный файл Telegram Marketing Assistant
"""

import asyncio
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
from dotenv import load_dotenv
import os

from handlers.start import start_command, help_command, user_command, today_command
from handlers.callback_handler import handle_callback
from utils.keyboards import create_user_search_keyboard, create_main_menu_keyboard
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
application.add_handler(CommandHandler("help", help_command))
application.add_handler(CommandHandler("user", user_command))
application.add_handler(CommandHandler("today", today_command))

# Добавляем обработчик всех callback'ов
application.add_handler(CallbackQueryHandler(handle_callback))

# Добавляем обработчик текстовых сообщений для поиска пользователей
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений для поиска пользователей"""
    user_id = update.effective_user.id
    message_text = update.message.text.strip()
    
    # Проверяем, является ли сообщение числом (ID пользователя)
    try:
        target_user_id = int(message_text)
        # Импортируем и вызываем функцию поиска пользователя
        from handlers.user_handler import get_user_summary
        await get_user_summary(update, context, target_user_id)
    except ValueError:
        await update.message.reply_text(
            "❌ Неверный формат ID пользователя\n\n"
            "ID должен быть числом. Пример: 123\n\n"
            "Или используйте команды:\n"
            "/start - главное меню\n"
            "/help - справка\n"
            "/user 123 - поиск пользователя",
            reply_markup=create_main_menu_keyboard()
        )

application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))


async def main():
    """Главная функция"""
    logging.info("🚀 Запуск Telegram Marketing Assistant...")
    logging.info(f"📊 API URL: {config.API_URL}")
    logging.info(f"🔑 Rate limit: {config.RATE_LIMIT_SECONDS}s")
    logging.info(f"📄 Page size: {config.PAGE_SIZE}")
    
    # Запускаем бота
    try:
        await application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True
        )
    except Exception as e:
        logging.error(f"❌ Ошибка запуска бота: {e}")
        raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot остановлен пользователем")
    except Exception as e:
        print(f"❌ Ошибка запуска бота: {e}")

