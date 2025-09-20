"""
Logging Middleware
Промежуточное ПО для логирования
"""

from loguru import logger
import sys


def setup_logging():
    """Настройка логирования"""
    # Удаляем стандартный обработчик
    logger.remove()
    
    # Добавляем консольный вывод
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO"
    )
    
    # Добавляем файловый вывод
    logger.add(
        "logs/api.log",
        rotation="1 day",
        retention="30 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG"
    )

