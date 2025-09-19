"""
Configuration
Конфигурация Telegram бота
"""

import os
from pydantic import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Настройки приложения"""
    
    # Telegram Bot
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    
    # API
    API_URL: str = os.getenv("API_URL", "http://localhost:8000")
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://mikitavalkunovich@localhost:5432/customer_data")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/bot.log")
    
    class Config:
        env_file = ".env"


def get_config() -> Settings:
    """Получение конфигурации"""
    return Settings()
