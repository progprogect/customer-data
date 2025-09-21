"""
Configuration
Конфигурация Telegram Marketing Assistant
"""

import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()


class Settings(BaseSettings):
    """Настройки Telegram Marketing Assistant"""
    
    # Telegram Bot
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "8218093650:AAFYQVpXavlm6JuPPumx95IZhilQr1ZuoPw")
    
    # API
    API_URL: str = os.getenv("API_URL", "http://localhost:8000")
    API_KEY: str = os.getenv("API_KEY", "dev-token-12345")  # API ключ для ML endpoints
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://mikitavalkunovich@localhost:5432/customer_data")
    
    # Bot Settings
    RATE_LIMIT_SECONDS: int = int(os.getenv("RATE_LIMIT_SECONDS", "2"))  # Rate limiting
    PAGE_SIZE: int = int(os.getenv("PAGE_SIZE", "10"))  # Записей на страницу
    
    # Default Thresholds
    DEFAULT_PROBABILITY_THRESHOLD: float = float(os.getenv("DEFAULT_PROBABILITY_THRESHOLD", "0.7"))
    DEFAULT_CHURN_THRESHOLD: float = float(os.getenv("DEFAULT_CHURN_THRESHOLD", "0.6"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/marketing_bot.log")
    
    class Config:
        env_file = ".env"


def get_config() -> Settings:
    """Получение конфигурации"""
    return Settings()
