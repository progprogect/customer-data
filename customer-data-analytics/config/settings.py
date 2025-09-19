"""
Global Settings
Глобальные настройки проекта
"""

import os
from pathlib import Path
from pydantic import BaseSettings
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Корневая директория проекта
PROJECT_ROOT = Path(__file__).parent.parent


class Settings(BaseSettings):
    """Глобальные настройки проекта"""
    
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://mikitavalkunovich@localhost:5432/customer_data")
    
    # API
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_URL: str = os.getenv("API_URL", "http://localhost:8000")
    
    # Frontend
    FRONTEND_URL: str = os.getenv("FRONTEND_URL", "http://localhost:3000")
    
    # Telegram Bot
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_DIR: str = os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs"))
    
    # ML Engine
    ML_MODELS_DIR: str = os.getenv("ML_MODELS_DIR", str(PROJECT_ROOT / "ml-engine" / "models"))
    ML_DATA_DIR: str = os.getenv("ML_DATA_DIR", str(PROJECT_ROOT / "ml-engine" / "data"))
    
    # Redis (для кеширования)
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Глобальный экземпляр настроек
settings = Settings()


def get_settings() -> Settings:
    """Получение настроек"""
    return settings
