"""
Database Service
Сервис для подключения к базе данных
"""

import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Настройки подключения к БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'customer_data'),
    'user': os.getenv('DB_USER', 'mikitavalkunovich'),
    'password': os.getenv('DB_PASSWORD', ''),
}

@contextmanager
def get_db_connection():
    """Получение подключения к БД"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database connection error: {e}")
        raise
    finally:
        if conn:
            conn.close()
