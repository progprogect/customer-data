#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database service for API
Author: Customer Data Analytics Team
"""

import psycopg2
from psycopg2 import pool
from typing import Generator
import logging
import os

logger = logging.getLogger(__name__)

# Параметры подключения к БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'dbname': os.getenv('DB_NAME', 'customer_data'),
    'user': os.getenv('DB_USER', 'mikitavalkunovich'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# Пул подключений
connection_pool = None

def init_connection_pool():
    """Инициализация пула подключений"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **DB_CONFIG
        )
        logger.info("Database connection pool initialized")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        raise

def get_db_connection():
    """Получение подключения к БД из пула"""
    global connection_pool
    
    if connection_pool is None:
        init_connection_pool()
    
    try:
        conn = connection_pool.getconn()
        if conn is None:
            raise Exception("Failed to get connection from pool")
        return conn
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        raise

def return_db_connection(conn):
    """Возврат подключения в пул"""
    global connection_pool
    
    if connection_pool and conn:
        try:
            connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Failed to return connection to pool: {e}")

def close_connection_pool():
    """Закрытие пула подключений"""
    global connection_pool
    
    if connection_pool:
        try:
            connection_pool.closeall()
            logger.info("Database connection pool closed")
        except Exception as e:
            logger.error(f"Failed to close connection pool: {e}")

# Dependency для FastAPI
def get_db_connection_dependency():
    """Dependency для получения подключения к БД"""
    conn = get_db_connection()
    try:
        yield conn
    finally:
        return_db_connection(conn)
