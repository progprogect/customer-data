#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Load RFM data from PostgreSQL for K-means clustering
Author: Customer Data Analytics Team
"""

import psycopg2
import pandas as pd
import sys
import os
from typing import Optional, Tuple
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Параметры подключения к БД
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'customer_data',
    'user': 'mikitavalkunovich',  # текущий пользователь системы
    'password': '',  # пустой пароль для локального подключения
    'port': 5432
}

def connect_to_db() -> Optional[psycopg2.extensions.connection]:
    """
    Подключение к PostgreSQL базе данных
    
    Returns:
        psycopg2.connection: Объект подключения к БД или None при ошибке
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Успешное подключение к базе данных")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None

def load_rfm_data(conn: psycopg2.extensions.connection) -> Optional[pd.DataFrame]:
    """
    Загрузка RFM данных за последний день из таблицы ml_user_features_daily_buyers
    
    Args:
        conn: Подключение к БД
        
    Returns:
        pd.DataFrame: DataFrame с RFM-признаками или None при ошибке
    """
    sql = """
    WITH last_snap AS (
        SELECT MAX(snapshot_date) AS snap 
        FROM ml_user_features_daily_buyers
    )
    SELECT
        user_id,
        recency_days,
        frequency_90d,
        monetary_180d,
        aov_180d,
        orders_lifetime,
        revenue_lifetime,
        categories_unique
    FROM ml_user_features_daily_buyers, last_snap
    WHERE snapshot_date = snap
    ORDER BY user_id;
    """
    
    try:
        df = pd.read_sql(sql, conn)
        logger.info(f"Загружено {len(df)} записей из таблицы ml_user_features_daily_buyers")
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        return None

def validate_data(df: pd.DataFrame) -> bool:
    """
    Валидация загруженных данных
    
    Args:
        df: DataFrame с данными
        
    Returns:
        bool: True если данные валидны, False иначе
    """
    if df is None or df.empty:
        logger.error("DataFrame пустой или None")
        return False
    
    # Проверка наличия необходимых колонок
    required_columns = [
        'user_id', 'recency_days', 'frequency_90d', 'monetary_180d',
        'aov_180d', 'orders_lifetime', 'revenue_lifetime', 'categories_unique'
    ]
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Отсутствуют колонки: {missing_columns}")
        return False
    
    # Проверка на NULL значения в ключевых полях
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"Найдены NULL значения:\n{null_counts[null_counts > 0]}")
    
    # Проверка типов данных
    numeric_columns = ['recency_days', 'frequency_90d', 'monetary_180d', 
                      'aov_180d', 'orders_lifetime', 'revenue_lifetime', 'categories_unique']
    
    for col in numeric_columns:
        if not pd.api.types.is_numeric_dtype(df[col]):
            logger.warning(f"Колонка {col} не является числовой")
    
    logger.info("Валидация данных прошла успешно")
    return True

def print_data_summary(df: pd.DataFrame) -> None:
    """
    Вывод сводной информации о данных
    
    Args:
        df: DataFrame с данными
    """
    print("\n" + "="*60)
    print("СВОДНАЯ ИНФОРМАЦИЯ О RFM ДАННЫХ")
    print("="*60)
    
    print(f"Общее количество записей: {len(df)}")
    print(f"Количество уникальных пользователей: {df['user_id'].nunique()}")
    
    print("\nСтатистика по RFM-признакам:")
    print("-" * 40)
    
    # Основные RFM метрики
    rfm_metrics = ['recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d']
    
    for metric in rfm_metrics:
        if metric in df.columns:
            print(f"{metric:20s}: min={df[metric].min():8.2f}, "
                  f"max={df[metric].max():8.2f}, "
                  f"mean={df[metric].mean():8.2f}")
    
    print("\nДополнительные метрики:")
    print("-" * 40)
    
    additional_metrics = ['orders_lifetime', 'revenue_lifetime', 'categories_unique']
    for metric in additional_metrics:
        if metric in df.columns:
            print(f"{metric:20s}: min={df[metric].min():8.2f}, "
                  f"max={df[metric].max():8.2f}, "
                  f"mean={df[metric].mean():8.2f}")
    
    print("\nПервые 5 записей:")
    print("-" * 40)
    print(df.head())
    
    print("\nИнформация о типах данных:")
    print("-" * 40)
    print(df.dtypes)

def main():
    """
    Основная функция для загрузки и проверки RFM данных
    """
    logger.info("Начало загрузки RFM данных для K-means кластеризации")
    
    # Подключение к БД
    conn = connect_to_db()
    if conn is None:
        logger.error("Не удалось подключиться к базе данных")
        sys.exit(1)
    
    try:
        # Загрузка данных
        df = load_rfm_data(conn)
        if df is None:
            logger.error("Не удалось загрузить данные")
            sys.exit(1)
        
        # Валидация данных
        if not validate_data(df):
            logger.error("Данные не прошли валидацию")
            sys.exit(1)
        
        # Вывод сводной информации
        print_data_summary(df)
        
        logger.info("Загрузка RFM данных завершена успешно")
        
        return df
        
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        sys.exit(1)
    
    finally:
        # Закрытие подключения
        if conn:
            conn.close()
            logger.info("Подключение к базе данных закрыто")

if __name__ == "__main__":
    df = main()
