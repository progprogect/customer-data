#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build Training Dataset for XGBoost
Скрипт для сборки единого обучающего датасета из фич и таргетов

Author: Customer Data Analytics Team
"""

import psycopg2
import pandas as pd
import logging
import sys
import os
from datetime import datetime
from typing import Dict, Any
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('build_training_dataset.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Параметры подключения к БД
DB_CONFIG = {
    'host': 'localhost',
    'dbname': 'customer_data',
    'user': 'mikitavalkunovich',
    'password': '',
    'port': 5432
}

def connect_to_db() -> psycopg2.extensions.connection:
    """
    Подключение к PostgreSQL базе данных
    
    Returns:
        psycopg2.connection: Объект подключения к БД
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✅ Успешное подключение к базе данных")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Ошибка подключения к базе данных: {e}")
        raise

def execute_sql_file(conn: psycopg2.extensions.connection, file_path: str) -> bool:
    """
    Выполнение SQL файла
    
    Args:
        conn: Подключение к БД
        file_path: Путь к SQL файлу
        
    Returns:
        bool: True если выполнено успешно
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()
        
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
            conn.commit()
            
        logger.info(f"✅ SQL файл {file_path} выполнен успешно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка выполнения SQL файла {file_path}: {e}")
        conn.rollback()
        return False

def get_dataset_sample(conn: psycopg2.extensions.connection, limit: int = 10) -> pd.DataFrame:
    """
    Получение образца данных из обучающего датасета
    
    Args:
        conn: Подключение к БД
        limit: Количество строк для выборки
        
    Returns:
        pd.DataFrame: Образец данных
    """
    query = f"""
    SELECT 
        user_id,
        snapshot_date,
        recency_days,
        frequency_90d,
        monetary_180d,
        aov_180d,
        orders_lifetime,
        revenue_lifetime,
        categories_unique,
        purchase_next_30d
    FROM ml_training_dataset 
    ORDER BY user_id, snapshot_date 
    LIMIT {limit}
    """
    
    return pd.read_sql_query(query, conn)

def get_feature_statistics(conn: psycopg2.extensions.connection) -> Dict[str, Any]:
    """
    Получение статистики по признакам
    
    Args:
        conn: Подключение к БД
        
    Returns:
        Dict: Статистика по признакам
    """
    query = """
    SELECT 
        -- Базовая статистика
        COUNT(*) as total_rows,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT snapshot_date) as unique_snapshots,
        
        -- Статистика по recency_days
        COUNT(recency_days) as recency_not_null,
        AVG(recency_days) as recency_mean,
        STDDEV(recency_days) as recency_std,
        MIN(recency_days) as recency_min,
        MAX(recency_days) as recency_max,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY recency_days) as recency_median,
        
        -- Статистика по frequency_90d
        AVG(frequency_90d) as frequency_mean,
        STDDEV(frequency_90d) as frequency_std,
        MIN(frequency_90d) as frequency_min,
        MAX(frequency_90d) as frequency_max,
        
        -- Статистика по monetary_180d
        AVG(monetary_180d) as monetary_mean,
        STDDEV(monetary_180d) as monetary_std,
        MIN(monetary_180d) as monetary_min,
        MAX(monetary_180d) as monetary_max,
        
        -- Статистика по orders_lifetime
        AVG(orders_lifetime) as orders_mean,
        STDDEV(orders_lifetime) as orders_std,
        MIN(orders_lifetime) as orders_min,
        MAX(orders_lifetime) as orders_max,
        
        -- Баланс классов
        COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END) as positive_class,
        COUNT(CASE WHEN purchase_next_30d = FALSE THEN 1 END) as negative_class
        
    FROM ml_training_dataset
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, result))
    
    return {}

def export_dataset_to_csv(conn: psycopg2.extensions.connection, output_path: str) -> bool:
    """
    Экспорт датасета в CSV файл
    
    Args:
        conn: Подключение к БД
        output_path: Путь к выходному CSV файлу
        
    Returns:
        bool: True если экспорт успешен
    """
    try:
        query = """
        SELECT 
            user_id,
            snapshot_date,
            recency_days,
            frequency_90d,
            monetary_180d,
            aov_180d,
            orders_lifetime,
            revenue_lifetime,
            categories_unique,
            purchase_next_30d::int as target
        FROM ml_training_dataset 
        ORDER BY user_id, snapshot_date
        """
        
        df = pd.read_sql_query(query, conn)
        df.to_csv(output_path, index=False)
        
        logger.info(f"✅ Датасет экспортирован в CSV: {output_path}")
        logger.info(f"📊 Размер экспортированного датасета: {len(df):,} строк, {len(df.columns)} столбцов")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка экспорта в CSV: {e}")
        return False

def main():
    """Главная функция"""
    logger.info("🚀 Запуск сборки обучающего датасета для XGBoost")
    
    # Путь к SQL файлам
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
    
    sql_files = [
        'create_training_dataset_table.sql',  # Создание таблицы датасета
        'populate_training_dataset.sql',      # Заполнение датасета
        'validate_training_dataset.sql'       # Валидация качества
    ]
    
    conn = None
    try:
        # Подключение к БД
        conn = connect_to_db()
        
        # Выполнение SQL файлов по порядку
        for sql_file in sql_files:
            file_path = os.path.join(sql_dir, sql_file)
            
            if not os.path.exists(file_path):
                logger.error(f"❌ SQL файл не найден: {file_path}")
                continue
                
            logger.info(f"🔄 Выполнение {sql_file}...")
            success = execute_sql_file(conn, file_path)
            
            if not success:
                logger.error(f"❌ Ошибка выполнения {sql_file}. Остановка.")
                return False
        
        # Получение статистики по признакам
        logger.info("📊 Сбор статистики по признакам...")
        feature_stats = get_feature_statistics(conn)
        
        # Получение образца данных
        logger.info("🔍 Получение образца данных...")
        sample_data = get_dataset_sample(conn, limit=5)
        
        # Экспорт в CSV
        csv_path = 'ml_training_dataset.csv'
        logger.info(f"💾 Экспорт датасета в CSV: {csv_path}")
        export_success = export_dataset_to_csv(conn, csv_path)
        
        # Сохранение метрик
        report = {
            'generation_time': datetime.now().isoformat(),
            'feature_statistics': feature_stats,
            'sample_data': sample_data.to_dict('records'),
            'csv_export_success': export_success,
            'csv_file_path': csv_path if export_success else None
        }
        
        with open('training_dataset_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str, ensure_ascii=False)
        
        # Финальный отчет
        logger.info("=" * 60)
        logger.info("🎉 СБОРКА ОБУЧАЮЩЕГО ДАТАСЕТА ЗАВЕРШЕНА!")
        logger.info("=" * 60)
        
        if feature_stats:
            logger.info(f"📊 ОСНОВНАЯ СТАТИСТИКА:")
            logger.info(f"  • Всего строк: {feature_stats.get('total_rows', 0):,}")
            logger.info(f"  • Уникальных пользователей: {feature_stats.get('unique_users', 0):,}")
            logger.info(f"  • Положительных примеров: {feature_stats.get('positive_class', 0):,}")
            logger.info(f"  • Отрицательных примеров: {feature_stats.get('negative_class', 0):,}")
            
            total = feature_stats.get('total_rows', 0)
            positive = feature_stats.get('positive_class', 0)
            if total > 0:
                positive_percent = (positive / total) * 100
                logger.info(f"  • Процент положительного класса: {positive_percent:.2f}%")
        
        logger.info(f"📁 Отчет сохранен в: training_dataset_report.json")
        if export_success:
            logger.info(f"📁 CSV файл сохранен в: {csv_path}")
        
        logger.info("✅ ДАТАСЕТ ГОТОВ К ОБУЧЕНИЮ МОДЕЛИ!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False
        
    finally:
        if conn:
            conn.close()
            logger.info("🔐 Соединение с БД закрыто")

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
