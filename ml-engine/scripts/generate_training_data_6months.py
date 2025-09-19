#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Training Data for XGBoost (6 Months Weekly)
Скрипт для генерации данных обучения XGBoost с еженедельными снапшотами за 6 месяцев

Author: Customer Data Analytics Team
"""

import psycopg2
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
        logging.FileHandler('generate_training_data.log', encoding='utf-8')
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

def validate_generated_data(conn: psycopg2.extensions.connection) -> Dict[str, Any]:
    """
    Валидация сгенерированных данных
    
    Args:
        conn: Подключение к БД
        
    Returns:
        Dict с метриками валидации
    """
    validation_metrics = {}
    
    try:
        with conn.cursor() as cursor:
            # Проверяем витрину фич
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_features,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT snapshot_date) as unique_snapshots,
                    MIN(snapshot_date) as min_date,
                    MAX(snapshot_date) as max_date
                FROM ml_user_features_daily_all
            """)
            features_stats = cursor.fetchone()
            
            validation_metrics['features'] = {
                'total_rows': features_stats[0],
                'unique_users': features_stats[1],
                'unique_snapshots': features_stats[2],
                'min_date': features_stats[3],
                'max_date': features_stats[4]
            }
            
            # Проверяем таргеты
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_labels,
                    COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END) as positive_class,
                    COUNT(CASE WHEN purchase_next_30d = FALSE THEN 1 END) as negative_class,
                    ROUND(
                        COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END)::NUMERIC / 
                        COUNT(*)::NUMERIC * 100, 2
                    ) as positive_class_percent,
                    MIN(snapshot_date) as min_date,
                    MAX(snapshot_date) as max_date
                FROM ml_labels_purchase_30d
            """)
            labels_stats = cursor.fetchone()
            
            validation_metrics['labels'] = {
                'total_rows': labels_stats[0],
                'positive_class': labels_stats[1],
                'negative_class': labels_stats[2],
                'positive_class_percent': float(labels_stats[3]) if labels_stats[3] else 0,
                'min_date': labels_stats[4],
                'max_date': labels_stats[5]
            }
            
            # Проверяем соответствие фич и лейблов
            cursor.execute("""
                SELECT COUNT(*) 
                FROM ml_user_features_daily_all f
                INNER JOIN ml_labels_purchase_30d l 
                    ON f.user_id = l.user_id 
                    AND f.snapshot_date = l.snapshot_date
            """)
            matching_rows = cursor.fetchone()[0]
            validation_metrics['matching_rows'] = matching_rows
            
    except Exception as e:
        logger.error(f"❌ Ошибка валидации данных: {e}")
        validation_metrics['error'] = str(e)
    
    return validation_metrics

def main():
    """Главная функция"""
    logger.info("🚀 Запуск генерации данных обучения для XGBoost (6 месяцев, еженедельно)")
    
    # Путь к SQL файлам
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
    
    sql_files = [
        'create_target_labels.sql',                    # Создание таблицы таргетов
        'compute_ml_user_features_6months_weekly_fixed.sql', # Генерация фич за 6 месяцев (исправленная)
        'populate_buyers_features.sql',               # Заполнение таблицы покупателей
        'log_features_stats.sql',                     # Логирование статистики фич
        'generate_target_labels_6months_fixed.sql',   # Генерация таргетов (исправленная)
        'log_target_stats.sql'                        # Логирование статистики таргетов
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
        
        # Валидация сгенерированных данных
        logger.info("🔍 Валидация сгенерированных данных...")
        validation_metrics = validate_generated_data(conn)
        
        # Вывод результатов валидации
        logger.info("📊 РЕЗУЛЬТАТЫ ВАЛИДАЦИИ:")
        logger.info("=" * 50)
        
        if 'features' in validation_metrics:
            f = validation_metrics['features']
            logger.info(f"🔧 ВИТРИНА ФИЧ:")
            logger.info(f"  • Всего строк: {f['total_rows']:,}")
            logger.info(f"  • Уникальных пользователей: {f['unique_users']:,}")
            logger.info(f"  • Уникальных снапшотов: {f['unique_snapshots']}")
            logger.info(f"  • Период: {f['min_date']} — {f['max_date']}")
        
        if 'labels' in validation_metrics:
            l = validation_metrics['labels']
            logger.info(f"🎯 ТАРГЕТЫ:")
            logger.info(f"  • Всего строк: {l['total_rows']:,}")
            logger.info(f"  • Положительный класс: {l['positive_class']:,}")
            logger.info(f"  • Отрицательный класс: {l['negative_class']:,}")
            logger.info(f"  • Процент положительного класса: {l['positive_class_percent']:.2f}%")
            logger.info(f"  • Период: {l['min_date']} — {l['max_date']}")
        
        if 'matching_rows' in validation_metrics:
            logger.info(f"🔗 СООТВЕТСТВИЕ:")
            logger.info(f"  • Строк с совпадающими фичами и таргетами: {validation_metrics['matching_rows']:,}")
        
        # Проверки качества данных
        logger.info("✅ ПРОВЕРКИ КАЧЕСТВА:")
        
        if 'labels' in validation_metrics:
            percent = validation_metrics['labels']['positive_class_percent']
            if 5 <= percent <= 30:
                logger.info(f"  ✅ Процент положительного класса в норме: {percent:.2f}%")
            else:
                logger.warning(f"  ⚠️ Процент положительного класса вне нормы (5-30%): {percent:.2f}%")
        
        if 'features' in validation_metrics and 'labels' in validation_metrics:
            if validation_metrics['features']['total_rows'] == validation_metrics['labels']['total_rows']:
                logger.info("  ✅ Количество строк в фичах и таргетах совпадает")
            else:
                logger.warning("  ⚠️ Количество строк в фичах и таргетах не совпадает")
        
        # Сохранение метрик в файл
        with open('training_data_metrics.json', 'w', encoding='utf-8') as f:
            json.dump(validation_metrics, f, indent=2, default=str, ensure_ascii=False)
        
        logger.info("🎉 Генерация данных обучения завершена успешно!")
        logger.info("📁 Метрики сохранены в training_data_metrics.json")
        
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
