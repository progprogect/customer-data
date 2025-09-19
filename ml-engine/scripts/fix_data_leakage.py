#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fix Data Leakage in Training Dataset
Скрипт для устранения утечек данных и регенерации чистого датасета

Author: Customer Data Analytics Team
"""

import psycopg2
import logging
import sys
import os
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fix_data_leakage.log', encoding='utf-8')
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
    """Подключение к PostgreSQL базе данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("✅ Успешное подключение к базе данных")
        return conn
    except psycopg2.Error as e:
        logger.error(f"❌ Ошибка подключения к базе данных: {e}")
        raise

def execute_sql_file(conn: psycopg2.extensions.connection, file_path: str) -> bool:
    """Выполнение SQL файла"""
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

def check_leakage_before(conn: psycopg2.extensions.connection):
    """Проверка утечек ДО исправления"""
    logger.info("🔍 Проверка утечек данных ДО исправления...")
    
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT 
                MIN(recency_days) as min_recency,
                MAX(recency_days) as max_recency,
                COUNT(CASE WHEN recency_days < 0 THEN 1 END) as negative_count,
                COUNT(*) as total
            FROM ml_training_dataset
        """)
        result = cursor.fetchone()
        
        if result:
            min_recency, max_recency, negative_count, total = result
            logger.warning(f"📊 СТАТИСТИКА ДО ИСПРАВЛЕНИЯ:")
            logger.warning(f"  • Минимальный recency: {min_recency}")
            logger.warning(f"  • Максимальный recency: {max_recency}")
            logger.warning(f"  • Отрицательных recency: {negative_count:,} из {total:,} ({negative_count/total*100:.1f}%)")
            
            if negative_count > 0:
                logger.error(f"🚨 ОБНАРУЖЕНА УТЕЧКА: {negative_count:,} строк с отрицательным recency!")
                return False
            else:
                logger.info("✅ Утечки не обнаружены")
                return True

def main():
    """Главная функция"""
    logger.info("🚨 ЗАПУСК ИСПРАВЛЕНИЯ УТЕЧЕК ДАННЫХ")
    logger.info("=" * 60)
    
    # Путь к SQL файлам
    sql_dir = os.path.join(os.path.dirname(__file__), '..', 'sql')
    
    # Последовательность исправления
    sql_files = [
        'compute_ml_user_features_no_leakage.sql',     # Пересчет фич без утечек
        'populate_buyers_features_no_leakage.sql',     # Заполнение покупателей без утечек
        'validate_no_data_leakage.sql'                 # Валидация отсутствия утечек
    ]
    
    conn = None
    try:
        # Подключение к БД
        conn = connect_to_db()
        
        # Проверка утечек ДО исправления
        leakage_detected = not check_leakage_before(conn)
        
        if not leakage_detected:
            logger.info("✅ Утечки не обнаружены. Исправление не требуется.")
            return True
        
        logger.info("🛠️ Начинаем исправление утечек...")
        
        # Выполнение SQL файлов по порядку
        for i, sql_file in enumerate(sql_files, 1):
            file_path = os.path.join(sql_dir, sql_file)
            
            if not os.path.exists(file_path):
                logger.error(f"❌ SQL файл не найден: {file_path}")
                continue
                
            logger.info(f"🔄 Шаг {i}/{len(sql_files)}: Выполнение {sql_file}...")
            success = execute_sql_file(conn, file_path)
            
            if not success:
                logger.error(f"❌ Ошибка выполнения {sql_file}. Остановка.")
                return False
        
        # Пересборка обучающего датасета
        logger.info("🔄 Пересборка обучающего датасета...")
        
        # Очистка старой таблицы
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE ml_training_dataset;")
            conn.commit()
        
        # Перезаполнение из исправленных фич
        rebuild_sql = os.path.join(sql_dir, 'populate_training_dataset.sql')
        if os.path.exists(rebuild_sql):
            success = execute_sql_file(conn, rebuild_sql)
            if not success:
                logger.error("❌ Ошибка пересборки обучающего датасета")
                return False
        
        # Финальная проверка
        logger.info("🔍 Финальная проверка отсутствия утечек...")
        
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    MIN(recency_days) as min_recency,
                    MAX(recency_days) as max_recency,
                    COUNT(CASE WHEN recency_days < 0 THEN 1 END) as negative_count,
                    COUNT(*) as total
                FROM ml_training_dataset
            """)
            result = cursor.fetchone()
            
            if result:
                min_recency, max_recency, negative_count, total = result
                logger.info(f"📊 СТАТИСТИКА ПОСЛЕ ИСПРАВЛЕНИЯ:")
                logger.info(f"  • Минимальный recency: {min_recency}")
                logger.info(f"  • Максимальный recency: {max_recency}")
                logger.info(f"  • Отрицательных recency: {negative_count:,} из {total:,}")
                logger.info(f"  • Всего строк: {total:,}")
                
                if negative_count == 0:
                    logger.info("🎉 УТЕЧКИ УСТРАНЕНЫ УСПЕШНО!")
                    logger.info("✅ ДАТАСЕТ ГОТОВ К БЕЗОПАСНОМУ ОБУЧЕНИЮ!")
                    return True
                else:
                    logger.error(f"🚨 УТЕЧКИ ВСЕ ЕЩЕ ПРИСУТСТВУЮТ: {negative_count:,} строк")
                    return False
        
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
