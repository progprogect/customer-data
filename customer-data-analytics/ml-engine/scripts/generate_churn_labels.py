#!/usr/bin/env python3
"""
Generate Churn Labels Script
Скрипт для генерации churn лейблов с горизонтом 60 дней

Author: Customer Data Analytics Team
"""

import os
import sys
import psycopg2
import logging
from datetime import datetime
from pathlib import Path

# Добавляем путь к корневой директории проекта
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import psycopg2
from psycopg2.extras import RealDictCursor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('churn_labels_generation.log')
    ]
)
logger = logging.getLogger(__name__)


def execute_sql_file(conn: psycopg2.extensions.connection, file_path: Path) -> bool:
    """Выполнение SQL файла"""
    try:
        logger.info(f"🔄 Выполнение {file_path.name}...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
            conn.commit()
            
        logger.info(f"✅ {file_path.name} выполнен успешно")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка выполнения {file_path.name}: {e}")
        conn.rollback()
        return False


def check_table_exists(conn: psycopg2.extensions.connection, table_name: str) -> bool:
    """Проверка существования таблицы"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            return cursor.fetchone()[0]
    except Exception as e:
        logger.error(f"❌ Ошибка проверки таблицы {table_name}: {e}")
        return False


def get_table_stats(conn: psycopg2.extensions.connection, table_name: str) -> dict:
    """Получение статистики таблицы"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE is_churn_next_60d = TRUE) as churn_count,
                    COUNT(*) FILTER (WHERE is_churn_next_60d = FALSE) as retention_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT snapshot_date) as unique_snapshots,
                    MIN(snapshot_date) as earliest_snapshot,
                    MAX(snapshot_date) as latest_snapshot
                FROM {table_name}
            """)
            
            result = cursor.fetchone()
            return {
                'total_records': result[0],
                'churn_count': result[1],
                'retention_count': result[2],
                'unique_users': result[3],
                'unique_snapshots': result[4],
                'earliest_snapshot': result[5],
                'latest_snapshot': result[6]
            }
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики {table_name}: {e}")
        return {}


def main():
    """Главная функция"""
    logger.info("🚀 ЗАПУСК ГЕНЕРАЦИИ CHURN ЛЕЙБЛОВ")
    logger.info("=" * 60)
    
    # Путь к SQL файлам
    sql_dir = Path(__file__).parent.parent / 'sql'
    
    # Последовательность выполнения
    sql_files = [
        'create_churn_table.sql',      # Создание таблицы
        'create_churn_labels_60d.sql', # Генерация лейблов
        'validate_churn_labels.sql'    # Валидация качества
    ]
    
    conn = None
    try:
        # Подключение к БД
        logger.info("🔌 Подключение к базе данных...")
        conn = psycopg2.connect(
            host="localhost",
            database="customer_data",
            user="mikitavalkunovich",
            port="5432"
        )
        logger.info("✅ Подключение установлено")
        
        # Проверяем существование витрины фич
        if not check_table_exists(conn, 'ml_user_features_daily_all'):
            logger.error("❌ Таблица ml_user_features_daily_all не найдена!")
            logger.error("   Сначала выполните генерацию витрины фич")
            return False
        
        logger.info("✅ Витрина фич найдена")
        
        # Выполнение SQL файлов по порядку
        for i, sql_file in enumerate(sql_files, 1):
            file_path = sql_dir / sql_file
            
            if not file_path.exists():
                logger.error(f"❌ SQL файл не найден: {file_path}")
                continue
                
            logger.info(f"📋 Шаг {i}/{len(sql_files)}: {sql_file}")
            success = execute_sql_file(conn, file_path)
            
            if not success:
                logger.error(f"❌ Критическая ошибка в {sql_file}. Остановка.")
                return False
        
        # Получаем финальную статистику
        logger.info("📊 Получение финальной статистики...")
        stats = get_table_stats(conn, 'ml_labels_churn_60d')
        
        if stats:
            churn_rate = (stats['churn_count'] / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
            
            logger.info("🎯 РЕЗУЛЬТАТЫ ГЕНЕРАЦИИ:")
            logger.info("=" * 40)
            logger.info(f"📋 Всего записей: {stats['total_records']:,}")
            logger.info(f"👥 Уникальных пользователей: {stats['unique_users']:,}")
            logger.info(f"📅 Уникальных снапшотов: {stats['unique_snapshots']}")
            logger.info(f"📅 Период: {stats['earliest_snapshot']} - {stats['latest_snapshot']}")
            logger.info(f"💔 Churn случаев: {stats['churn_count']:,} ({churn_rate:.1f}%)")
            logger.info(f"💚 Retention случаев: {stats['retention_count']:,} ({100-churn_rate:.1f}%)")
            logger.info("=" * 40)
            
            # Проверяем баланс классов
            if 20 <= churn_rate <= 40:
                logger.info("✅ Churn rate в ожидаемом диапазоне (20-40%)")
            else:
                logger.warning(f"⚠️  Churn rate ({churn_rate:.1f}%) вне ожидаемого диапазона")
        
        logger.info("🎉 ГЕНЕРАЦИЯ CHURN ЛЕЙБЛОВ ЗАВЕРШЕНА УСПЕШНО!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False
        
    finally:
        if conn:
            conn.close()
            logger.info("🔌 Соединение с БД закрыто")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
