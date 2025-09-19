#!/usr/bin/env python3
"""
Create Churn Training Dataset Script
Скрипт для создания тренировочного датасета churn prediction

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

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('churn_training_dataset.log')
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


def get_dataset_stats(conn: psycopg2.extensions.connection, table_name: str) -> dict:
    """Получение статистики датасета"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(*) FILTER (WHERE split_type = 'train') as train_records,
                    COUNT(*) FILTER (WHERE split_type = 'valid_test') as valid_test_records,
                    COUNT(*) FILTER (WHERE target = TRUE) as churn_count,
                    COUNT(*) FILTER (WHERE target = FALSE) as retention_count,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(DISTINCT snapshot_date) as unique_snapshots,
                    MIN(snapshot_date) as earliest_snapshot,
                    MAX(snapshot_date) as latest_snapshot,
                    COUNT(*) FILTER (WHERE recency_days IS NULL) as recency_nulls,
                    COUNT(*) FILTER (WHERE frequency_90d IS NULL) as frequency_nulls,
                    COUNT(*) FILTER (WHERE monetary_180d IS NULL) as monetary_nulls,
                    COUNT(*) FILTER (WHERE aov_180d IS NULL) as aov_nulls
                FROM {table_name}
            """)
            
            result = cursor.fetchone()
            return {
                'total_records': result[0],
                'train_records': result[1],
                'valid_test_records': result[2],
                'churn_count': result[3],
                'retention_count': result[4],
                'unique_users': result[5],
                'unique_snapshots': result[6],
                'earliest_snapshot': result[7],
                'latest_snapshot': result[8],
                'recency_nulls': result[9],
                'frequency_nulls': result[10],
                'monetary_nulls': result[11],
                'aov_nulls': result[12]
            }
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики {table_name}: {e}")
        return {}


def get_feature_correlations(conn: psycopg2.extensions.connection, table_name: str) -> dict:
    """Получение корреляций признаков с таргетом"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    CORR(COALESCE(recency_days, 999), target::int) as recency_corr,
                    CORR(frequency_90d, target::int) as frequency_corr,
                    CORR(monetary_180d, target::int) as monetary_corr,
                    CORR(COALESCE(aov_180d, 0), target::int) as aov_corr,
                    CORR(orders_lifetime, target::int) as orders_lifetime_corr,
                    CORR(revenue_lifetime, target::int) as revenue_lifetime_corr,
                    CORR(categories_unique, target::int) as categories_unique_corr
                FROM {table_name}
            """)
            
            result = cursor.fetchone()
            return {
                'recency_corr': result[0],
                'frequency_corr': result[1],
                'monetary_corr': result[2],
                'aov_corr': result[3],
                'orders_lifetime_corr': result[4],
                'revenue_lifetime_corr': result[5],
                'categories_unique_corr': result[6]
            }
    except Exception as e:
        logger.error(f"❌ Ошибка получения корреляций {table_name}: {e}")
        return {}


def main():
    """Главная функция"""
    logger.info("🚀 СОЗДАНИЕ ТРЕНИРОВОЧНОГО ДАТАСЕТА CHURN PREDICTION")
    logger.info("=" * 60)
    
    # Путь к SQL файлам
    sql_dir = Path(__file__).parent.parent / 'sql'
    
    # Последовательность выполнения
    sql_files = [
        'create_churn_training_table.sql',      # Создание таблицы
        'create_churn_training_dataset.sql',    # Заполнение датасета
        'analyze_churn_dataset.sql'             # Анализ качества
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
        
        # Проверяем зависимости
        required_tables = ['ml_user_features_daily_all', 'ml_labels_churn_60d']
        for table in required_tables:
            if not check_table_exists(conn, table):
                logger.error(f"❌ Таблица {table} не найдена!")
                logger.error("   Сначала выполните генерацию фич и лейблов")
                return False
            logger.info(f"✅ Таблица {table} найдена")
        
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
        stats = get_dataset_stats(conn, 'ml_training_dataset_churn')
        correlations = get_feature_correlations(conn, 'ml_training_dataset_churn')
        
        if stats:
            churn_rate = (stats['churn_count'] / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
            train_ratio = (stats['train_records'] / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
            null_rate = ((stats['recency_nulls'] + stats['frequency_nulls'] + stats['monetary_nulls'] + stats['aov_nulls']) / stats['total_records']) * 100 if stats['total_records'] > 0 else 0
            
            logger.info("🎯 РЕЗУЛЬТАТЫ СОЗДАНИЯ ДАТАСЕТА:")
            logger.info("=" * 50)
            logger.info(f"📋 Всего записей: {stats['total_records']:,}")
            logger.info(f"👥 Уникальных пользователей: {stats['unique_users']:,}")
            logger.info(f"📅 Уникальных снапшотов: {stats['unique_snapshots']}")
            logger.info(f"📅 Период: {stats['earliest_snapshot']} - {stats['latest_snapshot']}")
            logger.info(f"")
            logger.info(f"🎯 SPLIT СТАТИСТИКА:")
            logger.info(f"   Train: {stats['train_records']:,} ({train_ratio:.1f}%)")
            logger.info(f"   Valid/Test: {stats['valid_test_records']:,} ({100-train_ratio:.1f}%)")
            logger.info(f"")
            logger.info(f"💔 Churn случаев: {stats['churn_count']:,} ({churn_rate:.1f}%)")
            logger.info(f"💚 Retention случаев: {stats['retention_count']:,} ({100-churn_rate:.1f}%)")
            logger.info(f"")
            logger.info(f"❓ NULL значения:")
            logger.info(f"   Recency: {stats['recency_nulls']:,}")
            logger.info(f"   Frequency: {stats['frequency_nulls']:,}")
            logger.info(f"   Monetary: {stats['monetary_nulls']:,}")
            logger.info(f"   AOV: {stats['aov_nulls']:,}")
            logger.info(f"   Общий NULL rate: {null_rate:.1f}%")
            
            if correlations:
                logger.info(f"")
                logger.info(f"🔗 КОРРЕЛЯЦИИ С CHURN:")
                logger.info(f"   Recency: {correlations['recency_corr']:.4f}")
                logger.info(f"   Frequency: {correlations['frequency_corr']:.4f}")
                logger.info(f"   Monetary: {correlations['monetary_corr']:.4f}")
                logger.info(f"   AOV: {correlations['aov_corr']:.4f}")
                logger.info(f"   Orders Lifetime: {correlations['orders_lifetime_corr']:.4f}")
                logger.info(f"   Revenue Lifetime: {correlations['revenue_lifetime_corr']:.4f}")
                logger.info(f"   Categories Unique: {correlations['categories_unique_corr']:.4f}")
            
            logger.info("=" * 50)
            
            # Проверяем качество
            quality_issues = []
            if churn_rate < 15 or churn_rate > 50:
                quality_issues.append(f"Churn rate ({churn_rate:.1f}%) вне диапазона 15-50%")
            if train_ratio < 65 or train_ratio > 75:
                quality_issues.append(f"Train ratio ({train_ratio:.1f}%) вне диапазона 65-75%")
            if null_rate > 5:
                quality_issues.append(f"NULL rate ({null_rate:.1f}%) слишком высокий")
            
            if not quality_issues:
                logger.info("✅ ОТЛИЧНОЕ КАЧЕСТВО ДАННЫХ!")
            else:
                logger.warning("⚠️  ОБНАРУЖЕНЫ ПРОБЛЕМЫ:")
                for issue in quality_issues:
                    logger.warning(f"   - {issue}")
        
        logger.info("🎉 ТРЕНИРОВОЧНЫЙ ДАТАСЕТ CHURN PREDICTION СОЗДАН УСПЕШНО!")
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
