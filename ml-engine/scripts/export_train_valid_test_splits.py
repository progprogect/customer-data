#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export Train/Valid/Test Splits to CSV
Экспорт временных сплитов в отдельные CSV файлы для ML обучения

Author: Customer Data Analytics Team
"""

import psycopg2
import pandas as pd
import logging
import sys
import os
from datetime import datetime
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('export_splits.log', encoding='utf-8')
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

def export_split_to_csv(conn: psycopg2.extensions.connection, split_name: str) -> dict:
    """
    Экспорт конкретного сплита в CSV
    
    Args:
        conn: Подключение к БД
        split_name: Название сплита ('train', 'valid', 'test')
        
    Returns:
        dict: Статистика экспорта
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
        purchase_next_30d::int as target
    FROM ml_training_dataset 
    WHERE split = '{split_name}'
    ORDER BY user_id, snapshot_date
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # Имя файла
        filename = f'{split_name}_set.csv'
        df.to_csv(filename, index=False)
        
        # Статистика
        stats = {
            'split': split_name,
            'filename': filename,
            'rows': len(df),
            'columns': len(df.columns),
            'positive_class': int(df['target'].sum()),
            'negative_class': int(len(df) - df['target'].sum()),
            'positive_rate': round(df['target'].mean() * 100, 2),
            'unique_users': df['user_id'].nunique(),
            'unique_dates': df['snapshot_date'].nunique(),
            'date_range': {
                'start': str(df['snapshot_date'].min()),
                'end': str(df['snapshot_date'].max())
            },
            'feature_stats': {
                'recency_mean': round(df['recency_days'].mean(), 2) if df['recency_days'].notna().any() else None,
                'frequency_mean': round(df['frequency_90d'].mean(), 2),
                'monetary_mean': round(df['monetary_180d'].mean(), 2)
            }
        }
        
        logger.info(f"✅ {split_name.upper()} set экспортирован: {filename}")
        logger.info(f"   📊 Размер: {len(df):,} строк, {len(df.columns)} столбцов")
        logger.info(f"   🎯 Positive rate: {stats['positive_rate']}%")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ Ошибка экспорта {split_name} set: {e}")
        return {}

def main():
    """Главная функция"""
    logger.info("🚀 Запуск экспорта временных сплитов в CSV")
    
    conn = None
    try:
        # Подключение к БД
        conn = connect_to_db()
        
        # Экспорт каждого сплита
        splits = ['train', 'valid', 'test']
        all_stats = {}
        
        for split in splits:
            logger.info(f"🔄 Экспорт {split} set...")
            stats = export_split_to_csv(conn, split)
            if stats:
                all_stats[split] = stats
        
        # Общая статистика
        total_rows = sum(stats['rows'] for stats in all_stats.values())
        
        logger.info("=" * 60)
        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА ЭКСПОРТА:")
        logger.info("=" * 60)
        
        for split_name, stats in all_stats.items():
            logger.info(f"{split_name.upper()}:")
            logger.info(f"  • Файл: {stats['filename']}")
            logger.info(f"  • Строк: {stats['rows']:,} ({stats['rows']/total_rows*100:.1f}%)")
            logger.info(f"  • Positive rate: {stats['positive_rate']}%")
            logger.info(f"  • Период: {stats['date_range']['start']} — {stats['date_range']['end']}")
            logger.info(f"  • Пользователей: {stats['unique_users']:,}")
            
        # Сохранение полной статистики в JSON
        export_report = {
            'export_time': datetime.now().isoformat(),
            'total_rows': total_rows,
            'splits': all_stats,
            'files_created': [stats['filename'] for stats in all_stats.values()]
        }
        
        with open('splits_export_report.json', 'w', encoding='utf-8') as f:
            json.dump(export_report, f, indent=2, default=str, ensure_ascii=False)
        
        logger.info(f"📁 Экспортировано файлов: {len(all_stats)}")
        logger.info(f"📁 Отчет сохранен: splits_export_report.json")
        logger.info("✅ ЭКСПОРТ ЗАВЕРШЕН УСПЕШНО!")
        
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
