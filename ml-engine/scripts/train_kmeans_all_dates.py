#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train K-means model for all available dates and save results to database
Author: Customer Data Analytics Team
"""

import numpy as np
import pandas as pd
import psycopg2
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import logging
from typing import Tuple, Dict, List
import sys
import os
import json
from datetime import date, datetime

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
        logger.info("Успешное подключение к базе данных")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

def get_available_dates(conn: psycopg2.extensions.connection) -> List[date]:
    """Получение всех доступных дат из ml_user_features_daily_buyers"""
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT snapshot_date 
            FROM ml_user_features_daily_buyers 
            ORDER BY snapshot_date
        """)
        dates = [row[0] for row in cur.fetchall()]
        cur.close()
        logger.info(f"Найдено {len(dates)} доступных дат для кластеризации")
        return dates
    except psycopg2.Error as e:
        logger.error(f"Ошибка при получении дат: {e}")
        raise

def load_rfm_data_for_date(conn: psycopg2.extensions.connection, target_date: date) -> Tuple[pd.DataFrame, pd.Series]:
    """Загрузка RFM данных для конкретной даты"""
    sql = """
    SELECT
        user_id,
        recency_days,
        frequency_90d,
        monetary_180d,
        aov_180d,
        orders_lifetime,
        revenue_lifetime,
        categories_unique
    FROM ml_user_features_daily_buyers
    WHERE snapshot_date = %s
    ORDER BY user_id;
    """
    
    try:
        df = pd.read_sql(sql, conn, params=(target_date,))
        user_ids = df["user_id"]
        
        logger.info(f"Загружено {len(df)} записей для даты {target_date}")
        return df, user_ids
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных для {target_date}: {e}")
        raise

def preprocess_data(df: pd.DataFrame) -> Tuple[np.ndarray, StandardScaler]:
    """Предобработка данных для кластеризации"""
    # Выбираем признаки
    features = ["recency_days", "frequency_90d", "monetary_180d", "aov_180d", "categories_unique"]
    X = df[features].copy()
    
    # Лог-трансформация для денежных признаков
    for col in ["monetary_180d", "aov_180d"]:
        X[col] = np.log1p(X[col])
    
    # Стандартизация
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    return X_scaled, scaler

def train_kmeans_model(X_scaled: np.ndarray, k: int = 3) -> Tuple[KMeans, np.ndarray]:
    """Обучение K-means модели"""
    kmeans = KMeans(
        n_clusters=k,
        random_state=42,
        n_init=10,
        max_iter=300
    )
    
    labels = kmeans.fit_predict(X_scaled)
    return kmeans, labels

def save_clustering_results(conn: psycopg2.extensions.connection, 
                          user_ids: pd.Series, 
                          cluster_labels: np.ndarray,
                          snapshot_date: date) -> None:
    """Сохранение результатов кластеризации в БД"""
    logger.info(f"Сохранение результатов кластеризации для {len(user_ids)} пользователей на дату {snapshot_date}")
    
    try:
        cur = conn.cursor()
        
        # Подготовка данных для вставки
        data_to_insert = []
        for user_id, cluster_id in zip(user_ids, cluster_labels):
            data_to_insert.append((int(user_id), snapshot_date, int(cluster_id)))
        
        # Вставка данных с обработкой конфликтов
        insert_sql = """
        INSERT INTO user_segments_kmeans (user_id, snapshot_date, cluster_id)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id, snapshot_date) DO UPDATE SET
            cluster_id = EXCLUDED.cluster_id,
            created_at = NOW();
        """
        
        cur.executemany(insert_sql, data_to_insert)
        conn.commit()
        
        # Проверка сохраненных данных
        cur.execute("""
            SELECT cluster_id, COUNT(*) as count 
            FROM user_segments_kmeans 
            WHERE snapshot_date = %s 
            GROUP BY cluster_id 
            ORDER BY cluster_id
        """, (snapshot_date,))
        
        cluster_counts = cur.fetchall()
        
        logger.info(f"Результаты для {snapshot_date} сохранены в БД:")
        for cluster_id, count in cluster_counts:
            logger.info(f"  Кластер {cluster_id}: {count} пользователей")
        
        cur.close()
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка при сохранении результатов для {snapshot_date}: {e}")
        raise

def create_segments_table(conn: psycopg2.extensions.connection) -> None:
    """Создание таблицы user_segments_kmeans"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS user_segments_kmeans (
        user_id BIGINT NOT NULL,
        snapshot_date DATE NOT NULL,
        cluster_id INT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (user_id, snapshot_date)
    );
    
    -- Индексы для оптимизации запросов
    CREATE INDEX IF NOT EXISTS idx_user_segments_user_id ON user_segments_kmeans(user_id);
    CREATE INDEX IF NOT EXISTS idx_user_segments_cluster ON user_segments_kmeans(cluster_id);
    CREATE INDEX IF NOT EXISTS idx_user_segments_date ON user_segments_kmeans(snapshot_date);
    
    -- Комментарии к таблице
    COMMENT ON TABLE user_segments_kmeans IS 'Результаты K-means кластеризации пользователей';
    COMMENT ON COLUMN user_segments_kmeans.user_id IS 'ID пользователя';
    COMMENT ON COLUMN user_segments_kmeans.snapshot_date IS 'Дата снапшота кластеризации';
    COMMENT ON COLUMN user_segments_kmeans.cluster_id IS 'ID кластера (0, 1, 2, ...)';
    """
    
    try:
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        logger.info("Таблица user_segments_kmeans создана успешно")
    except psycopg2.Error as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

def main():
    """Основная функция для обучения K-means для всех дат"""
    logger.info("Начало обучения K-means для всех доступных дат")
    
    # Подключение к БД
    conn = connect_to_db()
    
    try:
        # Создание таблицы
        create_segments_table(conn)
        
        # Получение доступных дат
        available_dates = get_available_dates(conn)
        
        if not available_dates:
            logger.error("Нет доступных дат для кластеризации")
            return
        
        logger.info(f"Обработка {len(available_dates)} дат...")
        
        # Обработка каждой даты
        for i, target_date in enumerate(available_dates, 1):
            logger.info(f"Обработка даты {i}/{len(available_dates)}: {target_date}")
            
            try:
                # Загрузка данных
                df, user_ids = load_rfm_data_for_date(conn, target_date)
                
                if df.empty:
                    logger.warning(f"Нет данных для даты {target_date}, пропускаем")
                    continue
                
                # Предобработка
                X_scaled, scaler = preprocess_data(df)
                
                # Обучение модели
                kmeans, cluster_labels = train_kmeans_model(X_scaled, k=3)
                
                # Сохранение результатов
                save_clustering_results(conn, user_ids, cluster_labels, target_date)
                
                logger.info(f"✅ Дата {target_date} обработана успешно")
                
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке даты {target_date}: {e}")
                continue
        
        # Финальная проверка
        cur = conn.cursor()
        cur.execute("""
            SELECT snapshot_date, COUNT(*) as users_count 
            FROM user_segments_kmeans 
            GROUP BY snapshot_date 
            ORDER BY snapshot_date
        """)
        
        results = cur.fetchall()
        cur.close()
        
        logger.info("Финальная статистика:")
        for snapshot_date, users_count in results:
            logger.info(f"  {snapshot_date}: {users_count} пользователей")
        
        logger.info("🎉 Обучение K-means для всех дат завершено успешно!")
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Подключение к базе данных закрыто")

if __name__ == "__main__":
    main()

