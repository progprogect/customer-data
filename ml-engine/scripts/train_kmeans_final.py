#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Train final K-means model and save results to database
Author: Customer Data Analytics Team
"""

import numpy as np
import pandas as pd
import psycopg2
from sklearn.cluster import KMeans
import logging
from typing import Tuple, Dict
import sys
import os
import json
from datetime import date

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from prepare_data_for_clustering import main as prepare_data

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
    """
    Подключение к PostgreSQL базе данных
    
    Returns:
        psycopg2.connection: Объект подключения к БД
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Успешное подключение к базе данных")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        raise

def create_segments_table(conn: psycopg2.extensions.connection) -> None:
    """
    Создание таблицы user_segments_kmeans
    
    Args:
        conn: Подключение к БД
    """
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

def train_kmeans_model(X_scaled: np.ndarray, k: int = 3) -> Tuple[KMeans, np.ndarray]:
    """
    Обучение финальной модели K-means
    
    Args:
        X_scaled: Масштабированные данные
        k: Количество кластеров
        
    Returns:
        Tuple[KMeans, np.ndarray]: (обученная модель, метки кластеров)
    """
    logger.info(f"Обучение K-means модели с k = {k}")
    
    kmeans = KMeans(
        n_clusters=k,
        random_state=42,
        n_init=10,
        max_iter=300
    )
    
    labels = kmeans.fit_predict(X_scaled)
    
    logger.info(f"Модель обучена успешно")
    logger.info(f"Количество кластеров: {k}")
    logger.info(f"Размер данных: {X_scaled.shape}")
    logger.info(f"Inertia: {kmeans.inertia_:.2f}")
    
    return kmeans, labels

def save_clustering_results(conn: psycopg2.extensions.connection, 
                          user_ids: pd.Series, 
                          cluster_labels: np.ndarray,
                          snapshot_date: date = None) -> None:
    """
    Сохранение результатов кластеризации в БД
    
    Args:
        conn: Подключение к БД
        user_ids: Идентификаторы пользователей
        cluster_labels: Метки кластеров
        snapshot_date: Дата снапшота (по умолчанию сегодня)
    """
    if snapshot_date is None:
        snapshot_date = date.today()
    
    logger.info(f"Сохранение результатов кластеризации для {len(user_ids)} пользователей")
    
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
        
        logger.info("Результаты сохранены в БД:")
        for cluster_id, count in cluster_counts:
            logger.info(f"  Кластер {cluster_id}: {count} пользователей")
        
        cur.close()
        
    except psycopg2.Error as e:
        logger.error(f"Ошибка при сохранении результатов: {e}")
        raise

def analyze_clusters(conn: psycopg2.extensions.connection, 
                    snapshot_date: date = None) -> pd.DataFrame:
    """
    Анализ характеристик кластеров по исходным RFM-признакам
    
    Args:
        conn: Подключение к БД
        snapshot_date: Дата снапшота
        
    Returns:
        pd.DataFrame: Анализ кластеров
    """
    if snapshot_date is None:
        snapshot_date = date.today()
    
    logger.info("Анализ характеристик кластеров")
    
    analysis_sql = """
    WITH cluster_analysis AS (
        SELECT 
            s.cluster_id,
            COUNT(*) as users_count,
            AVG(u.recency_days) as avg_recency_days,
            AVG(u.frequency_90d) as avg_frequency_90d,
            AVG(u.monetary_180d) as avg_monetary_180d,
            AVG(u.aov_180d) as avg_aov_180d,
            AVG(u.orders_lifetime) as avg_orders_lifetime,
            AVG(u.revenue_lifetime) as avg_revenue_lifetime,
            AVG(u.categories_unique) as avg_categories_unique,
            MIN(u.recency_days) as min_recency_days,
            MAX(u.recency_days) as max_recency_days,
            MIN(u.monetary_180d) as min_monetary_180d,
            MAX(u.monetary_180d) as max_monetary_180d
        FROM user_segments_kmeans s
        JOIN ml_user_features_daily_buyers u ON u.user_id = s.user_id 
            AND u.snapshot_date = s.snapshot_date
        WHERE s.snapshot_date = %s
        GROUP BY s.cluster_id
        ORDER BY s.cluster_id
    )
    SELECT * FROM cluster_analysis;
    """
    
    try:
        df_analysis = pd.read_sql(analysis_sql, conn, params=(snapshot_date,))
        logger.info(f"Анализ кластеров выполнен для {len(df_analysis)} кластеров")
        return df_analysis
    except Exception as e:
        logger.error(f"Ошибка при анализе кластеров: {e}")
        raise

def interpret_clusters(df_analysis: pd.DataFrame) -> Dict[int, str]:
    """
    Интерпретация кластеров на основе их характеристик
    
    Args:
        df_analysis: DataFrame с анализом кластеров
        
    Returns:
        Dict[int, str]: Словарь cluster_id -> интерпретация
    """
    interpretations = {}
    
    for _, row in df_analysis.iterrows():
        cluster_id = int(row['cluster_id'])
        recency = row['avg_recency_days']
        frequency = row['avg_frequency_90d']
        monetary = row['avg_monetary_180d']
        aov = row['avg_aov_180d']
        categories = row['avg_categories_unique']
        
        # Логика интерпретации на основе RFM-анализа
        if recency <= 30 and frequency >= 4 and monetary >= 2000:
            interpretation = "VIP / Лояльные клиенты"
        elif recency <= 60 and frequency >= 2 and monetary >= 500:
            interpretation = "Активные клиенты"
        elif recency <= 120 and frequency >= 1 and monetary >= 100:
            interpretation = "Обычные клиенты"
        elif recency > 120 and frequency < 2 and monetary < 200:
            interpretation = "Спящие клиенты"
        elif monetary >= 1000:
            interpretation = "Высокоценные клиенты"
        elif frequency >= 3:
            interpretation = "Частые покупатели"
        else:
            interpretation = "Низкоактивные клиенты"
        
        interpretations[cluster_id] = interpretation
    
    return interpretations

def print_cluster_analysis(df_analysis: pd.DataFrame, interpretations: Dict[int, str]) -> None:
    """
    Вывод детального анализа кластеров
    
    Args:
        df_analysis: DataFrame с анализом кластеров
        interpretations: Словарь с интерпретациями
    """
    print("\n" + "="*80)
    print("АНАЛИЗ КЛАСТЕРОВ ПОЛЬЗОВАТЕЛЕЙ")
    print("="*80)
    
    for _, row in df_analysis.iterrows():
        cluster_id = int(row['cluster_id'])
        interpretation = interpretations.get(cluster_id, "Не определено")
        
        print(f"\n🔹 КЛАСТЕР {cluster_id}: {interpretation}")
        print("-" * 60)
        print(f"Количество пользователей: {int(row['users_count'])}")
        print(f"Процент от общего числа: {row['users_count']/df_analysis['users_count'].sum()*100:.1f}%")
        
        print(f"\n📊 RFM-характеристики:")
        print(f"  Recency (дни с последней покупки): {row['avg_recency_days']:.1f} (диапазон: {row['min_recency_days']:.0f}-{row['max_recency_days']:.0f})")
        print(f"  Frequency (заказы за 90 дней): {row['avg_frequency_90d']:.1f}")
        print(f"  Monetary (сумма за 180 дней): ${row['avg_monetary_180d']:.2f} (диапазон: ${row['min_monetary_180d']:.2f}-${row['max_monetary_180d']:.2f})")
        print(f"  AOV (средний чек): ${row['avg_aov_180d']:.2f}")
        print(f"  Заказов за всё время: {row['avg_orders_lifetime']:.1f}")
        print(f"  Выручка за всё время: ${row['avg_revenue_lifetime']:.2f}")
        print(f"  Уникальных категорий: {row['avg_categories_unique']:.1f}")
        
        # Рекомендации по сегменту
        print(f"\n💡 Рекомендации:")
        if "VIP" in interpretation or "Лояльные" in interpretation:
            print("  - Персональные предложения и эксклюзивные акции")
            print("  - Программа лояльности с повышенными бонусами")
            print("  - Приоритетная поддержка")
        elif "Активные" in interpretation:
            print("  - Регулярные email-рассылки с новинками")
            print("  - Программа лояльности")
            print("  - Кросс-продажи и апселлы")
        elif "Обычные" in interpretation:
            print("  - Стандартные маркетинговые кампании")
            print("  - Скидки и промо-акции")
            print("  - Мотивация к увеличению частоты покупок")
        elif "Спящие" in interpretation:
            print("  - Реактивационные кампании")
            print("  - Специальные предложения для возврата")
            print("  - Анализ причин ухода")
        elif "Высокоценные" in interpretation:
            print("  - Премиум-продукты и услуги")
            print("  - Персональные менеджеры")
            print("  - Эксклюзивные предложения")
        elif "Частые" in interpretation:
            print("  - Программа подписок")
            print("  - Автоматические заказы")
            print("  - Бонусы за регулярность")

def save_analysis_results(df_analysis: pd.DataFrame, interpretations: Dict[int, str], 
                         snapshot_date: date = None) -> None:
    """
    Сохранение результатов анализа в файлы
    
    Args:
        df_analysis: DataFrame с анализом кластеров
        interpretations: Словарь с интерпретациями
        snapshot_date: Дата снапшота
    """
    if snapshot_date is None:
        snapshot_date = date.today()
    
    # Сохранение в CSV
    csv_path = f'ml-engine/results/cluster_analysis_{snapshot_date}.csv'
    df_analysis.to_csv(csv_path, index=False)
    logger.info(f"Анализ кластеров сохранен в {csv_path}")
    
    # Сохранение интерпретаций в JSON
    json_path = f'ml-engine/results/cluster_interpretations_{snapshot_date}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(interpretations, f, indent=2, ensure_ascii=False)
    logger.info(f"Интерпретации кластеров сохранены в {json_path}")

def main():
    """
    Основная функция для обучения K-means и анализа кластеров
    """
    logger.info("Начало обучения финальной модели K-means")
    
    # Подготовка данных
    X_scaled, user_ids, scaler = prepare_data()
    
    # Подключение к БД
    conn = connect_to_db()
    
    try:
        # Создание таблицы
        create_segments_table(conn)
        
        # Обучение модели
        kmeans, cluster_labels = train_kmeans_model(X_scaled, k=3)
        
        # Сохранение результатов
        save_clustering_results(conn, user_ids, cluster_labels)
        
        # Анализ кластеров
        df_analysis = analyze_clusters(conn)
        
        # Интерпретация кластеров
        interpretations = interpret_clusters(df_analysis)
        
        # Вывод результатов
        print_cluster_analysis(df_analysis, interpretations)
        
        # Сохранение результатов анализа
        save_analysis_results(df_analysis, interpretations)
        
        logger.info("Обучение K-means и анализ кластеров завершены успешно")
        
        return kmeans, cluster_labels, df_analysis, interpretations
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Подключение к базе данных закрыто")

if __name__ == "__main__":
    kmeans, labels, analysis, interpretations = main()
