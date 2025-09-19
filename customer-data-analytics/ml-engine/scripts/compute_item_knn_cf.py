#!/usr/bin/env python3
"""
Item-kNN Collaborative Filtering Computation
Вычисление item-item CF похожести на основе ко-покупок

Author: Customer Data Analytics Team
"""

import os
import sys
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from scipy.sparse import csr_matrix, coo_matrix
from sklearn.metrics.pairwise import cosine_similarity
import json

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('item_knn_cf.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры алгоритма
CF_CONFIG = {
    'min_co_users': 5,           # минимум общих пользователей
    'min_item_purchases': 5,     # минимум покупок товара для участия в CF
    'top_k': 50,                 # топ-50 похожих товаров
    'min_sim_score': 0.01,       # минимальный порог сходства
    'similarity_metric': 'cosine' # cosine или jaccard
}

class ItemKNNCollaborativeFiltering:
    """Вычисление Item-kNN Collaborative Filtering"""
    
    def __init__(self, db_connection_string: str):
        self.db_conn_str = db_connection_string
        self.conn = None
        self.interactions_df = None
        self.user_item_matrix = None
        self.item_similarities = None
        
    def connect_db(self):
        """Подключение к БД"""
        try:
            self.conn = psycopg2.connect(self.db_conn_str)
            logger.info("✅ Connected to database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def load_interactions(self) -> pd.DataFrame:
        """Загрузка взаимодействий пользователь-товар"""
        logger.info("📊 Loading user-item interactions...")
        
        query = """
        SELECT 
            user_id,
            product_id,
            event_ts,
            amount
        FROM ml_interactions_implicit
        WHERE event_ts >= NOW() - INTERVAL '6 months'
        ORDER BY user_id, product_id, event_ts
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            interactions = pd.DataFrame(cur.fetchall())
        
        if interactions.empty:
            raise ValueError("No interactions found")
        
        logger.info(f"📈 Loaded {len(interactions)} interactions")
        logger.info(f"👥 Users: {interactions['user_id'].nunique()}")
        logger.info(f"🛍️ Products: {interactions['product_id'].nunique()}")
        
        self.interactions_df = interactions
        return interactions
    
    def filter_items_by_popularity(self, interactions: pd.DataFrame) -> pd.DataFrame:
        """Фильтрация товаров по минимальному количеству покупок"""
        logger.info(f"🔍 Filtering items with min {CF_CONFIG['min_item_purchases']} purchases...")
        
        # Подсчитываем уникальных покупателей для каждого товара
        item_user_counts = interactions.groupby('product_id')['user_id'].nunique()
        popular_items = item_user_counts[
            item_user_counts >= CF_CONFIG['min_item_purchases']
        ].index
        
        # Фильтруем взаимодействия
        filtered_interactions = interactions[
            interactions['product_id'].isin(popular_items)
        ]
        
        logger.info(f"📊 Kept {len(popular_items)} items out of {len(item_user_counts)}")
        logger.info(f"📊 Filtered interactions: {len(filtered_interactions)}")
        
        return filtered_interactions
    
    def create_user_item_matrix(self, interactions: pd.DataFrame) -> Tuple[csr_matrix, Dict, Dict]:
        """Создание разреженной user×item матрицы"""
        logger.info("🔢 Creating user×item matrix...")
        
        # Создаем маппинги для индексов
        unique_users = interactions['user_id'].unique()
        unique_items = interactions['product_id'].unique()
        
        user_to_idx = {user_id: idx for idx, user_id in enumerate(unique_users)}
        item_to_idx = {item_id: idx for idx, item_id in enumerate(unique_items)}
        idx_to_user = {idx: user_id for user_id, idx in user_to_idx.items()}
        idx_to_item = {idx: item_id for item_id, idx in item_to_idx.items()}
        
        # Агрегируем взаимодействия (бинарная матрица: 1 = покупал, 0 = не покупал)
        user_item_agg = interactions.groupby(['user_id', 'product_id']).size().reset_index(name='count')
        
        # Создаем координаты для sparse матрицы
        rows = [user_to_idx[user_id] for user_id in user_item_agg['user_id']]
        cols = [item_to_idx[item_id] for item_id in user_item_agg['product_id']]
        data = [1] * len(rows)  # бинарные значения
        
        # Создаем sparse матрицу
        matrix_shape = (len(unique_users), len(unique_items))
        user_item_matrix = csr_matrix((data, (rows, cols)), shape=matrix_shape)
        
        logger.info(f"📊 Matrix shape: {matrix_shape}")
        logger.info(f"📊 Matrix density: {user_item_matrix.nnz / (matrix_shape[0] * matrix_shape[1]) * 100:.3f}%")
        logger.info(f"📊 Non-zero entries: {user_item_matrix.nnz:,}")
        
        mappings = {
            'user_to_idx': user_to_idx,
            'item_to_idx': item_to_idx,
            'idx_to_user': idx_to_user,
            'idx_to_item': idx_to_item
        }
        
        self.user_item_matrix = user_item_matrix
        return user_item_matrix, mappings, {'users': len(unique_users), 'items': len(unique_items)}
    
    def compute_item_similarities(self, user_item_matrix: csr_matrix, mappings: Dict) -> List[Dict]:
        """Вычисление cosine similarity между товарами"""
        logger.info("🔄 Computing item-item cosine similarities...")
        
        # Транспонируем матрицу для получения item×user
        item_user_matrix = user_item_matrix.T
        
        n_items = item_user_matrix.shape[0]
        logger.info(f"📊 Computing similarities for {n_items} items...")
        
        # Вычисляем cosine similarity между товарами
        # Используем батчи для экономии памяти
        batch_size = 100
        all_similarities = []
        
        for i in range(0, n_items, batch_size):
            end_i = min(i + batch_size, n_items)
            batch_matrix = item_user_matrix[i:end_i]
            
            # Cosine similarity с остальными товарами
            similarities = cosine_similarity(batch_matrix, item_user_matrix)
            
            # Обрабатываем каждый товар в батче
            for batch_idx, global_idx in enumerate(range(i, end_i)):
                item_id = mappings['idx_to_item'][global_idx]
                item_similarities = similarities[batch_idx]
                
                # Получаем топ-K похожих (исключая сам товар)
                item_similarities[global_idx] = -1  # исключаем себя
                top_indices = np.argsort(item_similarities)[::-1]
                
                # Фильтруем по порогам и добавляем в результат
                count = 0
                for similar_idx in top_indices:
                    if count >= CF_CONFIG['top_k']:
                        break
                    
                    sim_score = item_similarities[similar_idx]
                    if sim_score < CF_CONFIG['min_sim_score']:
                        continue
                    
                    similar_item_id = mappings['idx_to_item'][similar_idx]
                    
                    # Вычисляем количество общих пользователей
                    item_users = set(user_item_matrix[:, global_idx].nonzero()[0])
                    similar_users = set(user_item_matrix[:, similar_idx].nonzero()[0])
                    co_users = len(item_users & similar_users)
                    
                    # Проверяем минимальный порог со-пользователей
                    if co_users < CF_CONFIG['min_co_users']:
                        continue
                    
                    all_similarities.append({
                        'product_id': int(item_id),
                        'similar_product_id': int(similar_item_id),
                        'sim_score': float(sim_score),
                        'co_users': int(co_users)
                    })
                    count += 1
            
            if (i // batch_size + 1) % 10 == 0:
                logger.info(f"⏳ Processed {end_i}/{n_items} items...")
        
        logger.info(f"✅ Generated {len(all_similarities)} similarity pairs")
        
        # Статистика качества
        if all_similarities:
            sim_scores = [s['sim_score'] for s in all_similarities]
            co_users_counts = [s['co_users'] for s in all_similarities]
            
            logger.info(f"📊 Similarity stats:")
            logger.info(f"   Avg similarity: {np.mean(sim_scores):.3f}")
            logger.info(f"   Min similarity: {np.min(sim_scores):.3f}")
            logger.info(f"   Max similarity: {np.max(sim_scores):.3f}")
            logger.info(f"   Avg co-users: {np.mean(co_users_counts):.1f}")
            logger.info(f"   Min co-users: {np.min(co_users_counts)}")
            logger.info(f"   Max co-users: {np.max(co_users_counts)}")
        
        self.item_similarities = all_similarities
        return all_similarities
    
    def save_similarities_to_db(self, similarities: List[Dict]):
        """Сохранение результатов в БД"""
        logger.info("💾 Saving CF similarities to database...")
        
        # Очищаем старые данные
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM ml_item_sim_cf WHERE algorithm = %s", 
                       (CF_CONFIG['similarity_metric'],))
            logger.info("🗑️ Cleared old CF similarity data")
        
        # Подготавливаем данные для batch insert
        insert_data = []
        for sim in similarities:
            insert_data.append((
                sim['product_id'],
                sim['similar_product_id'], 
                sim['sim_score'],
                sim['co_users'],
                f"item_knn_{CF_CONFIG['similarity_metric']}"
            ))
        
        # Batch insert
        insert_query = """
        INSERT INTO ml_item_sim_cf 
        (product_id, similar_product_id, sim_score, co_users, algorithm)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        with self.conn.cursor() as cur:
            cur.executemany(insert_query, insert_data)
            self.conn.commit()
        
        logger.info(f"✅ Saved {len(similarities)} CF similarity records to database")
    
    def validate_cf_quality(self) -> Dict:
        """Валидация качества CF результатов"""
        logger.info("🔍 Validating CF similarity quality...")
        
        validation_query = """
        SELECT 
            COUNT(DISTINCT product_id) as products_with_similarities,
            COUNT(*) as total_similarity_pairs,
            AVG(sim_score) as avg_similarity,
            MIN(sim_score) as min_similarity,
            MAX(sim_score) as max_similarity,
            AVG(co_users) as avg_co_users,
            MIN(co_users) as min_co_users,
            MAX(co_users) as max_co_users
        FROM ml_item_sim_cf
        WHERE algorithm = %s
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(validation_query, (f"item_knn_{CF_CONFIG['similarity_metric']}",))
            stats = cur.fetchone()
        
        # Проверяем покрытие товаров
        coverage_query = """
        WITH cf_products AS (
            SELECT DISTINCT product_id 
            FROM ml_item_sim_cf 
            WHERE algorithm = %s
        ),
        active_products AS (
            SELECT DISTINCT product_id 
            FROM ml_item_content_features 
            WHERE is_active = true
        )
        SELECT 
            COUNT(cf.product_id) as cf_products,
            COUNT(ap.product_id) as active_products,
            COUNT(cf.product_id) * 100.0 / COUNT(ap.product_id) as coverage_percent
        FROM active_products ap
        LEFT JOIN cf_products cf ON ap.product_id = cf.product_id
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(coverage_query, (f"item_knn_{CF_CONFIG['similarity_metric']}",))
            coverage = cur.fetchone()
        
        results = {
            'products_with_similarities': stats['products_with_similarities'],
            'total_similarity_pairs': stats['total_similarity_pairs'],
            'avg_similarity': float(stats['avg_similarity']),
            'similarity_range': [float(stats['min_similarity']), float(stats['max_similarity'])],
            'avg_co_users': float(stats['avg_co_users']),
            'co_users_range': [int(stats['min_co_users']), int(stats['max_co_users'])],
            'coverage_percent': float(coverage['coverage_percent']),
            'algorithm': f"item_knn_{CF_CONFIG['similarity_metric']}",
            'config': CF_CONFIG
        }
        
        logger.info("📊 CF Quality Report:")
        logger.info(f"   Products with similarities: {results['products_with_similarities']}")
        logger.info(f"   Coverage: {results['coverage_percent']:.1f}%")
        logger.info(f"   Avg similarity: {results['avg_similarity']:.3f}")
        logger.info(f"   Avg co-users: {results['avg_co_users']:.1f}")
        
        return results
    
    def run_item_knn_cf_pipeline(self):
        """Запуск полного пайплайна Item-kNN CF"""
        logger.info("🚀 Starting Item-kNN Collaborative Filtering pipeline...")
        start_time = datetime.now()
        
        try:
            # 1. Подключение к БД и загрузка данных
            self.connect_db()
            interactions = self.load_interactions()
            
            # 2. Фильтрация товаров по популярности
            filtered_interactions = self.filter_items_by_popularity(interactions)
            
            # 3. Создание user×item матрицы
            user_item_matrix, mappings, matrix_info = self.create_user_item_matrix(filtered_interactions)
            
            # 4. Вычисление item-item similarities
            similarities = self.compute_item_similarities(user_item_matrix, mappings)
            
            # 5. Сохранение в БД
            if similarities:
                self.save_similarities_to_db(similarities)
                
                # 6. Валидация качества
                quality_report = self.validate_cf_quality()
                
                # Финальная статистика
                end_time = datetime.now()
                duration = end_time - start_time
                
                logger.info("🎉 Item-kNN CF pipeline completed successfully!")
                logger.info(f"⏱️ Total duration: {duration}")
                logger.info(f"📊 Processed {matrix_info['items']} items, {matrix_info['users']} users")
                logger.info(f"📊 Generated {len(similarities)} similarity pairs")
                
                return quality_report
            else:
                raise ValueError("No similarities generated - check thresholds")
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Главная функция"""
    # Параметры подключения к БД
    db_connection = "postgresql://mikitavalkunovich@localhost:5432/customer_data"
    
    # Создаем и запускаем CF компьютер
    cf_computer = ItemKNNCollaborativeFiltering(db_connection)
    quality_report = cf_computer.run_item_knn_cf_pipeline()
    
    # Сохраняем отчет
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"item_knn_cf_quality_report_{timestamp}.json"
    
    with open(report_file, 'w') as f:
        json.dump(quality_report, f, indent=2)
    
    logger.info(f"💾 Quality report saved to {report_file}")


if __name__ == "__main__":
    main()
