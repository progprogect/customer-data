#!/usr/bin/env python3
"""
Collaborative Filtering Recommendations Evaluation
Быстрая оценка качества CF рекомендаций vs baselines

Author: Customer Data Analytics Team
"""

import os
import sys
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры оценки
EVALUATION_CONFIG = {
    'holdout_days': 7,          # последние 7 дней как holdout
    'min_user_history': 2,      # минимум 2 покупки для участия в evaluation
    'k_values': [5, 10, 20],    # значения K для HitRate@K и NDCG@K
    'sample_size': 300,         # размер выборки пользователей для быстроты
    'random_seed': 42,
    'cf_min_history': 2         # минимум покупок для CF рекомендаций
}

class CFRecommendationEvaluator:
    """Оценка качества CF рекомендаций"""
    
    def __init__(self, db_connection_string: str):
        self.db_conn_str = db_connection_string
        self.conn = None
        random.seed(EVALUATION_CONFIG['random_seed'])
        np.random.seed(EVALUATION_CONFIG['random_seed'])
        
    def connect_db(self):
        """Подключение к БД"""
        try:
            self.conn = psycopg2.connect(self.db_conn_str)
            logger.info("✅ Connected to database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def create_holdout_split(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Создание holdout split: train/test"""
        logger.info("📊 Creating holdout split...")
        
        holdout_cutoff = datetime.now(timezone.utc) - timedelta(days=EVALUATION_CONFIG['holdout_days'])
        
        # Получаем все взаимодействия пользователей
        query = """
        SELECT 
            user_id,
            product_id,
            event_ts,
            amount
        FROM ml_interactions_implicit
        WHERE event_ts >= NOW() - INTERVAL '60 days'  -- последние 2 месяца
        ORDER BY user_id, event_ts
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            interactions = pd.DataFrame(cur.fetchall())
        
        if interactions.empty:
            raise ValueError("No interactions found for evaluation")
        
        logger.info(f"📈 Loaded {len(interactions)} interactions")
        
        # Разделяем на train и test
        train_interactions = interactions[interactions['event_ts'] < holdout_cutoff]
        test_interactions = interactions[interactions['event_ts'] >= holdout_cutoff]
        
        logger.info(f"📊 Train: {len(train_interactions)} interactions")
        logger.info(f"📊 Test: {len(test_interactions)} interactions")
        
        return train_interactions, test_interactions
    
    def get_evaluation_users(self, train_interactions: pd.DataFrame, test_interactions: pd.DataFrame) -> List[int]:
        """Получение пользователей для evaluation"""
        logger.info("👥 Selecting users for evaluation...")
        
        # Пользователи с минимальной историей в train
        train_user_counts = train_interactions['user_id'].value_counts()
        eligible_train_users = train_user_counts[
            train_user_counts >= EVALUATION_CONFIG['min_user_history']
        ].index.tolist()
        
        # Пользователи с покупками в test
        test_users = test_interactions['user_id'].unique()
        
        # Пересечение
        evaluation_users = list(set(eligible_train_users) & set(test_users))
        
        # Случайная выборка для быстроты
        if len(evaluation_users) > EVALUATION_CONFIG['sample_size']:
            evaluation_users = random.sample(evaluation_users, EVALUATION_CONFIG['sample_size'])
        
        logger.info(f"👤 Selected {len(evaluation_users)} users for evaluation")
        return evaluation_users
    
    def get_cf_recommendations_for_user(self, user_id: int, k: int, train_interactions: pd.DataFrame) -> List[int]:
        """Получение CF рекомендаций для пользователя (имитируем логику из API)"""
        
        # Получаем историю покупок пользователя (только train)
        user_history = train_interactions[train_interactions['user_id'] == user_id]
        
        if len(user_history) < EVALUATION_CONFIG['cf_min_history']:
            return []
        
        # Берем последние 5 покупок
        recent_purchases = user_history.nlargest(5, 'event_ts')['product_id'].tolist()
        
        # Собираем кандидатов от каждой покупки
        all_candidates = {}
        recency_decay_factor = 0.9
        
        for idx, product_id in enumerate(recent_purchases):
            # Вес по recency
            recency_weight = recency_decay_factor ** idx
            
            # Получаем CF похожие товары из БД
            similar_query = """
            SELECT similar_product_id, sim_score
            FROM ml_item_sim_cf
            WHERE product_id = %s
            ORDER BY sim_score DESC
            LIMIT 30
            """
            
            with self.conn.cursor() as cur:
                cur.execute(similar_query, (int(product_id),))
                similar_items = cur.fetchall()
            
            # Добавляем кандидатов
            for similar_product_id, sim_score in similar_items:
                # Исключаем уже купленные
                if similar_product_id in recent_purchases:
                    continue
                
                cf_score = float(sim_score) * recency_weight
                
                if similar_product_id not in all_candidates:
                    all_candidates[similar_product_id] = 0
                
                all_candidates[similar_product_id] += cf_score
        
        # Сортируем и возвращаем топ-K
        sorted_candidates = sorted(all_candidates.items(), key=lambda x: x[1], reverse=True)
        recommendations = [item[0] for item in sorted_candidates[:k]]
        
        return recommendations
    
    def get_popular_baseline(self, k: int, train_interactions: pd.DataFrame) -> List[int]:
        """Популярный baseline для сравнения"""
        popular_items = train_interactions['product_id'].value_counts().head(k).index.tolist()
        return popular_items
    
    def get_content_based_recommendations(self, user_id: int, k: int, train_interactions: pd.DataFrame) -> List[int]:
        """Content-based baseline (упрощенная версия)"""
        user_history = train_interactions[train_interactions['user_id'] == user_id]
        
        if user_history.empty:
            return []
        
        # Берем последнюю покупку и находим content-based похожие
        last_purchase = int(user_history.nlargest(1, 'event_ts')['product_id'].iloc[0])
        
        similar_query = """
        SELECT similar_product_id
        FROM ml_item_sim_content
        WHERE product_id = %s
        ORDER BY sim_score DESC
        LIMIT %s
        """
        
        with self.conn.cursor() as cur:
            cur.execute(similar_query, (last_purchase, int(k)))
            similar_items = [row[0] for row in cur.fetchall()]
        
        return similar_items
    
    def calculate_hitrate(self, recommendations: List[int], actual_purchases: List[int]) -> float:
        """Вычисление HitRate@K"""
        if not actual_purchases:
            return 0.0
        
        hits = len(set(recommendations) & set(actual_purchases))
        return 1.0 if hits > 0 else 0.0
    
    def calculate_ndcg(self, recommendations: List[int], actual_purchases: List[int]) -> float:
        """Упрощенное вычисление NDCG@K"""
        if not actual_purchases:
            return 0.0
        
        # Простая версия NDCG: релевантность = 1 если товар куплен, 0 иначе
        relevance_scores = []
        for item in recommendations:
            relevance_scores.append(1.0 if item in actual_purchases else 0.0)
        
        # DCG
        dcg = 0.0
        for i, rel in enumerate(relevance_scores):
            dcg += rel / np.log2(i + 2)  # i+2 потому что log2(1) = 0
        
        # IDCG (для количества актуальных покупок)
        ideal_relevance = [1.0] * min(len(actual_purchases), len(recommendations))
        idcg = 0.0
        for i, rel in enumerate(ideal_relevance):
            idcg += rel / np.log2(i + 2)
        
        if idcg == 0:
            return 0.0
        
        return dcg / idcg
    
    def evaluate_cf_recommendations(self) -> Dict:
        """Основная функция evaluation"""
        logger.info("🎯 Starting CF recommendations evaluation...")
        start_time = datetime.now()
        
        # 1. Создаем holdout split
        train_interactions, test_interactions = self.create_holdout_split()
        
        # 2. Выбираем пользователей для evaluation
        evaluation_users = self.get_evaluation_users(train_interactions, test_interactions)
        
        if not evaluation_users:
            raise ValueError("No users available for evaluation")
        
        # 3. Оценка для каждого K
        results = {}
        
        for k in EVALUATION_CONFIG['k_values']:
            logger.info(f"📊 Evaluating HitRate@{k} and NDCG@{k}...")
            
            cf_hitrates = []
            cf_ndcgs = []
            content_hitrates = []
            content_ndcgs = []
            popular_hitrates = []
            popular_ndcgs = []
            
            # Получаем baseline рекомендации один раз
            popular_recs = self.get_popular_baseline(k, train_interactions)
            
            users_processed = 0
            
            for user_id in evaluation_users:
                # Получаем фактические покупки в test
                user_test_purchases = test_interactions[
                    test_interactions['user_id'] == user_id
                ]['product_id'].tolist()
                
                if not user_test_purchases:
                    continue
                
                users_processed += 1
                
                # CF рекомендации
                cf_recs = self.get_cf_recommendations_for_user(user_id, k, train_interactions)
                if cf_recs:  # Только если CF дал рекомендации
                    cf_hitrate = self.calculate_hitrate(cf_recs, user_test_purchases)
                    cf_ndcg = self.calculate_ndcg(cf_recs, user_test_purchases)
                    cf_hitrates.append(cf_hitrate)
                    cf_ndcgs.append(cf_ndcg)
                
                # Content-based baseline
                content_recs = self.get_content_based_recommendations(user_id, k, train_interactions)
                content_hitrate = self.calculate_hitrate(content_recs, user_test_purchases)
                content_ndcg = self.calculate_ndcg(content_recs, user_test_purchases)
                content_hitrates.append(content_hitrate)
                content_ndcgs.append(content_ndcg)
                
                # Popular baseline
                popular_hitrate = self.calculate_hitrate(popular_recs, user_test_purchases)
                popular_ndcg = self.calculate_ndcg(popular_recs, user_test_purchases)
                popular_hitrates.append(popular_hitrate)
                popular_ndcgs.append(popular_ndcg)
                
                if users_processed % 50 == 0:
                    logger.info(f"⏳ Processed {users_processed} users...")
            
            # Вычисляем средние метрики
            results[f'k_{k}'] = {
                'hitrate': {
                    'cf': np.mean(cf_hitrates) if cf_hitrates else 0,
                    'content_based': np.mean(content_hitrates) if content_hitrates else 0,
                    'popular': np.mean(popular_hitrates) if popular_hitrates else 0
                },
                'ndcg': {
                    'cf': np.mean(cf_ndcgs) if cf_ndcgs else 0,
                    'content_based': np.mean(content_ndcgs) if content_ndcgs else 0,
                    'popular': np.mean(popular_ndcgs) if popular_ndcgs else 0
                },
                'users_with_cf_recs': len(cf_hitrates),
                'total_users_evaluated': users_processed
            }
            
            logger.info(f"✅ K={k} - CF HitRate: {results[f'k_{k}']['hitrate']['cf']:.3f}, "
                       f"Content: {results[f'k_{k}']['hitrate']['content_based']:.3f}, "
                       f"Popular: {results[f'k_{k}']['hitrate']['popular']:.3f}")
        
        # Финальная статистика
        end_time = datetime.now()
        evaluation_duration = end_time - start_time
        
        results['metadata'] = {
            'evaluation_duration': str(evaluation_duration),
            'holdout_days': EVALUATION_CONFIG['holdout_days'],
            'total_users_evaluated': len(evaluation_users),
            'train_interactions': len(train_interactions),
            'test_interactions': len(test_interactions),
            'evaluation_date': end_time.isoformat()
        }
        
        logger.info("🎉 CF evaluation completed successfully!")
        return results
    
    def run_evaluation(self):
        """Запуск полного evaluation"""
        try:
            self.connect_db()
            results = self.evaluate_cf_recommendations()
            
            # Выводим результаты
            print("\n" + "="*60)
            print("📊 COLLABORATIVE FILTERING EVALUATION RESULTS")
            print("="*60)
            
            for k_key, data in results.items():
                if k_key == 'metadata':
                    continue
                    
                k = k_key.split('_')[1]
                print(f"\n🎯 K={k}:")
                print(f"   CF Users: {data['users_with_cf_recs']}/{data['total_users_evaluated']}")
                print(f"   HitRate@{k}:")
                print(f"     CF:           {data['hitrate']['cf']:.3f}")
                print(f"     Content-Based: {data['hitrate']['content_based']:.3f}")
                print(f"     Popular:       {data['hitrate']['popular']:.3f}")
                print(f"   NDCG@{k}:")
                print(f"     CF:           {data['ndcg']['cf']:.3f}")
                print(f"     Content-Based: {data['ndcg']['content_based']:.3f}")
                print(f"     Popular:       {data['ndcg']['popular']:.3f}")
                
                # Проверка превосходства
                cf_vs_popular = data['hitrate']['cf'] > data['hitrate']['popular']
                cf_vs_content = data['ndcg']['cf'] >= (data['ndcg']['content_based'] * 0.9)  # ±10%
                
                if cf_vs_popular:
                    print(f"   ✅ CF BEATS popular baseline on HitRate@{k}!")
                if cf_vs_content:
                    print(f"   ✅ CF comparable to content-based on NDCG@{k}!")
            
            print(f"\n📈 SUMMARY:")
            print(f"   Duration: {results['metadata']['evaluation_duration']}")
            print(f"   Users: {results['metadata']['total_users_evaluated']}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Evaluation failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Главная функция"""
    db_connection = "postgresql://mikitavalkunovich@localhost:5432/customer_data"
    
    evaluator = CFRecommendationEvaluator(db_connection)
    results = evaluator.run_evaluation()
    
    # Сохраняем результаты
    import json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"cf_evaluation_results_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"💾 Results saved to {results_file}")


if __name__ == "__main__":
    main()
