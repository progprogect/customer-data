#!/usr/bin/env python3
"""
Content-Based Recommendations Evaluation
Быстрая оценка качества content-based рекомендаций

Author: Customer Data Analytics Team
"""

import os
import sys
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta
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
    'k_values': [5, 10, 20],    # значения K для HitRate@K
    'sample_size': 500,         # размер выборки пользователей для быстроты
    'random_seed': 42
}

class ContentRecommendationEvaluator:
    """Оценка качества content-based рекомендаций"""
    
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
        
        from datetime import timezone
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
    
    def get_content_recommendations_for_user(self, user_id: int, k: int, train_interactions: pd.DataFrame) -> List[int]:
        """Получение content-based рекомендаций для пользователя"""
        
        # Получаем историю покупок пользователя (только train)
        user_history = train_interactions[train_interactions['user_id'] == user_id]
        
        if user_history.empty:
            return []
        
        # Берем последние 3 покупки
        recent_purchases = user_history.nlargest(3, 'event_ts')['product_id'].tolist()
        
        # Собираем кандидатов от каждой покупки
        all_candidates = {}
        
        for product_id in recent_purchases:
            # Получаем похожие товары из БД
            similar_query = """
            SELECT similar_product_id, sim_score
            FROM ml_item_sim_content
            WHERE product_id = %s
            ORDER BY sim_score DESC
            LIMIT 20
            """
            
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(similar_query, (product_id,))
                similar_items = cur.fetchall()
            
            # Добавляем кандидатов
            for item in similar_items:
                candidate_id = item['similar_product_id']
                score = item['sim_score']
                
                # Исключаем уже купленные
                if candidate_id in user_history['product_id'].values:
                    continue
                
                if candidate_id not in all_candidates:
                    all_candidates[candidate_id] = 0
                
                all_candidates[candidate_id] += score
        
        # Сортируем и возвращаем топ-K
        sorted_candidates = sorted(all_candidates.items(), key=lambda x: x[1], reverse=True)
        recommendations = [item[0] for item in sorted_candidates[:k]]
        
        return recommendations
    
    def get_popular_baseline(self, k: int, train_interactions: pd.DataFrame) -> List[int]:
        """Популярный baseline для сравнения"""
        popular_items = train_interactions['product_id'].value_counts().head(k).index.tolist()
        return popular_items
    
    def get_random_baseline(self, k: int) -> List[int]:
        """Случайный baseline"""
        # Получаем все активные товары
        query = """
        SELECT product_id 
        FROM ml_item_content_features 
        WHERE is_active = true
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            all_products = [row[0] for row in cur.fetchall()]
        
        return random.sample(all_products, min(k, len(all_products)))
    
    def calculate_hitrate(self, recommendations: List[int], actual_purchases: List[int]) -> float:
        """Вычисление HitRate@K"""
        if not actual_purchases:
            return 0.0
        
        hits = len(set(recommendations) & set(actual_purchases))
        return 1.0 if hits > 0 else 0.0
    
    def evaluate_recommendations(self) -> Dict:
        """Основная функция evaluation"""
        logger.info("🎯 Starting content-based recommendations evaluation...")
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
            logger.info(f"📊 Evaluating HitRate@{k}...")
            
            content_hitrates = []
            popular_hitrates = []
            random_hitrates = []
            
            # Получаем baseline рекомендации один раз
            popular_recs = self.get_popular_baseline(k, train_interactions)
            
            for i, user_id in enumerate(evaluation_users):
                # Получаем фактические покупки в test
                user_test_purchases = test_interactions[
                    test_interactions['user_id'] == user_id
                ]['product_id'].tolist()
                
                if not user_test_purchases:
                    continue
                
                # Content-based рекомендации
                content_recs = self.get_content_recommendations_for_user(user_id, k, train_interactions)
                content_hitrate = self.calculate_hitrate(content_recs, user_test_purchases)
                content_hitrates.append(content_hitrate)
                
                # Popular baseline
                popular_hitrate = self.calculate_hitrate(popular_recs, user_test_purchases)
                popular_hitrates.append(popular_hitrate)
                
                # Random baseline  
                random_recs = self.get_random_baseline(k)
                random_hitrate = self.calculate_hitrate(random_recs, user_test_purchases)
                random_hitrates.append(random_hitrate)
                
                if (i + 1) % 50 == 0:
                    logger.info(f"⏳ Processed {i + 1}/{len(evaluation_users)} users...")
            
            # Вычисляем средние метрики
            results[f'hitrate@{k}'] = {
                'content_based': np.mean(content_hitrates) if content_hitrates else 0,
                'popular_baseline': np.mean(popular_hitrates) if popular_hitrates else 0,
                'random_baseline': np.mean(random_hitrates) if random_hitrates else 0,
                'users_evaluated': len(content_hitrates)
            }
            
            logger.info(f"✅ HitRate@{k} - Content: {results[f'hitrate@{k}']['content_based']:.3f}, "
                       f"Popular: {results[f'hitrate@{k}']['popular_baseline']:.3f}, "
                       f"Random: {results[f'hitrate@{k}']['random_baseline']:.3f}")
        
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
        
        logger.info("🎉 Evaluation completed successfully!")
        return results
    
    def run_evaluation(self):
        """Запуск полного evaluation"""
        try:
            self.connect_db()
            results = self.evaluate_recommendations()
            
            # Выводим результаты
            print("\n" + "="*60)
            print("📊 CONTENT-BASED RECOMMENDATIONS EVALUATION RESULTS")
            print("="*60)
            
            for metric, data in results.items():
                if metric == 'metadata':
                    continue
                    
                print(f"\n🎯 {metric.upper()}:")
                print(f"   Content-Based:    {data['content_based']:.3f}")
                print(f"   Popular Baseline: {data['popular_baseline']:.3f}")
                print(f"   Random Baseline:  {data['random_baseline']:.3f}")
                print(f"   Users Evaluated:  {data['users_evaluated']}")
                
                # Проверка превосходства над baseline
                if data['content_based'] > data['popular_baseline']:
                    print(f"   ✅ Content-based BEATS popular baseline!")
                else:
                    print(f"   ⚠️ Content-based below popular baseline")
            
            print(f"\n📈 SUMMARY:")
            print(f"   Total Users: {results['metadata']['total_users_evaluated']}")
            print(f"   Duration: {results['metadata']['evaluation_duration']}")
            print(f"   Holdout: {results['metadata']['holdout_days']} days")
            
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
    
    evaluator = ContentRecommendationEvaluator(db_connection)
    results = evaluator.run_evaluation()
    
    # Сохраняем результаты
    import json
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"content_evaluation_results_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"💾 Results saved to {results_file}")


if __name__ == "__main__":
    main()
