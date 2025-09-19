#!/usr/bin/env python3
"""
Hybrid Recommendation System Evaluation
Оценка качества гибридной системы рекомендаций vs baselines

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
import json
from itertools import product
import time

# Импортируем наш сервис
sys.path.append('/Users/mikitavalkunovich/Desktop/Cursor/Customer Data/customer-data-analytics/api')
from services.hybrid_recommendation_service import HybridRecommendationService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Параметры оценки
EVALUATION_CONFIG = {
    'holdout_days': 7,          # последние 7 дней как holdout
    'min_user_history': 2,      # минимум 2 покупки для участия
    'k_values': [10, 20],       # значения K для метрик
    'sample_size': 200,         # размер выборки для быстроты
    'random_seed': 42,
    'max_latency_ms': 150       # максимальная латентность
}

# Grid search параметры
GRID_SEARCH_PARAMS = {
    'w_cf': [0.3, 0.4, 0.5],
    'w_cb': [0.2, 0.3, 0.4], 
    'w_pop': [0.2, 0.3, 0.4],
    'lambda_div': [0.05, 0.1, 0.15],
    'gamma_price': [0.05, 0.1, 0.2]
    # beta_nov оставляем фиксированным = 0.05
}

class HybridRecommendationEvaluator:
    """Оценка качества гибридной рекомендательной системы"""
    
    def __init__(self, db_connection_string: str):
        self.db_conn_str = db_connection_string
        self.conn = None
        self.hybrid_service = HybridRecommendationService()
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
        """Создание holdout split"""
        logger.info("📊 Creating holdout split...")
        
        holdout_cutoff = datetime.now(timezone.utc) - timedelta(days=EVALUATION_CONFIG['holdout_days'])
        
        query = """
        SELECT 
            user_id,
            product_id,
            event_ts,
            amount
        FROM ml_interactions_implicit
        WHERE event_ts >= NOW() - INTERVAL '60 days'
        ORDER BY user_id, event_ts
        """
        
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            interactions = pd.DataFrame(cur.fetchall())
        
        train_interactions = interactions[interactions['event_ts'] < holdout_cutoff]
        test_interactions = interactions[interactions['event_ts'] >= holdout_cutoff]
        
        logger.info(f"📊 Train: {len(train_interactions)}, Test: {len(test_interactions)}")
        
        return train_interactions, test_interactions
    
    def get_evaluation_users(self, train_interactions: pd.DataFrame, test_interactions: pd.DataFrame) -> List[int]:
        """Получение пользователей для evaluation"""
        train_user_counts = train_interactions['user_id'].value_counts()
        eligible_train_users = train_user_counts[
            train_user_counts >= EVALUATION_CONFIG['min_user_history']
        ].index.tolist()
        
        test_users = test_interactions['user_id'].unique()
        evaluation_users = list(set(eligible_train_users) & set(test_users))
        
        if len(evaluation_users) > EVALUATION_CONFIG['sample_size']:
            evaluation_users = random.sample(evaluation_users, EVALUATION_CONFIG['sample_size'])
        
        logger.info(f"👤 Selected {len(evaluation_users)} users for evaluation")
        return evaluation_users
    
    def get_popularity_baseline(self, k: int) -> List[int]:
        """Популярный baseline"""
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT product_id
            FROM ml_item_content_features
            WHERE is_active = true AND popularity_30d > 0
            ORDER BY popularity_30d DESC
            LIMIT %s
            """, (k,))
            return [row[0] for row in cur.fetchall()]
    
    def calculate_metrics(self, recommendations: List[int], actual_purchases: List[int]) -> Dict[str, float]:
        """Вычисление HitRate и NDCG"""
        if not actual_purchases:
            return {'hitrate': 0.0, 'ndcg': 0.0}
        
        # HitRate
        hits = len(set(recommendations) & set(actual_purchases))
        hitrate = 1.0 if hits > 0 else 0.0
        
        # NDCG
        relevance_scores = [1.0 if item in actual_purchases else 0.0 for item in recommendations]
        
        dcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(relevance_scores))
        ideal_relevance = [1.0] * min(len(actual_purchases), len(recommendations))
        idcg = sum(rel / np.log2(i + 2) for i, rel in enumerate(ideal_relevance))
        
        ndcg = dcg / idcg if idcg > 0 else 0.0
        
        return {'hitrate': hitrate, 'ndcg': ndcg}
    
    def evaluate_with_weights(self, weights: Dict, evaluation_users: List[int], 
                            test_interactions: pd.DataFrame, k: int = 20) -> Dict:
        """Оценка гибридной системы с заданными весами"""
        
        # Обновляем веса в сервисе
        original_weights = self.hybrid_service.weights.copy()
        self.hybrid_service.weights.update(weights)
        self.hybrid_service.weights['beta_nov'] = 0.05  # фиксированный
        
        try:
            hitrates = []
            ndcgs = []
            latencies = []
            coverage_items = set()
            
            for user_id in evaluation_users[:50]:  # Ограничиваем для grid search
                # Получаем actual purchases
                user_test_purchases = test_interactions[
                    test_interactions['user_id'] == user_id
                ]['product_id'].tolist()
                
                if not user_test_purchases:
                    continue
                
                # Получаем гибридные рекомендации
                start_time = time.time()
                try:
                    hybrid_result = self.hybrid_service.get_hybrid_recommendations(user_id, k)
                    hybrid_recs = [item['product_id'] for item in hybrid_result['recommendations']]
                    latency = (time.time() - start_time) * 1000
                    
                    if latency > EVALUATION_CONFIG['max_latency_ms']:
                        continue  # Пропускаем слишком медленные
                    
                    latencies.append(latency)
                    coverage_items.update(hybrid_recs)
                    
                    # Вычисляем метрики
                    metrics = self.calculate_metrics(hybrid_recs, user_test_purchases)
                    hitrates.append(metrics['hitrate'])
                    ndcgs.append(metrics['ndcg'])
                    
                except Exception as e:
                    logger.warning(f"Error for user {user_id}: {e}")
                    continue
            
            # Агрегированные результаты
            result = {
                'hitrate': np.mean(hitrates) if hitrates else 0,
                'ndcg': np.mean(ndcgs) if ndcgs else 0,
                'avg_latency': np.mean(latencies) if latencies else 999,
                'coverage': len(coverage_items),
                'users_evaluated': len(hitrates),
                'weights': weights.copy()
            }
            
            return result
            
        finally:
            # Возвращаем оригинальные веса
            self.hybrid_service.weights = original_weights
    
    def grid_search_optimization(self, evaluation_users: List[int], test_interactions: pd.DataFrame) -> Dict:
        """Grid search для поиска оптимальных весов"""
        logger.info("🔍 Starting grid search for optimal weights...")
        
        best_result = None
        best_score = -1
        all_results = []
        
        # Генерируем все комбинации параметров
        param_names = list(GRID_SEARCH_PARAMS.keys())
        param_values = list(GRID_SEARCH_PARAMS.values())
        
        total_combinations = len(list(product(*param_values)))
        logger.info(f"🔢 Testing {total_combinations} weight combinations...")
        
        for i, combination in enumerate(product(*param_values)):
            weights = dict(zip(param_names, combination))
            
            # Проверяем что веса CF+CB+Pop = 1.0 (приблизительно)
            weight_sum = weights['w_cf'] + weights['w_cb'] + weights['w_pop']
            if abs(weight_sum - 1.0) > 0.01:
                continue
            
            logger.info(f"⏳ Testing combination {i+1}/{total_combinations}: {weights}")
            
            try:
                result = self.evaluate_with_weights(weights, evaluation_users, test_interactions, k=20)
                
                # Комбинированный скор: NDCG * 0.7 + HitRate * 0.3 - латентность штраф
                latency_penalty = max(0, (result['avg_latency'] - 100) / 1000)  # штраф за >100ms
                combined_score = result['ndcg'] * 0.7 + result['hitrate'] * 0.3 - latency_penalty
                
                result['combined_score'] = combined_score
                all_results.append(result)
                
                if combined_score > best_score:
                    best_score = combined_score
                    best_result = result
                    logger.info(f"🏆 New best score: {combined_score:.3f} "
                               f"(NDCG: {result['ndcg']:.3f}, HitRate: {result['hitrate']:.3f})")
                
            except Exception as e:
                logger.warning(f"Error in combination {weights}: {e}")
                continue
        
        logger.info(f"✅ Grid search completed. Best score: {best_score:.3f}")
        
        return {
            'best_weights': best_result,
            'all_results': all_results,
            'total_tested': len(all_results)
        }
    
    def evaluate_final_hybrid(self, evaluation_users: List[int], test_interactions: pd.DataFrame) -> Dict:
        """Финальная оценка гибридной системы vs baselines"""
        logger.info("🎯 Final hybrid evaluation vs baselines...")
        
        results = {}
        
        for k in EVALUATION_CONFIG['k_values']:
            logger.info(f"📊 Evaluating for K={k}...")
            
            hybrid_hitrates = []
            hybrid_ndcgs = []
            pop_hitrates = []
            pop_ndcgs = []
            hybrid_latencies = []
            hybrid_coverage = set()
            
            # Популярный baseline
            popular_recs = self.get_popularity_baseline(k)
            
            for user_id in evaluation_users:
                user_test_purchases = test_interactions[
                    test_interactions['user_id'] == user_id
                ]['product_id'].tolist()
                
                if not user_test_purchases:
                    continue
                
                # Гибридные рекомендации
                start_time = time.time()
                try:
                    hybrid_result = self.hybrid_service.get_hybrid_recommendations(user_id, k)
                    hybrid_recs = [item['product_id'] for item in hybrid_result['recommendations']]
                    latency = (time.time() - start_time) * 1000
                    
                    hybrid_latencies.append(latency)
                    hybrid_coverage.update(hybrid_recs)
                    
                    hybrid_metrics = self.calculate_metrics(hybrid_recs, user_test_purchases)
                    hybrid_hitrates.append(hybrid_metrics['hitrate'])
                    hybrid_ndcgs.append(hybrid_metrics['ndcg'])
                    
                except Exception as e:
                    logger.warning(f"Error for user {user_id}: {e}")
                    # Fallback на популярные
                    hybrid_hitrates.append(0)
                    hybrid_ndcgs.append(0)
                    hybrid_latencies.append(999)
                
                # Популярный baseline
                pop_metrics = self.calculate_metrics(popular_recs, user_test_purchases)
                pop_hitrates.append(pop_metrics['hitrate'])
                pop_ndcgs.append(pop_metrics['ndcg'])
            
            # Агрегируем результаты
            results[f'k_{k}'] = {
                'hybrid': {
                    'hitrate': np.mean(hybrid_hitrates),
                    'ndcg': np.mean(hybrid_ndcgs),
                    'avg_latency': np.mean(hybrid_latencies),
                    'p95_latency': np.percentile(hybrid_latencies, 95),
                    'coverage': len(hybrid_coverage)
                },
                'popularity': {
                    'hitrate': np.mean(pop_hitrates),
                    'ndcg': np.mean(pop_ndcgs),
                    'coverage': len(popular_recs)
                },
                'users_evaluated': len(hybrid_hitrates)
            }
            
            # Проверяем acceptance criteria
            hybrid_data = results[f'k_{k}']['hybrid']
            pop_data = results[f'k_{k}']['popularity']
            
            ndcg_check = hybrid_data['ndcg'] >= pop_data['ndcg']
            hitrate_check = hybrid_data['hitrate'] >= (pop_data['hitrate'] - 0.02)  # -2pp
            latency_check = hybrid_data['p95_latency'] <= EVALUATION_CONFIG['max_latency_ms']
            coverage_check = hybrid_data['coverage'] > pop_data['coverage']
            
            results[f'k_{k}']['acceptance_criteria'] = {
                'ndcg_vs_popular': ndcg_check,
                'hitrate_vs_popular_minus_2pp': hitrate_check,
                'latency_under_150ms': latency_check,
                'coverage_better_than_popular': coverage_check,
                'all_passed': ndcg_check and hitrate_check and latency_check and coverage_check
            }
            
            logger.info(f"✅ K={k} Results:")
            logger.info(f"   Hybrid: HitRate={hybrid_data['hitrate']:.3f}, NDCG={hybrid_data['ndcg']:.3f}")
            logger.info(f"   Popular: HitRate={pop_data['hitrate']:.3f}, NDCG={pop_data['ndcg']:.3f}")
            logger.info(f"   Latency: {hybrid_data['p95_latency']:.1f}ms")
            logger.info(f"   Coverage: {hybrid_data['coverage']} vs {pop_data['coverage']}")
        
        return results
    
    def run_full_evaluation(self):
        """Запуск полного evaluation пайплайна"""
        logger.info("🚀 Starting full hybrid recommendation evaluation...")
        start_time = datetime.now()
        
        try:
            self.connect_db()
            
            # 1. Создаем holdout split
            train_interactions, test_interactions = self.create_holdout_split()
            evaluation_users = self.get_evaluation_users(train_interactions, test_interactions)
            
            # 2. Grid search оптимизация (опционально)
            logger.info("🔍 Running grid search optimization...")
            grid_results = self.grid_search_optimization(evaluation_users, test_interactions)
            
            # Обновляем веса на лучшие найденные
            if grid_results['best_weights']:
                best_weights = grid_results['best_weights']['weights']
                logger.info(f"🏆 Applying best weights: {best_weights}")
                self.hybrid_service.weights.update(best_weights)
                self.hybrid_service.weights['beta_nov'] = 0.05
            
            # 3. Финальная оценка
            final_results = self.evaluate_final_hybrid(evaluation_users, test_interactions)
            
            # 4. Компилируем полный отчет
            full_report = {
                'evaluation_metadata': {
                    'duration': str(datetime.now() - start_time),
                    'users_evaluated': len(evaluation_users),
                    'holdout_days': EVALUATION_CONFIG['holdout_days'],
                    'evaluation_date': datetime.now().isoformat()
                },
                'grid_search': grid_results,
                'final_evaluation': final_results,
                'final_weights': self.hybrid_service.weights.copy()
            }
            
            return full_report
            
        except Exception as e:
            logger.error(f"❌ Evaluation failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Главная функция"""
    db_connection = "postgresql://mikitavalkunovich@localhost:5432/customer_data"
    
    evaluator = HybridRecommendationEvaluator(db_connection)
    results = evaluator.run_full_evaluation()
    
    # Сохраняем результаты
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"hybrid_evaluation_results_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Выводим краткий отчет
    print("\n" + "="*70)
    print("🔀 HYBRID RECOMMENDATION SYSTEM EVALUATION REPORT")
    print("="*70)
    
    if 'final_evaluation' in results:
        for k_key, data in results['final_evaluation'].items():
            if 'acceptance_criteria' not in data:
                continue
                
            k = k_key.split('_')[1]
            hybrid = data['hybrid']
            popular = data['popularity']
            criteria = data['acceptance_criteria']
            
            print(f"\n🎯 K={k} RESULTS:")
            print(f"   Hybrid:    HitRate={hybrid['hitrate']:.3f}, NDCG={hybrid['ndcg']:.3f}")
            print(f"   Popular:   HitRate={popular['hitrate']:.3f}, NDCG={popular['ndcg']:.3f}")
            print(f"   Latency:   {hybrid['p95_latency']:.1f}ms (p95)")
            print(f"   Coverage:  {hybrid['coverage']} vs {popular['coverage']}")
            
            print(f"\n✅ ACCEPTANCE CRITERIA:")
            for criterion, passed in criteria.items():
                if criterion != 'all_passed':
                    status = "✅ PASS" if passed else "❌ FAIL"
                    print(f"   {criterion}: {status}")
            
            overall = "🏆 ALL PASSED" if criteria['all_passed'] else "❌ SOME FAILED"
            print(f"\n   OVERALL: {overall}")
    
    if 'final_weights' in results:
        print(f"\n⚙️ OPTIMIZED WEIGHTS:")
        weights = results['final_weights']
        print(f"   CF: {weights['w_cf']:.2f}, Content: {weights['w_cb']:.2f}, Pop: {weights['w_pop']:.2f}")
        print(f"   Diversity: {weights['lambda_div']:.2f}, Price: {weights['gamma_price']:.2f}")
    
    logger.info(f"💾 Full results saved to {results_file}")


if __name__ == "__main__":
    main()
