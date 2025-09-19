"""
Hybrid Recommendation Service
Гибридная рекомендательная система с reranking

Author: Customer Data Analytics Team
"""

import logging
import psycopg2
import psycopg2.extras
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import os
import numpy as np
import time
from collections import defaultdict

logger = logging.getLogger(__name__)

# Строка подключения к БД
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://mikitavalkunovich@localhost:5432/customer_data"
)

class HybridRecommendationService:
    """Гибридная рекомендательная система"""
    
    def __init__(self):
        # Веса источников (максимальная фокусировка на популярности)
        self.weights = {
            'w_cf': 0.15,       # Collaborative Filtering (еще больше снижено)
            'w_cb': 0.15,       # Content-Based (еще больше снижено)
            'w_pop': 0.7,       # Popularity (максимально увеличено)
            'lambda_div': 0.01, # MMR diversity penalty (минимально)
            'beta_nov': 0.01,   # Novelty bonus (минимально)
            'gamma_price': 0.01 # Price gap penalty (минимально)
        }
        
        # Параметры кандидатов
        self.candidate_limits = {
            'cf_limit': 100,
            'cb_limit': 100, 
            'pop_limit': 100
        }
        
        # Параметры персонализации
        self.recency_decay_factor = 0.9
        self.max_user_history = 5
        self.cf_min_history = 2
    
    def get_user_purchase_history(self, user_id: int) -> List[Dict]:
        """Получение истории покупок пользователя"""
        with psycopg2.connect(DATABASE_URL) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                history_query = """
                SELECT 
                    product_id,
                    event_ts,
                    amount,
                    EXTRACT(days FROM NOW() - event_ts) as days_ago
                FROM ml_interactions_implicit
                WHERE user_id = %s
                ORDER BY event_ts DESC
                LIMIT %s
                """
                
                cur.execute(history_query, (user_id, self.max_user_history))
                return [dict(row) for row in cur.fetchall()]
    
    def get_cf_candidates(self, user_id: int, limit: int = 100) -> List[Dict]:
        """Получение CF кандидатов"""
        try:
            user_history = self.get_user_purchase_history(user_id)
            
            if len(user_history) < self.cf_min_history:
                logger.info(f"User {user_id} has insufficient history for CF")
                return []
            
            purchased_product_ids = [int(item['product_id']) for item in user_history]
            candidates = {}
            
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    for hist_item in user_history:
                        recency_weight = self.recency_decay_factor ** float(hist_item['days_ago'])
                        
                        cf_query = """
                        SELECT 
                            s.similar_product_id,
                            s.sim_score,
                            s.co_users,
                            p.title,
                            p.brand,
                            p.category,
                            p.price_current,
                            p.popularity_30d,
                            p.rating
                        FROM ml_item_sim_cf s
                        JOIN ml_item_content_features p ON s.similar_product_id = p.product_id
                        WHERE s.product_id = %s
                            AND p.is_active = true
                            AND s.similar_product_id != ALL(%s)
                        ORDER BY s.sim_score DESC
                        LIMIT 50
                        """
                        
                        cur.execute(cf_query, (int(hist_item['product_id']), purchased_product_ids))
                        similar_items = cur.fetchall()
                        
                        for item in similar_items:
                            product_id = int(item['similar_product_id'])
                            cf_score = float(item['sim_score']) * recency_weight
                            
                            if product_id not in candidates or candidates[product_id]['score'] < cf_score:
                                candidates[product_id] = {
                                    "product_id": product_id,
                                    "title": item['title'],
                                    "brand": item['brand'],
                                    "category": item['category'],
                                    "price": float(item['price_current']),
                                    "popularity_score": float(item['popularity_30d'] or 0),
                                    "rating": float(item['rating'] or 0),
                                    "score": cf_score,
                                    "source": "cf",
                                    "cf_similarity": float(item['sim_score']),
                                    "co_users": int(item['co_users'])
                                }
            
            # Сортируем и возвращаем топ-N
            sorted_candidates = sorted(candidates.values(), key=lambda x: x["score"], reverse=True)
            return sorted_candidates[:limit]
            
        except Exception as e:
            logger.error(f"Error getting CF candidates: {e}")
            return []
    
    def get_content_candidates(self, user_id: int, limit: int = 100) -> List[Dict]:
        """Получение Content-based кандидатов"""
        try:
            user_history = self.get_user_purchase_history(user_id)
            
            if not user_history:
                return []
            
            purchased_product_ids = [int(item['product_id']) for item in user_history]
            candidates = {}
            
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Берем последние 3 покупки для content-based
                    for hist_item in user_history[:3]:
                        content_query = """
                        SELECT 
                            s.similar_product_id,
                            s.sim_score,
                            p.title,
                            p.brand,
                            p.category,
                            p.price_current,
                            p.popularity_30d,
                            p.rating
                        FROM ml_item_sim_content s
                        JOIN ml_item_content_features p ON s.similar_product_id = p.product_id
                        WHERE s.product_id = %s
                            AND p.is_active = true
                            AND s.similar_product_id != ALL(%s)
                        ORDER BY s.sim_score DESC
                        LIMIT 40
                        """
                        
                        cur.execute(content_query, (int(hist_item['product_id']), purchased_product_ids))
                        similar_items = cur.fetchall()
                        
                        for item in similar_items:
                            product_id = int(item['similar_product_id'])
                            cb_score = float(item['sim_score'])
                            
                            if product_id not in candidates or candidates[product_id]['score'] < cb_score:
                                candidates[product_id] = {
                                    "product_id": product_id,
                                    "title": item['title'],
                                    "brand": item['brand'],
                                    "category": item['category'],
                                    "price": float(item['price_current']),
                                    "popularity_score": float(item['popularity_30d'] or 0),
                                    "rating": float(item['rating'] or 0),
                                    "score": cb_score,
                                    "source": "content",
                                    "content_similarity": cb_score
                                }
            
            sorted_candidates = sorted(candidates.values(), key=lambda x: x["score"], reverse=True)
            return sorted_candidates[:limit]
            
        except Exception as e:
            logger.error(f"Error getting content candidates: {e}")
            return []
    
    def get_popularity_candidates(self, user_id: int, limit: int = 100) -> List[Dict]:
        """Получение Popularity кандидатов"""
        try:
            # Получаем основные категории пользователя
            user_history = self.get_user_purchase_history(user_id)
            user_categories = []
            
            if user_history:
                with psycopg2.connect(DATABASE_URL) as conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        # Получаем категории покупок пользователя
                        purchased_ids = [int(item['product_id']) for item in user_history]
                        categories_query = """
                        SELECT DISTINCT category
                        FROM ml_item_content_features
                        WHERE product_id = ANY(%s) AND category IS NOT NULL
                        """
                        cur.execute(categories_query, (purchased_ids,))
                        user_categories = [row['category'] for row in cur.fetchall()]
            
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Популярные товары (с предпочтением категорий пользователя)
                    if user_categories:
                        pop_query = """
                        SELECT 
                            product_id,
                            title,
                            brand,
                            category,
                            price_current,
                            popularity_30d,
                            rating,
                            CASE 
                                WHEN category = ANY(%s) THEN popularity_30d * 1.5
                                ELSE popularity_30d
                            END as weighted_popularity
                        FROM ml_item_content_features
                        WHERE is_active = true 
                            AND popularity_30d > 0
                            AND product_id != ALL(%s)
                        ORDER BY weighted_popularity DESC
                        LIMIT %s
                        """
                        purchased_ids = [int(item['product_id']) for item in user_history] if user_history else []
                        cur.execute(pop_query, (user_categories, purchased_ids, limit))
                    else:
                        # Просто популярные товары
                        pop_query = """
                        SELECT 
                            product_id,
                            title,
                            brand,
                            category,
                            price_current,
                            popularity_30d,
                            rating
                        FROM ml_item_content_features
                        WHERE is_active = true 
                            AND popularity_30d > 0
                        ORDER BY popularity_30d DESC
                        LIMIT %s
                        """
                        cur.execute(pop_query, (limit,))
                    
                    items = cur.fetchall()
                    
                    candidates = []
                    for item in items:
                        candidates.append({
                            "product_id": int(item['product_id']),
                            "title": item['title'],
                            "brand": item['brand'],
                            "category": item['category'],
                            "price": float(item['price_current']),
                            "popularity_score": float(item['popularity_30d']),
                            "rating": float(item['rating'] or 0),
                            "score": float(item.get('weighted_popularity', item['popularity_30d'])),
                            "source": "popularity"
                        })
                    
                    return candidates
            
        except Exception as e:
            logger.error(f"Error getting popularity candidates: {e}")
            return []
    
    def normalize_scores(self, candidates: List[Dict], score_field: str = 'score') -> List[Dict]:
        """Min-max нормализация скоров к [0, 1]"""
        if not candidates:
            return candidates
        
        scores = [c[score_field] for c in candidates]
        min_score = min(scores)
        max_score = max(scores)
        
        if max_score == min_score:  # Избегаем деления на ноль
            for candidate in candidates:
                candidate[f'{score_field}_normalized'] = 1.0
        else:
            for candidate in candidates:
                normalized = (candidate[score_field] - min_score) / (max_score - min_score)
                candidate[f'{score_field}_normalized'] = normalized
        
        return candidates
    
    def calculate_diversity_penalty(self, candidates: List[Dict], top_k: int = 20) -> Dict[int, float]:
        """Вычисление MMR diversity penalty"""
        penalties = {}
        
        # Группируем по категориям для простого MMR
        category_counts = defaultdict(int)
        
        for i, candidate in enumerate(candidates[:top_k * 2]):  # Рассматриваем больше для MMR
            category = candidate.get('category', 'unknown')
            category_counts[category] += 1
            
            # Штраф растет с количеством товаров в категории
            penalty = min(category_counts[category] * 0.1, 0.5)  # Максимум 0.5
            penalties[candidate['product_id']] = penalty
        
        return penalties
    
    def calculate_novelty_bonus(self, candidates: List[Dict]) -> Dict[int, float]:
        """Вычисление novelty bonus (обратно пропорционально популярности)"""
        bonuses = {}
        
        if not candidates:
            return bonuses
        
        # Нормализуем популярность
        popularities = [c.get('popularity_score', 0) for c in candidates]
        max_pop = max(popularities) if popularities else 1
        
        for candidate in candidates:
            pop_score = candidate.get('popularity_score', 0)
            # Бонус за новизну = 1 - normalized_popularity
            novelty = 1.0 - (pop_score / max_pop) if max_pop > 0 else 0.5
            bonuses[candidate['product_id']] = novelty
        
        return bonuses
    
    def calculate_price_penalty(self, candidates: List[Dict], user_history: List[Dict]) -> Dict[int, float]:
        """Вычисление price gap penalty"""
        penalties = {}
        
        if not user_history:
            return penalties
        
        # Средняя цена покупок пользователя
        user_avg_price = np.mean([float(item.get('amount', 0)) for item in user_history])
        
        for candidate in candidates:
            price_gap = abs(candidate['price'] - user_avg_price) / user_avg_price if user_avg_price > 0 else 0
            # Нормализуем штраф к [0, 1]
            penalty = min(price_gap, 1.0)
            penalties[candidate['product_id']] = penalty
        
        return penalties
    
    def merge_and_rerank_candidates(self, cf_candidates: List[Dict], cb_candidates: List[Dict], 
                                   pop_candidates: List[Dict], user_history: List[Dict], k: int = 20) -> List[Dict]:
        """Объединение и переранжирование кандидатов"""
        
        # 1. Нормализуем скоры по источникам
        cf_normalized = self.normalize_scores(cf_candidates.copy())
        cb_normalized = self.normalize_scores(cb_candidates.copy())
        pop_normalized = self.normalize_scores(pop_candidates.copy())
        
        # 2. Объединяем всех кандидатов
        all_candidates = {}
        
        # Добавляем CF кандидатов
        for candidate in cf_normalized:
            pid = candidate['product_id']
            candidate['cf_score'] = candidate.get('score_normalized', 0)
            candidate['cb_score'] = 0
            candidate['pop_score'] = 0
            all_candidates[pid] = candidate
        
        # Добавляем Content-based кандидатов
        for candidate in cb_normalized:
            pid = candidate['product_id']
            if pid in all_candidates:
                all_candidates[pid]['cb_score'] = candidate.get('score_normalized', 0)
                all_candidates[pid]['source'] = 'cf+content'
            else:
                candidate['cf_score'] = 0
                candidate['cb_score'] = candidate.get('score_normalized', 0)
                candidate['pop_score'] = 0
                all_candidates[pid] = candidate
        
        # Добавляем Popularity кандидатов
        for candidate in pop_normalized:
            pid = candidate['product_id']
            if pid in all_candidates:
                all_candidates[pid]['pop_score'] = candidate.get('score_normalized', 0)
                if all_candidates[pid]['source'] == 'cf+content':
                    all_candidates[pid]['source'] = 'cf+content+pop'
                elif 'cf' in all_candidates[pid]['source']:
                    all_candidates[pid]['source'] = 'cf+pop'
                elif 'content' in all_candidates[pid]['source']:
                    all_candidates[pid]['source'] = 'content+pop'
            else:
                candidate['cf_score'] = 0
                candidate['cb_score'] = 0
                candidate['pop_score'] = candidate.get('score_normalized', 0)
                all_candidates[pid] = candidate
        
        candidates_list = list(all_candidates.values())
        
        # 3. Вычисляем штрафы и бонусы
        diversity_penalties = self.calculate_diversity_penalty(candidates_list, k)
        novelty_bonuses = self.calculate_novelty_bonus(candidates_list)
        price_penalties = self.calculate_price_penalty(candidates_list, user_history)
        
        # 4. Применяем гибридную формулу
        for candidate in candidates_list:
            pid = candidate['product_id']
            
            # Базовый скор
            hybrid_score = (
                self.weights['w_cf'] * candidate['cf_score'] +
                self.weights['w_cb'] * candidate['cb_score'] +
                self.weights['w_pop'] * candidate['pop_score']
            )
            
            # Применяем штрафы и бонусы
            hybrid_score -= self.weights['lambda_div'] * diversity_penalties.get(pid, 0)
            hybrid_score += self.weights['beta_nov'] * novelty_bonuses.get(pid, 0)
            hybrid_score -= self.weights['gamma_price'] * price_penalties.get(pid, 0)
            
            candidate['hybrid_score'] = hybrid_score
            candidate['diversity_penalty'] = diversity_penalties.get(pid, 0)
            candidate['novelty_bonus'] = novelty_bonuses.get(pid, 0)
            candidate['price_penalty'] = price_penalties.get(pid, 0)
        
        # 5. Сортируем по гибридному скору
        final_candidates = sorted(candidates_list, key=lambda x: x['hybrid_score'], reverse=True)
        
        return final_candidates[:k]
    
    def get_hybrid_recommendations(self, user_id: int, k: int = 20) -> Dict:
        """Основная функция получения гибридных рекомендаций"""
        start_time = time.time()
        
        try:
            logger.info(f"🔀 Getting hybrid recommendations for user {user_id}, k={k}")
            
            # 1. Получаем историю пользователя
            user_history = self.get_user_purchase_history(user_id)
            
            # 2. Параллельно собираем кандидатов от всех источников
            cf_candidates = self.get_cf_candidates(user_id, self.candidate_limits['cf_limit'])
            cb_candidates = self.get_content_candidates(user_id, self.candidate_limits['cb_limit'])
            pop_candidates = self.get_popularity_candidates(user_id, self.candidate_limits['pop_limit'])
            
            logger.info(f"📊 Candidates: CF={len(cf_candidates)}, Content={len(cb_candidates)}, Pop={len(pop_candidates)}")
            
            # 3. Объединяем и переранжируем
            final_recommendations = self.merge_and_rerank_candidates(
                cf_candidates, cb_candidates, pop_candidates, user_history, k
            )
            
            # 4. Статистика источников
            source_stats = {}
            for rec in final_recommendations:
                source = rec.get('source', 'unknown')
                source_stats[source] = source_stats.get(source, 0) + 1
            
            processing_time = (time.time() - start_time) * 1000
            
            result = {
                'user_id': user_id,
                'recommendations': final_recommendations,
                'source_statistics': source_stats,
                'processing_time_ms': processing_time,
                'weights_used': self.weights.copy(),
                'candidate_counts': {
                    'cf': len(cf_candidates),
                    'content': len(cb_candidates),
                    'popularity': len(pop_candidates),
                    'final': len(final_recommendations)
                }
            }
            
            logger.info(f"✅ Hybrid recommendations: {len(final_recommendations)} items, {processing_time:.1f}ms")
            logger.info(f"📊 Source breakdown: {source_stats}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error in hybrid recommendations: {e}")
            raise


# Глобальный экземпляр сервиса
hybrid_recommendation_service = HybridRecommendationService()
