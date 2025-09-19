"""
Recommendation Service
Сервис для content-based рекомендаций товаров
"""

import logging
import psycopg2
import psycopg2.extras
from typing import List, Dict, Optional
from datetime import datetime
import os
import numpy as np

logger = logging.getLogger(__name__)

# Строка подключения к БД
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://mikitavalkunovich@localhost:5432/customer_data"
)

class RecommendationService:
    """Сервис для content-based и collaborative filtering рекомендаций"""
    
    def __init__(self):
        self.recency_decay_factor = 0.9  # затухание веса по времени
        self.max_user_history = 5        # максимум последних покупок для анализа
        self.price_tolerance = 0.5       # ±50% от средней цены покупок пользователя
        self.cf_min_history = 2          # минимум покупок для CF рекомендаций
    
    def get_item_similar(self, product_id: int, k: int = 20) -> List[Dict]:
        """Получение похожих товаров для конкретного продукта"""
        try:
            logger.info(f"🔍 Getting {k} similar items for product {product_id}")
            
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    similar_query = """
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
                    ORDER BY s.sim_score DESC
                    LIMIT %s
                    """
                    
                    cur.execute(similar_query, (product_id, k))
                    similar_items = cur.fetchall()
                    
                    recommendations = []
                    for item in similar_items:
                        recommendations.append({
                            "product_id": item["similar_product_id"],
                            "title": item["title"],
                            "brand": item["brand"],
                            "category": item["category"],
                            "price": float(item["price_current"]),
                            "similarity_score": float(item["sim_score"]),
                            "popularity_score": float(item["popularity_30d"] or 0),
                            "rating": float(item["rating"] or 0)
                        })
                    
                    logger.info(f"✅ Found {len(recommendations)} similar items")
                    return recommendations
                
        except Exception as e:
            logger.error(f"❌ Error getting similar items: {e}")
            raise

    def get_user_cf_recommendations(self, user_id: int, k: int = 20) -> List[Dict]:
        """Получение CF рекомендаций для пользователя на основе ко-покупок"""
        try:
            logger.info(f"🤝 Getting {k} CF recommendations for user {user_id}")
            
            with psycopg2.connect(DATABASE_URL) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # 1. Получаем историю покупок пользователя
                    user_history_query = """
                    SELECT 
                        product_id,
                        event_ts,
                        EXTRACT(days FROM NOW() - event_ts) as days_ago
                    FROM ml_interactions_implicit
                    WHERE user_id = %s
                    ORDER BY event_ts DESC
                    LIMIT %s
                    """
                    
                    cur.execute(user_history_query, (user_id, self.max_user_history))
                    user_history = cur.fetchall()
                    
                    if len(user_history) < self.cf_min_history:
                        logger.info(f"👤 User {user_id} has insufficient history for CF ({len(user_history)} purchases)")
                        return []
                    
                    # 2. Собираем кандидатов от каждой покупки
                    purchased_product_ids = [item['product_id'] for item in user_history]
                    candidates = {}
                    
                    for hist_item in user_history:
                        # Вычисляем вес на основе recency (экспоненциальное затухание)
                        recency_weight = self.recency_decay_factor ** float(hist_item['days_ago'])
                        
                        # Получаем CF похожие товары
                        cf_similar_query = """
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
                        LIMIT 30
                        """
                        
                        cur.execute(cf_similar_query, (hist_item['product_id'], purchased_product_ids))
                        similar_items = cur.fetchall()
                        
                        # Добавляем кандидатов с весами
                        for item in similar_items:
                            product_id = item['similar_product_id']
                            
                            # CF score = similarity * recency_weight
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
                                    "cf_similarity": float(item['sim_score']),
                                    "co_users": int(item['co_users']),
                                    "recency_weight": recency_weight,
                                    "recommendation_reason": "collaborative_filtering"
                                }
                    
                    # 3. Сортируем и возвращаем топ-K
                    sorted_candidates = sorted(
                        candidates.values(), 
                        key=lambda x: x["score"], 
                        reverse=True
                    )
                    
                    recommendations = sorted_candidates[:k]
                    
                    logger.info(f"✅ Generated {len(recommendations)} CF recommendations for user {user_id}")
                    logger.info(f"📊 Avg CF similarity: {np.mean([r['cf_similarity'] for r in recommendations]):.3f}")
                    logger.info(f"📊 Avg co-users: {np.mean([r['co_users'] for r in recommendations]):.1f}")
                    
                    return recommendations
                    
        except Exception as e:
            logger.error(f"❌ Error getting CF recommendations for user {user_id}: {e}")
            raise

# Глобальный экземпляр сервиса
recommendation_service = RecommendationService()