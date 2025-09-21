"""
Telegram Bot API - Прямая работа с БД
Получает данные напрямую из таблиц БД без внешних API вызовов
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
import os
import sys

# Добавляем путь для импорта shared модулей
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))

from database.connection import SessionLocal
from sqlalchemy import text

router = APIRouter(prefix="/api/v1/telegram-db", tags=["telegram-db"])
logger = logging.getLogger(__name__)

@router.get("/user-summary/{user_id}")
async def get_user_summary_for_telegram_db(
    user_id: int
) -> Dict[str, Any]:
    """
    Получение полной сводки пользователя для Telegram бота напрямую из БД
    
    Возвращает все необходимые данные для отображения в Telegram:
    - LTV и последняя покупка
    - Сегмент пользователя
    - Вероятность покупки (эвристика)
    - Риск оттока (эвристика)
    - Рекомендации товаров
    - Аномалии
    """
    try:
        db = SessionLocal()
        
        # 1. Получаем базовую информацию о пользователе
        user_data = await _get_user_basic_info_db(db, user_id)
        
        # 2. Получаем LTV данные
        ltv_data = await _get_user_ltv_db(db, user_id)
        
        # 3. Получаем сегмент пользователя
        segment = await _get_user_segment_db(db, user_id)
        
        # 4. Получаем features для ML
        features = await _get_user_features_db(db, user_id)
        
        # 5. Получаем ML предсказания (эвристика)
        ml_predictions = await _get_ml_predictions_db(user_id, features)
        
        # 6. Получаем рекомендации
        recommendations = await _get_user_recommendations_db(db, user_id)
        
        # 7. Получаем аномалии
        anomalies = await _get_user_anomalies_db(db, user_id)
        
        # Формируем итоговый ответ
        summary = {
            "user_id": user_id,
            "segment": segment,
            "ltv_12m": ltv_data.get("revenue_12m", 0),
            "last_order_days": ltv_data.get("days_since_last_order"),
            "prob_purchase_30d": ml_predictions.get("prob_purchase"),
            "prob_churn_60d": ml_predictions.get("prob_churn"),
            "recommendations": recommendations,
            "anomalies": anomalies,
            "features": features,
            "snapshot_date": datetime.now().isoformat(),
            "source": "telegram_db_direct"  # Уникальный идентификатор
        }
        
        logger.info(f"Generated user summary for user {user_id} from DB: ltv={ltv_data.get('revenue_12m')}, segment={segment}")
        return summary
        
    except Exception as e:
        logger.error(f"Error generating user summary for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения сводки пользователя: {str(e)}")
    finally:
        db.close()

@router.get("/users/available")
async def get_available_users_db(
    limit: int = 50
) -> Dict[str, Any]:
    """
    Получение списка доступных пользователей для поиска
    """
    try:
        db = SessionLocal()
        
        query = text("""
            SELECT DISTINCT u.user_id
            FROM users u
            LEFT JOIN ml_user_ltv ltv ON u.user_id = ltv.user_id
            WHERE ltv.user_id IS NOT NULL
            ORDER BY u.user_id
            LIMIT :limit
        """)
        
        result = db.execute(query, {"limit": limit}).fetchall()
        user_ids = [row[0] for row in result]
        
        return {
            "users": user_ids,
            "total": len(user_ids),
            "limit": limit
        }
        
    except Exception as e:
        logger.error(f"Error getting available users: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения списка пользователей: {str(e)}")
    finally:
        db.close()

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

async def _get_user_basic_info_db(db, user_id: int) -> Dict[str, Any]:
    """Получение базовой информации о пользователе"""
    try:
        query = text("""
            SELECT 
                user_id,
                registered_at,
                country,
                city
            FROM users 
            WHERE user_id = :user_id
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            return {
                "user_id": result.user_id,
                "registered_at": result.registered_at,
                "country": result.country,
                "city": result.city
            }
        else:
            return {}
            
    except Exception as e:
        logger.warning(f"Error getting basic info for user {user_id}: {e}")
        return {}

async def _get_user_ltv_db(db, user_id: int) -> Dict[str, Any]:
    """Получение LTV данных пользователя"""
    try:
        query = text("""
            SELECT 
                revenue_12m,
                last_order_date,
                days_since_last_order,
                orders_12m
            FROM ml_user_ltv 
            WHERE user_id = :user_id
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            return {
                "revenue_12m": float(result.revenue_12m) if result.revenue_12m else 0,
                "last_order_date": result.last_order_date,
                "days_since_last_order": result.days_since_last_order,
                "orders_12m": result.orders_12m or 0
            }
        else:
            # Если нет данных в ml_user_ltv, рассчитываем из orders
            return await _calculate_ltv_from_orders(db, user_id)
            
    except Exception as e:
        logger.warning(f"Error getting LTV for user {user_id}: {e}")
        return {"revenue_12m": 0, "last_order_date": None, "days_since_last_order": None}

async def _calculate_ltv_from_orders(db, user_id: int) -> Dict[str, Any]:
    """Расчет LTV из таблицы orders если нет данных в ml_user_ltv"""
    try:
        query = text("""
            SELECT 
                COALESCE(SUM(total_amount), 0) as revenue_12m,
                MAX(created_at) as last_order_date,
                COUNT(*) as orders_12m
            FROM orders 
            WHERE user_id = :user_id 
            AND created_at >= CURRENT_DATE - INTERVAL '12 months'
            AND status = 'completed'
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            days_since = None
            if result.last_order_date:
                days_since = (datetime.now().date() - result.last_order_date.date()).days
            
            return {
                "revenue_12m": float(result.revenue_12m) if result.revenue_12m else 0,
                "last_order_date": result.last_order_date,
                "days_since_last_order": days_since,
                "orders_12m": result.orders_12m or 0
            }
        else:
            return {"revenue_12m": 0, "last_order_date": None, "days_since_last_order": None, "orders_12m": 0}
            
    except Exception as e:
        logger.warning(f"Error calculating LTV from orders for user {user_id}: {e}")
        return {"revenue_12m": 0, "last_order_date": None, "days_since_last_order": None, "orders_12m": 0}

async def _get_user_segment_db(db, user_id: int) -> str:
    """Определение сегмента пользователя из таблицы user_segments_kmeans"""
    try:
        # Сначала пытаемся получить из таблицы сегментов
        query = text("""
            SELECT cluster_id
            FROM user_segments_kmeans 
            WHERE user_id = :user_id
            ORDER BY snapshot_date DESC
            LIMIT 1
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            cluster_id = result.cluster_id
            # Маппинг кластеров на названия сегментов
            segment_mapping = {
                0: "Новые/Неактивные",
                1: "Обычные", 
                2: "VIP"
            }
            return segment_mapping.get(cluster_id, "Новые/Неактивные")
        
        # Если нет данных о сегментах, определяем по статистике
        return await _calculate_segment_from_stats(db, user_id)
            
    except Exception as e:
        logger.warning(f"Error getting segment for user {user_id}: {e}")
        return "Новые/Неактивные"

async def _calculate_segment_from_stats(db, user_id: int) -> str:
    """Расчет сегмента по статистике заказов"""
    try:
        query = text("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as total_revenue
            FROM orders 
            WHERE user_id = :user_id
            AND status = 'completed'
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            orders_count = result.orders_count or 0
            total_revenue = float(result.total_revenue) if result.total_revenue else 0
            
            # Логика определения сегмента
            if orders_count == 0:
                return "Новые/Неактивные"
            elif total_revenue >= 5000 and orders_count >= 10:
                return "VIP"
            elif total_revenue >= 2000 or orders_count >= 5:
                return "Обычные"
            elif total_revenue >= 500 or orders_count >= 2:
                return "Обычные"
            else:
                return "Новые/Неактивные"
        else:
            return "Новые/Неактивные"
            
    except Exception as e:
        logger.warning(f"Error calculating segment from stats for user {user_id}: {e}")
        return "Новые/Неактивные"

async def _get_user_features_db(db, user_id: int) -> Dict[str, Any]:
    """Получение features пользователя из таблицы ml_user_features_daily_all"""
    try:
        query = text("""
            SELECT 
                recency_days,
                frequency_90d,
                monetary_180d,
                aov_180d,
                orders_lifetime,
                revenue_lifetime,
                categories_unique
            FROM ml_user_features_daily_all 
            WHERE user_id = :user_id
            ORDER BY snapshot_date DESC
            LIMIT 1
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            return {
                "recency_days": result.recency_days,
                "frequency_90d": result.frequency_90d,
                "monetary_180d": float(result.monetary_180d) if result.monetary_180d else 0,
                "aov_180d": float(result.aov_180d) if result.aov_180d else 0,
                "orders_lifetime": result.orders_lifetime,
                "revenue_lifetime": float(result.revenue_lifetime) if result.revenue_lifetime else 0,
                "categories_unique": result.categories_unique
            }
        else:
            return {}
            
    except Exception as e:
        logger.warning(f"Error getting features for user {user_id}: {e}")
        return {}

async def _get_ml_predictions_db(user_id: int, features: Dict[str, Any]) -> Dict[str, Any]:
    """Получение ML предсказаний на основе features"""
    try:
        # Проверяем, есть ли реальные features
        if not features or features.get("recency_days") is None:
            logger.info(f"No features available for user {user_id}, returning None for predictions")
            return {"prob_purchase": None, "prob_churn": None}
        
        recency_days = features.get("recency_days", 999)
        frequency_90d = features.get("frequency_90d", 0)
        monetary_180d = features.get("monetary_180d", 0)
        
        # Простая эвристика для вероятности покупки
        if frequency_90d == 0:
            prob_purchase = 0.1  # Нет активности
        elif recency_days <= 7:
            prob_purchase = 0.8  # Недавно покупал
        elif recency_days <= 30:
            prob_purchase = 0.6  # Покупал в течение месяца
        elif recency_days <= 90:
            prob_purchase = 0.4  # Покупал в течение квартала
        else:
            prob_purchase = 0.2  # Давно не покупал
        
        # Корректируем на основе частоты
        if frequency_90d >= 5:
            prob_purchase = min(0.9, prob_purchase + 0.2)
        elif frequency_90d >= 3:
            prob_purchase = min(0.8, prob_purchase + 0.1)
        
        # Простая эвристика для риска оттока
        if recency_days <= 7:
            prob_churn = 0.1  # Недавно покупал
        elif recency_days <= 30:
            prob_churn = 0.3  # Покупал в течение месяца
        elif recency_days <= 90:
            prob_churn = 0.6  # Покупал в течение квартала
        else:
            prob_churn = 0.8  # Давно не покупал
        
        # Корректируем на основе частоты
        if frequency_90d >= 5:
            prob_churn = max(0.1, prob_churn - 0.3)
        elif frequency_90d >= 3:
            prob_churn = max(0.2, prob_churn - 0.2)
        
        return {
            "prob_purchase": round(prob_purchase, 2),
            "prob_churn": round(prob_churn, 2)
        }
        
    except Exception as e:
        logger.warning(f"Error getting ML predictions for user {user_id}: {e}")
        return {"prob_purchase": None, "prob_churn": None}

async def _get_user_recommendations_db(db, user_id: int) -> List[Dict[str, Any]]:
    """Получение рекомендаций товаров для пользователя"""
    try:
        query = text("""
            SELECT 
                p.product_id,
                p.title,
                p.price,
                p.category,
                p.brand,
                p.rating,
                COUNT(oi.order_item_id) as popularity_score
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE p.product_id NOT IN (
                SELECT DISTINCT oi2.product_id 
                FROM order_items oi2 
                JOIN orders o2 ON oi2.order_id = o2.order_id 
                WHERE o2.user_id = :user_id
                AND o2.status = 'completed'
            )
            AND p.is_active = true
            GROUP BY p.product_id, p.title, p.price, p.category, p.brand, p.rating
            ORDER BY popularity_score DESC, p.rating DESC
            LIMIT 3
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchall()
        
        recommendations = []
        for row in result:
            recommendations.append({
                "product_id": row.product_id,
                "title": row.title,
                "price": float(row.price) if row.price else 0,
                "category": row.category,
                "brand": row.brand,
                "rating": float(row.rating) if row.rating else 0,
                "popularity_score": row.popularity_score or 0
            })
        
        return recommendations
        
    except Exception as e:
        logger.warning(f"Error getting recommendations for user {user_id}: {e}")
        return []

async def _get_user_anomalies_db(db, user_id: int) -> List[Dict[str, Any]]:
    """Получение аномалий пользователя из таблицы ml_user_anomalies_weekly"""
    try:
        query = text("""
            SELECT 
                week_start,
                anomaly_score,
                is_anomaly,
                triggers
            FROM ml_user_anomalies_weekly 
            WHERE user_id = :user_id
            AND week_start >= CURRENT_DATE - INTERVAL '90 days'
            AND is_anomaly = true
            ORDER BY week_start DESC
            LIMIT 5
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchall()
        
        anomalies = []
        for row in result:
            anomalies.append({
                "week_start": row.week_start.isoformat() if row.week_start else None,
                "anomaly_score": float(row.anomaly_score) if row.anomaly_score else 0,
                "is_anomaly": row.is_anomaly,
                "triggers": row.triggers if row.triggers else []
            })
        
        return anomalies
        
    except Exception as e:
        logger.warning(f"Error getting anomalies for user {user_id}: {e}")
        return []
