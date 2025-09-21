"""
Telegram Bot API Routes
Специальные эндпоинты для Telegram Marketing Assistant
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
from sqlalchemy.orm import Session
from sqlalchemy import text
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from database.connection import SessionLocal
from models.telegram import TelegramUserSummary

router = APIRouter(prefix="/api/v1/telegram", tags=["telegram"])
logger = logging.getLogger(__name__)


@router.get("/user-summary/{user_id}")
async def get_user_summary_for_telegram(
    user_id: int
) -> Dict[str, Any]:
    """
    Получение полной сводки пользователя для Telegram бота
    
    Возвращает все необходимые данные для отображения в Telegram:
    - LTV и последняя покупка
    - Сегмент пользователя
    - Вероятность покупки (ML предсказание)
    - Риск оттока (ML предсказание)
    - Рекомендации товаров
    - Аномалии
    - Features для ML
    """
    try:
        db = SessionLocal()
        
        # Получаем базовую информацию о пользователе
        user_data = await _get_user_basic_info(db, user_id)
        
        # Получаем LTV данные
        ltv_data = await _get_user_ltv(db, user_id)
        
        # Получаем сегмент пользователя
        segment = await _get_user_segment(db, user_id)
        
        # Получаем features для ML
        features = await _get_user_features(db, user_id)
        
        # Получаем ML предсказания
        ml_predictions = await _get_ml_predictions(user_id, features)
        
        # Получаем рекомендации
        recommendations = await _get_user_recommendations(db, user_id)
        
        # Получаем аномалии
        anomalies = await _get_user_anomalies(db, user_id)
        
        # Формируем итоговый ответ
        summary = {
            "user_id": user_id,
            "segment": segment if segment != "Новый" else "Новые/Неактивные",
            "ltv_12m": ltv_data.get("revenue_12m", 0),
            "last_order_days": ltv_data.get("days_since_last_order"),
            "prob_purchase_30d": ml_predictions.get("prob_purchase") if ml_predictions.get("prob_purchase") is not None else None,
            "prob_churn_60d": ml_predictions.get("prob_churn") if ml_predictions.get("prob_churn") is not None else None,
            "recommendations": recommendations,
            "anomalies": anomalies,
            "features": features,
            "snapshot_date": datetime.now().isoformat()
        }
        
        logger.info(f"Generated user summary for user {user_id}")
        return summary
        
    except Exception as e:
        logger.error(f"Error generating user summary for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения сводки пользователя: {str(e)}")


async def _get_user_basic_info(db: Session, user_id: int) -> Dict[str, Any]:
    """Получение базовой информации о пользователе"""
    try:
        query = text("""
            SELECT 
                user_id,
                created_at,
                (SELECT COUNT(*) FROM orders WHERE user_id = :user_id) as orders_count,
                (SELECT SUM(total_amount) FROM orders WHERE user_id = :user_id) as total_revenue
            FROM users 
            WHERE user_id = :user_id
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            return {
                "user_id": result.user_id,
                "created_at": result.created_at,
                "orders_count": result.orders_count or 0,
                "total_revenue": result.total_revenue or 0
            }
        else:
            return {"user_id": user_id, "orders_count": 0, "total_revenue": 0}
            
    except Exception as e:
        logger.warning(f"Error getting basic info for user {user_id}: {e}")
        return {"user_id": user_id, "orders_count": 0, "total_revenue": 0}


async def _get_user_ltv(db: Session, user_id: int) -> Dict[str, Any]:
    """Получение LTV данных пользователя"""
    try:
        # LTV за последние 12 месяцев
        query_ltv = text("""
            SELECT 
                COALESCE(SUM(total_amount), 0) as revenue_12m,
                MAX(order_date) as last_order_date
            FROM orders 
            WHERE user_id = :user_id 
            AND order_date >= CURRENT_DATE - INTERVAL '12 months'
        """)
        
        result = db.execute(query_ltv, {"user_id": user_id}).fetchone()
        
        if result:
            revenue_12m = result.revenue_12m or 0
            last_order_date = result.last_order_date
            
            # Вычисляем дни с последней покупки
            days_since_last_order = None
            if last_order_date:
                days_since_last_order = (datetime.now().date() - last_order_date).days
            
            return {
                "revenue_12m": float(revenue_12m),
                "last_order_date": last_order_date,
                "days_since_last_order": days_since_last_order
            }
        else:
            return {"revenue_12m": 0, "last_order_date": None, "days_since_last_order": None}
            
    except Exception as e:
        logger.warning(f"Error getting LTV for user {user_id}: {e}")
        return {"revenue_12m": 0, "last_order_date": None, "days_since_last_order": None}


async def _get_user_segment(db: Session, user_id: int) -> str:
    """Определение сегмента пользователя"""
    try:
        # Получаем статистику пользователя
        query = text("""
            SELECT 
                COUNT(*) as orders_count,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                MAX(order_date) as last_order_date
            FROM orders 
            WHERE user_id = :user_id
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            orders_count = result.orders_count or 0
            total_revenue = result.total_revenue or 0
            last_order_date = result.last_order_date
            
            # Определяем сегмент на основе активности и трат
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
            return "Новый"
            
    except Exception as e:
        logger.warning(f"Error getting segment for user {user_id}: {e}")
        return "Новый"


async def _get_user_features(db: Session, user_id: int) -> Dict[str, Any]:
    """Получение features пользователя для ML"""
    try:
        # Вычисляем основные features
        query = text("""
            SELECT 
                -- Recency: дни с последней покупки
                COALESCE(
                    EXTRACT(DAY FROM (CURRENT_DATE - MAX(order_date))), 
                    999
                ) as recency_days,
                
                -- Frequency: количество заказов за последние 90 дней
                COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 END) as frequency_90d,
                
                -- Monetary: сумма за последние 180 дней
                COALESCE(
                    SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '180 days' THEN total_amount END), 
                    0
                ) as monetary_180d,
                
                -- AOV за последние 180 дней
                COALESCE(
                    AVG(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '180 days' THEN total_amount END), 
                    0
                ) as aov_180d,
                
                -- Общее количество заказов
                COUNT(*) as orders_lifetime,
                
                -- Общая выручка
                COALESCE(SUM(total_amount), 0) as revenue_lifetime,
                
                -- Количество уникальных категорий
                COUNT(DISTINCT category_id) as categories_unique
                
            FROM orders 
            WHERE user_id = :user_id
        """)
        
        result = db.execute(query, {"user_id": user_id}).fetchone()
        
        if result:
            return {
                "recency_days": float(result.recency_days or 999),
                "frequency_90d": int(result.frequency_90d or 0),
                "monetary_180d": float(result.monetary_180d or 0),
                "aov_180d": float(result.aov_180d or 0),
                "orders_lifetime": int(result.orders_lifetime or 0),
                "revenue_lifetime": float(result.revenue_lifetime or 0),
                "categories_unique": int(result.categories_unique or 0)
            }
        else:
            # Возвращаем значения по умолчанию для новых пользователей
            return {
                "recency_days": 999.0,
                "frequency_90d": 0,
                "monetary_180d": 0.0,
                "aov_180d": 0.0,
                "orders_lifetime": 0,
                "revenue_lifetime": 0.0,
                "categories_unique": 0
            }
            
    except Exception as e:
        logger.warning(f"Error getting features for user {user_id}: {e}")
        return {
            "recency_days": 999.0,
            "frequency_90d": 0,
            "monetary_180d": 0.0,
            "aov_180d": 0.0,
            "orders_lifetime": 0,
            "revenue_lifetime": 0.0,
            "categories_unique": 0
        }


async def _get_ml_predictions(user_id: int, features: Dict[str, Any]) -> Dict[str, Any]:
    """Получение ML предсказаний"""
    try:
        # Проверяем, есть ли реальные features
        if not features or features.get("recency_days") is None:
            logger.info(f"No features available for user {user_id}, returning None for predictions")
            return {"prob_purchase": None, "prob_churn": None}
        
        recency_days = features.get("recency_days", 999)
        frequency_90d = features.get("frequency_90d", 0)
        monetary_180d = features.get("monetary_180d", 0)
        
        # Простая эвристика для вероятности покупки
        # Чем больше частота и меньше давность - тем выше вероятность
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
        # Чем больше давность - тем выше риск
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


async def _get_user_recommendations(db: Session, user_id: int, limit: int = 3) -> List[Dict[str, Any]]:
    """Получение рекомендаций для пользователя"""
    try:
        # Простая логика рекомендаций на основе популярных товаров
        # В будущем можно заменить на вызов ML API рекомендаций
        
        query = text("""
            SELECT 
                p.product_id,
                p.name as title,
                p.price,
                p.category,
                p.brand,
                p.rating,
                COUNT(o.order_id) as popularity_score
            FROM products p
            LEFT JOIN order_items oi ON p.product_id = oi.product_id
            LEFT JOIN orders o ON oi.order_id = o.order_id
            WHERE p.product_id NOT IN (
                SELECT DISTINCT oi2.product_id 
                FROM order_items oi2 
                JOIN orders o2 ON oi2.order_id = o2.order_id 
                WHERE o2.user_id = :user_id
            )
            GROUP BY p.product_id, p.name, p.price, p.category, p.brand, p.rating
            ORDER BY popularity_score DESC, p.rating DESC
            LIMIT :limit
        """)
        
        results = db.execute(query, {"user_id": user_id, "limit": limit}).fetchall()
        
        recommendations = []
        for result in results:
            recommendations.append({
                "product_id": result.product_id,
                "title": result.title,
                "price": float(result.price or 0),
                "category": result.category,
                "brand": result.brand,
                "rating": float(result.rating or 0),
                "score": float(result.popularity_score or 0),
                "source": "popularity"
            })
        
        return recommendations
        
    except Exception as e:
        logger.warning(f"Error getting recommendations for user {user_id}: {e}")
        return []


async def _get_user_anomalies(db: Session, user_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    """Получение аномалий пользователя"""
    try:
        # Простая логика поиска аномалий на основе статистики
        # В будущем можно заменить на вызов ML API аномалий
        
        query = text("""
            SELECT 
                DATE_TRUNC('week', order_date) as week_start,
                COUNT(*) as orders_count,
                SUM(total_amount) as total_amount,
                AVG(total_amount) as avg_amount
            FROM orders 
            WHERE user_id = :user_id
            AND order_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY DATE_TRUNC('week', order_date)
            ORDER BY week_start DESC
            LIMIT 10
        """)
        
        results = db.execute(query, {"user_id": user_id}).fetchall()
        
        if len(results) < 2:
            return []  # Недостаточно данных для анализа
        
        # Вычисляем средние значения для сравнения
        all_amounts = [float(r.total_amount or 0) for r in results]
        all_counts = [int(r.orders_count or 0) for r in results]
        
        avg_amount = sum(all_amounts) / len(all_amounts) if all_amounts else 0
        avg_count = sum(all_counts) / len(all_counts) if all_counts else 0
        
        anomalies = []
        for result in results:
            week_amount = float(result.total_amount or 0)
            week_count = int(result.orders_count or 0)
            
            # Ищем аномалии (значения больше чем в 2 раза от среднего)
            if week_amount > avg_amount * 2 or week_count > avg_count * 2:
                anomalies.append({
                    "week_start": result.week_start.isoformat(),
                    "anomaly_score": min(99.9, (week_amount / avg_amount * 50) if avg_amount > 0 else 0),
                    "triggers": [
                        "spending_spike" if week_amount > avg_amount * 2 else None,
                        "orders_spike" if week_count > avg_count * 2 else None
                    ],
                    "details": {
                        "orders_count": week_count,
                        "total_amount": week_amount,
                        "avg_amount": float(result.avg_amount or 0)
                    }
                })
        
        return anomalies[:limit]
        
    except Exception as e:
        logger.warning(f"Error getting anomalies for user {user_id}: {e}")
        return []


@router.get("/users/available")
async def get_available_users(
    limit: int = Query(20, description="Количество пользователей")
) -> Dict[str, Any]:
    """
    Получение списка доступных пользователей для тестирования
    """
    try:
        db = SessionLocal()
        
        query = text("""
            SELECT 
                u.user_id,
                COUNT(o.order_id) as orders_count,
                COALESCE(SUM(o.total_amount), 0) as total_revenue,
                MAX(o.order_date) as last_order_date
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            GROUP BY u.user_id
            HAVING COUNT(o.order_id) > 0
            ORDER BY total_revenue DESC, orders_count DESC
            LIMIT :limit
        """)
        
        results = db.execute(query, {"limit": limit}).fetchall()
        
        users = []
        for result in results:
            users.append({
                "user_id": result.user_id,
                "orders_count": result.orders_count,
                "total_revenue": float(result.total_revenue),
                "last_order_date": result.last_order_date.isoformat() if result.last_order_date else None
            })
        
        return {
            "users": users,
            "total_count": len(users),
            "snapshot_date": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting available users: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения списка пользователей: {str(e)}")
