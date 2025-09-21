"""
Простой Telegram Bot API Routes
Без сложных зависимостей
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any
import logging

router = APIRouter(prefix="/api/v1/telegram", tags=["telegram"])
logger = logging.getLogger(__name__)


@router.get("/user-summary/{user_id}")
async def get_user_summary_for_telegram(user_id: int) -> Dict[str, Any]:
    """
    Получение полной сводки пользователя для Telegram бота
    Простая версия с моковыми данными для тестирования
    """
    try:
        # Пока возвращаем моковые данные для тестирования
        summary = {
            "user_id": user_id,
            "segment": "VIP" if user_id % 3 == 0 else "Обычный",
            "ltv_12m": 4850.50 + (user_id * 100),
            "last_order_days": 7 if user_id % 2 == 0 else 15,
            "prob_purchase_30d": 0.85 if user_id % 3 == 0 else 0.65,
            "prob_churn_60d": 0.15 if user_id % 3 == 0 else 0.35,
            "recommendations": [
                {
                    "product_id": 1,
                    "title": f"Premium Product for User {user_id}",
                    "price": 299.99,
                    "category": "Electronics",
                    "brand": "TechBrand",
                    "rating": 4.8,
                    "score": 95.5,
                    "source": "popularity"
                },
                {
                    "product_id": 2,
                    "title": f"Accessory for User {user_id}",
                    "price": 49.99,
                    "category": "Accessories",
                    "brand": "AccessoryBrand",
                    "rating": 4.5,
                    "score": 88.0,
                    "source": "collaborative"
                }
            ],
            "anomalies": [
                {
                    "week_start": "2025-09-15T00:00:00",
                    "anomaly_score": 87.3,
                    "triggers": ["spending_spike", "orders_spike"],
                    "details": {
                        "orders_count": 8,
                        "total_amount": 2400.00,
                        "avg_amount": 300.00
                    }
                }
            ] if user_id % 4 == 0 else [],
            "features": {
                "recency_days": 7.0,
                "frequency_90d": 5,
                "monetary_180d": 3200.00,
                "aov_180d": 640.00,
                "orders_lifetime": 12,
                "revenue_lifetime": 4850.50,
                "categories_unique": 8
            },
            "snapshot_date": "2025-09-21T12:00:00"
        }
        
        logger.info(f"Generated user summary for user {user_id}")
        return summary
        
    except Exception as e:
        logger.error(f"Error generating user summary for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения сводки пользователя: {str(e)}")


@router.get("/users/available")
async def get_available_users() -> Dict[str, Any]:
    """
    Получение списка доступных пользователей для тестирования
    """
    try:
        # Возвращаем список тестовых пользователей
        users = []
        for i in range(1, 21):
            users.append({
                "user_id": i,
                "orders_count": i * 2,
                "total_revenue": float(i * 500),
                "last_order_date": "2025-09-14T10:30:00"
            })
        
        return {
            "users": users,
            "total_count": len(users),
            "snapshot_date": "2025-09-21T12:00:00"
        }
        
    except Exception as e:
        logger.error(f"Error getting available users: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения списка пользователей: {str(e)}")
