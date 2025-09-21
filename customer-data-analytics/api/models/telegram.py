"""
Pydantic models for Telegram Bot API
"""

from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from datetime import datetime


class TelegramUserSummary(BaseModel):
    """Модель для полной сводки пользователя для Telegram бота"""
    
    user_id: int
    segment: str
    ltv_12m: float
    last_order_days: Optional[int]
    prob_purchase_30d: float
    prob_churn_60d: float
    recommendations: List[Dict[str, Any]]
    anomalies: List[Dict[str, Any]]
    features: Dict[str, Any]
    snapshot_date: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 123,
                "segment": "VIP",
                "ltv_12m": 4850.50,
                "last_order_days": 7,
                "prob_purchase_30d": 0.85,
                "prob_churn_60d": 0.15,
                "recommendations": [
                    {
                        "product_id": 1,
                        "title": "Premium Headphones",
                        "price": 299.99,
                        "category": "Electronics",
                        "brand": "TechBrand",
                        "rating": 4.8,
                        "score": 95.5,
                        "source": "popularity"
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
                ],
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
        }


class TelegramAvailableUsers(BaseModel):
    """Модель для списка доступных пользователей"""
    
    users: List[Dict[str, Any]]
    total_count: int
    snapshot_date: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "users": [
                    {
                        "user_id": 123,
                        "orders_count": 12,
                        "total_revenue": 4850.50,
                        "last_order_date": "2025-09-14T10:30:00"
                    }
                ],
                "total_count": 1,
                "snapshot_date": "2025-09-21T12:00:00"
            }
        }
