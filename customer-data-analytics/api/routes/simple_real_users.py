"""
Simple Real Users API
Упрощенный API для получения реальных пользователей
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Dict, Any, Optional
import logging
import sys
import os
from datetime import datetime, date

# Добавляем путь к shared
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from database.connection import SessionLocal

logger = logging.getLogger(__name__)
router = APIRouter()


def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/simple-real-users")
async def get_simple_real_users(
    limit: int = Query(100, description="Максимальное количество пользователей", le=1000),
    snapshot_date: Optional[str] = Query(None, description="Дата снапшота (YYYY-MM-DD)"),
    db = Depends(get_db)
) -> Dict[str, Any]:
    """
    Получение реальных пользователей с их фичами (упрощенная версия)
    """
    try:
        # Используем последнюю доступную дату если не указана
        if snapshot_date is None:
            # Получаем последнюю дату из базы
            from sqlalchemy import text
            date_result = db.execute(text("SELECT MAX(snapshot_date) FROM ml_user_features_daily_all"))
            max_date_row = date_result.fetchone()
            snapshot_date = max_date_row[0].isoformat() if max_date_row and max_date_row[0] else "2025-09-15"
        
        logger.info(f"🔍 Получение {limit} пользователей для snapshot_date={snapshot_date}")
        
        # Простой SQL запрос
        query = """
        SELECT 
            f.user_id,
            COALESCE(f.recency_days, 0) as recency_days,
            COALESCE(f.frequency_90d, 0) as frequency_90d,
            COALESCE(f.monetary_180d, 0) as monetary_180d,
            COALESCE(f.aov_180d, 0) as aov_180d,
            COALESCE(f.orders_lifetime, 0) as orders_lifetime,
            COALESCE(f.revenue_lifetime, 0) as revenue_lifetime,
            COALESCE(f.categories_unique, 0) as categories_unique
        FROM ml_user_features_daily_all f
        WHERE f.snapshot_date = %s
          AND f.orders_lifetime >= 1
        ORDER BY f.orders_lifetime DESC, f.monetary_180d DESC
        LIMIT %s
        """
        
        from sqlalchemy import text
        result = db.execute(text(query), (snapshot_date, limit))
        rows = result.fetchall()
        
        if not rows:
            return {
                "users": [],
                "total_count": 0,
                "snapshot_date": snapshot_date,
                "message": "Нет данных для указанной даты"
            }
        
        # Преобразуем в простой формат
        users = []
        for row in rows:
            user_data = {
                "user_id": int(row[0]),
                "snapshot_date": snapshot_date,
                "features": {
                    "recency_days": float(row[1]),
                    "frequency_90d": int(row[2]),
                    "monetary_180d": float(row[3]),
                    "aov_180d": float(row[4]),
                    "orders_lifetime": int(row[5]),
                    "revenue_lifetime": float(row[6]),
                    "categories_unique": int(row[7])
                },
                "explanation": generate_simple_explanation({
                    "recency_days": float(row[1]),
                    "frequency_90d": int(row[2]),
                    "monetary_180d": float(row[3]),
                    "aov_180d": float(row[4]),
                    "orders_lifetime": int(row[5]),
                    "revenue_lifetime": float(row[6]),
                    "categories_unique": int(row[7])
                })
            }
            users.append(user_data)
        
        logger.info(f"✅ Найдено {len(users)} пользователей")
        
        return {
            "users": users,
            "total_count": len(users),
            "snapshot_date": snapshot_date
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных: {str(e)}")


def generate_simple_explanation(features: Dict[str, Any]) -> str:
    """Генерация простого объяснения для клиента"""
    explanations = []
    
    # Анализ лояльности
    orders_lifetime = features.get('orders_lifetime', 0)
    if orders_lifetime >= 10:
        explanations.append(f"{orders_lifetime} заказов за всё время")
    elif orders_lifetime >= 5:
        explanations.append(f"активный клиент ({orders_lifetime} заказов)")
    elif orders_lifetime >= 2:
        explanations.append(f"повторный клиент ({orders_lifetime} заказа)")
    
    # Анализ недавней активности
    recency_days = features.get('recency_days', 999)
    if recency_days <= 30:
        explanations.append(f"покупал недавно ({int(recency_days)} дней назад)")
    elif recency_days <= 90:
        explanations.append(f"покупал в последние 3 месяца")
    
    # Анализ разнообразия
    categories_unique = features.get('categories_unique', 0)
    if categories_unique >= 5:
        explanations.append(f"покупает в {categories_unique} категориях")
    elif categories_unique >= 3:
        explanations.append(f"разнообразные покупки")
    
    return ", ".join(explanations[:2]) if explanations else "новый клиент"
