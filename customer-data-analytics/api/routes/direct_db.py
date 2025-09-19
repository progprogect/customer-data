"""
Direct Database Access for Frontend
Прямой доступ к базе данных для фронтенда
"""

from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import logging
import os
import psycopg2
from datetime import datetime

logger = logging.getLogger(__name__)
router = APIRouter()


def get_db_connection():
    """Прямое подключение к PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="customer_data",
            user="mikitavalkunovich",
            port=5432
        )
        return conn
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к БД: {e}")
        raise


@router.get("/database-stats")
async def get_database_statistics() -> Dict[str, Any]:
    """
    Получение общей статистики базы данных для правильного масштабирования
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем последнюю дату
        cursor.execute("SELECT MAX(snapshot_date) FROM ml_user_features_daily_all")
        max_date = cursor.fetchone()[0]
        snapshot_date = max_date.isoformat() if max_date else "2025-09-15"
        
        # Получаем общую статистику
        stats_query = """
        SELECT 
            COUNT(*) as total_users,
            COUNT(*) FILTER (WHERE orders_lifetime >= 1) as users_with_orders,
            COUNT(*) FILTER (WHERE recency_days IS NOT NULL) as users_with_features
        FROM ml_user_features_daily_all 
        WHERE snapshot_date = %s
        """
        
        cursor.execute(stats_query, (snapshot_date,))
        total_users, users_with_orders, users_with_features = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        logger.info(f"📊 Статистика БД: {total_users} всего, {users_with_orders} с заказами")
        
        return {
            "snapshot_date": snapshot_date,
            "total_users_in_db": int(total_users),
            "users_with_orders": int(users_with_orders),
            "users_with_features": int(users_with_features),
            "sample_size_used": 500  # Размер выборки который мы используем
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики БД: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")


@router.get("/direct-users")
async def get_direct_users(limit: int = 500) -> Dict[str, Any]:  # Увеличиваем дефолт до 500
    """
    Прямое получение пользователей из базы данных
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем последнюю дату
        cursor.execute("SELECT MAX(snapshot_date) FROM ml_user_features_daily_all")
        max_date = cursor.fetchone()[0]
        snapshot_date = max_date.isoformat() if max_date else "2025-09-15"
        
        logger.info(f"🔍 Получение {limit} пользователей для snapshot_date={snapshot_date}")
        
        # Запрос реальных пользователей (случайная выборка для репрезентативности)
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
        ORDER BY RANDOM()  -- Случайная выборка для репрезентативности
        LIMIT %s
        """
        
        cursor.execute(query, (snapshot_date, limit))
        rows = cursor.fetchall()
        
        # Преобразуем в формат для ML API
        users_for_ml = []
        users_with_explanations = []
        
        for row in rows:
            user_id, recency, freq_90d, monetary_180d, aov_180d, orders_lifetime, revenue_lifetime, categories = row
            
            # Создаем объект для ML API
            features = {
                "recency_days": float(recency),
                "frequency_90d": int(freq_90d),
                "monetary_180d": float(monetary_180d),
                "aov_180d": float(aov_180d),
                "orders_lifetime": int(orders_lifetime),
                "revenue_lifetime": float(revenue_lifetime),
                "categories_unique": int(categories)
            }
            
            user_for_ml = {
                "user_id": int(user_id),
                "snapshot_date": snapshot_date,
                "features": features
            }
            users_for_ml.append(user_for_ml)
            
            # Создаем объяснение
            explanation = generate_explanation(features)
            
            user_with_explanation = {
                "user_id": int(user_id),
                "snapshot_date": snapshot_date,
                "features": features,
                "explanation": explanation
            }
            users_with_explanations.append(user_with_explanation)
        
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Найдено {len(users_with_explanations)} пользователей")
        
        return {
            "users": users_with_explanations,
            "users_for_ml": users_for_ml,
            "total_count": len(users_with_explanations),
            "snapshot_date": snapshot_date
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных: {str(e)}")


def generate_explanation(features: Dict[str, Any]) -> str:
    """Генерация объяснения для клиента"""
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
    
    # Анализ трат
    monetary_180d = features.get('monetary_180d', 0)
    if monetary_180d >= 1000:
        explanations.append(f"потратил ${int(monetary_180d)} за полгода")
    
    return ", ".join(explanations[:2]) if explanations else "новый клиент"
