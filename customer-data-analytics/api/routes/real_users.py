"""
Real Users API for ML Predictions
API для получения реальных пользователей с их фичами
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

# Импорт моделей
from models.ml_models import UserFeatures, PredictionRow

logger = logging.getLogger(__name__)
router = APIRouter()


def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get(
    "/real-users-with-features",
    summary="Get real users with ML features",
    description="Получение реальных пользователей с их ML фичами для предсказаний"
)
async def get_real_users_with_features(
    limit: int = Query(100, description="Максимальное количество пользователей", le=1000),
    min_orders: int = Query(1, description="Минимальное количество заказов", ge=0),
    snapshot_date: Optional[str] = Query(None, description="Дата снапшота (YYYY-MM-DD), по умолчанию сегодня"),
    db = Depends(get_db)
) -> Dict[str, Any]:
    """
    Получение реальных пользователей с их ML фичами
    
    Returns:
        Dict с пользователями и их фичами для ML предсказаний
    """
    try:
        # Используем текущую дату если не указана
        if snapshot_date is None:
            snapshot_date = datetime.now().date().isoformat()
        else:
            # Валидация даты
            try:
                datetime.fromisoformat(snapshot_date).date()
            except ValueError:
                raise HTTPException(status_code=400, detail="Неверный формат даты, используйте YYYY-MM-DD")
        
        logger.info(f"🔍 Получение {limit} пользователей с features для snapshot_date={snapshot_date}")
        
        # SQL запрос для получения пользователей с их фичами
        query = """
        SELECT 
            f.user_id,
            f.recency_days,
            f.frequency_90d,
            f.monetary_180d,
            f.aov_180d,
            f.orders_lifetime,
            f.revenue_lifetime,
            f.categories_unique,
            u.created_at as user_registered_at,
            -- Дополнительная информация для объяснений
            f.snapshot_date,
            (SELECT COUNT(*) FROM orders o WHERE o.user_id = f.user_id AND o.status IN ('paid', 'shipped', 'completed')) as total_orders_count,
            (SELECT MAX(o.created_at::date) FROM orders o WHERE o.user_id = f.user_id AND o.status IN ('paid', 'shipped', 'completed')) as last_order_date,
            (SELECT SUM(oi.quantity * oi.price) 
             FROM orders o 
             JOIN order_items oi ON o.order_id = oi.order_id 
             WHERE o.user_id = f.user_id AND o.status IN ('paid', 'shipped', 'completed')
            ) as total_lifetime_spent
        FROM ml_user_features_daily_all f
        JOIN users u ON f.user_id = u.user_id
        WHERE f.snapshot_date = %s
          AND f.orders_lifetime >= %s
          AND f.recency_days IS NOT NULL
          AND f.frequency_90d IS NOT NULL
          AND f.monetary_180d IS NOT NULL
        ORDER BY f.orders_lifetime DESC, f.monetary_180d DESC
        LIMIT %s
        """
        
        # Выполняем запрос  
        from sqlalchemy import text
        result = db.execute(text(query), (snapshot_date, min_orders, limit))
        rows = result.fetchall()
        
        if not rows:
            logger.warning(f"⚠️ Нет пользователей с фичами на дату {snapshot_date}")
            return {
                "users": [],
                "total_count": 0,
                "snapshot_date": snapshot_date,
                "message": f"Нет данных для snapshot_date={snapshot_date}. Проверьте что данные сгенерированы."
            }
        
        # Преобразуем в нужный формат
        users = []
        for row in rows:
            # Создаем словарь фичей (row is a tuple-like object)
            features_dict = {
                "recency_days": float(row[1]) if row[1] is not None else None,
                "frequency_90d": int(row[2]) if row[2] is not None else None,
                "monetary_180d": float(row[3]) if row[3] is not None else None,
                "aov_180d": float(row[4]) if row[4] is not None else None,
                "orders_lifetime": int(row[5]) if row[5] is not None else None,
                "revenue_lifetime": float(row[6]) if row[6] is not None else None,
                "categories_unique": int(row[7]) if row[7] is not None else None,
            }
            
            # Создаем объект фичей
            features = UserFeatures(**features_dict)
            
            # Создаем строку для предсказания
            prediction_row = PredictionRow(
                user_id=row[0],  # user_id
                snapshot_date=snapshot_date,
                features=features
            )
            
            # Дополнительная информация для human-readable объяснений
            user_info = {
                "prediction_row": prediction_row.dict(),
                "user_registered_at": row[8].isoformat() if row[8] else None,  # user_registered_at
                "last_order_date": row[11].isoformat() if row[11] else None,  # last_order_date
                "total_orders_count": row[10] or 0,  # total_orders_count
                "total_lifetime_spent": float(row[12]) if row[12] else 0.0,  # total_lifetime_spent
                
                # Расчетные поля для объяснений
                "days_since_registration": (
                    datetime.fromisoformat(snapshot_date).date() - row[8].date()
                ).days if row[8] else None,
                
                "days_since_last_order": row[1],  # recency_days
                
                # Профиль клиента для объяснений
                "customer_profile": get_customer_profile_description(features_dict, row)
            }
            
            users.append(user_info)
        
        logger.info(f"✅ Найдено {len(users)} пользователей с реальными фичами")
        
        return {
            "users": users,
            "total_count": len(users),
            "snapshot_date": snapshot_date,
            "parameters": {
                "limit": limit,
                "min_orders": min_orders,
                "snapshot_date": snapshot_date
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей с фичами: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения данных: {str(e)}")


def get_customer_profile_description(features: Dict[str, Any], row) -> str:
    """
    Генерация человеко-понятного описания профиля клиента
    на основе реальных данных
    """
    explanations = []
    
    # Анализ лояльности (orders_lifetime)
    orders_lifetime = features.get('orders_lifetime', 0) or 0
    if orders_lifetime >= 20:
        explanations.append(f"очень лояльный клиент ({orders_lifetime} заказов)")
    elif orders_lifetime >= 10:
        explanations.append(f"постоянный клиент ({orders_lifetime} заказов)")
    elif orders_lifetime >= 5:
        explanations.append(f"активный клиент ({orders_lifetime} заказов)")
    elif orders_lifetime >= 2:
        explanations.append(f"повторный клиент ({orders_lifetime} заказа)")
    else:
        explanations.append("новый клиент")
    
    # Анализ недавней активности (recency_days)
    recency_days = features.get('recency_days', 999) or 999
    if recency_days <= 7:
        explanations.append(f"покупал недавно ({int(recency_days)} дней назад)")
    elif recency_days <= 30:
        explanations.append(f"покупал в этом месяце ({int(recency_days)} дней назад)")
    elif recency_days <= 90:
        explanations.append(f"покупал в последние 3 месяца ({int(recency_days)} дней назад)")
    else:
        explanations.append(f"давно не покупал ({int(recency_days)} дней назад)")
    
    # Анализ разнообразия (categories_unique)
    categories_unique = features.get('categories_unique', 0) or 0
    if categories_unique >= 7:
        explanations.append(f"покупает в {categories_unique} категориях")
    elif categories_unique >= 4:
        explanations.append(f"разнообразные покупки ({categories_unique} категории)")
    elif categories_unique >= 2:
        explanations.append(f"покупает в {categories_unique} категориях")
    
    # Анализ суммы трат (monetary_180d)
    monetary_180d = features.get('monetary_180d', 0) or 0
    if monetary_180d >= 2000:
        explanations.append(f"крупные траты (${int(monetary_180d)} за полгода)")
    elif monetary_180d >= 1000:
        explanations.append(f"существенные траты (${int(monetary_180d)} за полгода)")
    elif monetary_180d >= 500:
        explanations.append(f"умеренные траты (${int(monetary_180d)} за полгода)")
    
    # Анализ частоты (frequency_90d)
    frequency_90d = features.get('frequency_90d', 0) or 0
    if frequency_90d >= 5:
        explanations.append(f"часто заказывает ({frequency_90d} раз за 90 дней)")
    elif frequency_90d >= 3:
        explanations.append(f"регулярно заказывает ({frequency_90d} раза за 90 дней)")
    
    # Возвращаем топ-2 самых важных факторов
    return ", ".join(explanations[:2])


@router.get(
    "/users-features-summary",
    summary="Get features summary statistics",
    description="Получение статистики по фичам пользователей"
)
async def get_features_summary(
    snapshot_date: Optional[str] = Query(None, description="Дата снапшота (YYYY-MM-DD)"),
    db = Depends(get_db)
) -> Dict[str, Any]:
    """Получение статистики по фичам"""
    try:
        if snapshot_date is None:
            snapshot_date = datetime.now().date().isoformat()
        
        query = """
        SELECT 
            COUNT(*) as total_users,
            COUNT(CASE WHEN orders_lifetime >= 10 THEN 1 END) as loyal_users,
            COUNT(CASE WHEN recency_days <= 30 THEN 1 END) as recent_buyers,
            COUNT(CASE WHEN categories_unique >= 5 THEN 1 END) as diverse_buyers,
            AVG(recency_days) as avg_recency,
            AVG(frequency_90d) as avg_frequency_90d,
            AVG(monetary_180d) as avg_monetary_180d,
            AVG(orders_lifetime) as avg_orders_lifetime,
            MIN(snapshot_date) as min_snapshot_date,
            MAX(snapshot_date) as max_snapshot_date
        FROM ml_user_features_daily_all 
        WHERE snapshot_date = %s
        """
        
        result = db.execute(text(query), (snapshot_date,))
        row = result.fetchone()
        
        if not row or row.total_users == 0:
            return {
                "snapshot_date": snapshot_date,
                "total_users": 0,
                "message": "Нет данных для указанной даты"
            }
        
        return {
            "snapshot_date": snapshot_date,
            "total_users": row.total_users,
            "user_segments": {
                "loyal_users": row.loyal_users,
                "recent_buyers": row.recent_buyers,
                "diverse_buyers": row.diverse_buyers
            },
            "average_features": {
                "recency_days": round(float(row.avg_recency), 1) if row.avg_recency else None,
                "frequency_90d": round(float(row.avg_frequency_90d), 1) if row.avg_frequency_90d else None,
                "monetary_180d": round(float(row.avg_monetary_180d), 1) if row.avg_monetary_180d else None,
                "orders_lifetime": round(float(row.avg_orders_lifetime), 1) if row.avg_orders_lifetime else None
            },
            "data_range": {
                "min_snapshot_date": row.min_snapshot_date.isoformat() if row.min_snapshot_date else None,
                "max_snapshot_date": row.max_snapshot_date.isoformat() if row.max_snapshot_date else None
            }
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения статистики: {str(e)}")
