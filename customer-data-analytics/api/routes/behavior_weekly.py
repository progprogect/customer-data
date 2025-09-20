"""
Behavior Weekly Routes
Маршруты для работы с недельной витриной поведения пользователей
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import logging
import sys
import os

# Добавляем путь к shared модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from database.connection import SessionLocal

from models.analytics import (
    BehaviorWeeklyRequest,
    BehaviorWeeklyResponse,
    UserBehaviorWeekly,
    AnomalyDetectionRequest,
    AnomalyDetectionResponse
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/behavior-weekly", tags=["behavior-weekly"])


def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/", response_model=BehaviorWeeklyResponse)
async def get_behavior_weekly(
    user_id: Optional[int] = Query(None, description="ID пользователя"),
    start_date: Optional[str] = Query(None, description="Начальная дата (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="Конечная дата (YYYY-MM-DD)"),
    min_orders: int = Query(0, description="Минимальное количество заказов"),
    min_monetary: float = Query(0, description="Минимальная сумма покупок"),
    limit: int = Query(1000, description="Максимальное количество записей"),
    offset: int = Query(0, description="Смещение для пагинации"),
    db = Depends(get_db)
):
    """Получение данных недельного поведения пользователей"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ml_user_behavior_weekly'
            );
        """).scalar()
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail="Таблица ml_user_behavior_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Строим запрос
        query = """
            SELECT 
                user_id,
                week_start_date,
                orders_count,
                monetary_sum,
                categories_count,
                aov_weekly
            FROM ml_user_behavior_weekly
            WHERE 1=1
        """
        params = {}
        
        if user_id is not None:
            query += " AND user_id = :user_id"
            params['user_id'] = user_id
            
        if start_date:
            query += " AND week_start_date >= :start_date"
            params['start_date'] = start_date
            
        if end_date:
            query += " AND week_start_date <= :end_date"
            params['end_date'] = end_date
            
        if min_orders > 0:
            query += " AND orders_count >= :min_orders"
            params['min_orders'] = min_orders
            
        if min_monetary > 0:
            query += " AND monetary_sum >= :min_monetary"
            params['min_monetary'] = min_monetary
        
        # Добавляем сортировку и пагинацию
        query += " ORDER BY user_id, week_start_date"
        query += " LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        # Выполняем запрос
        result = db.execute(query, params).fetchall()
        
        # Преобразуем в модели
        data = []
        for row in result:
            data.append(UserBehaviorWeekly(
                user_id=row.user_id,
                week_start_date=str(row.week_start_date),
                orders_count=row.orders_count,
                monetary_sum=float(row.monetary_sum),
                categories_count=row.categories_count,
                aov_weekly=float(row.aov_weekly) if row.aov_weekly else None
            ))
        
        # Получаем общее количество записей
        count_query = query.replace("SELECT user_id, week_start_date, orders_count, monetary_sum, categories_count, aov_weekly", "SELECT COUNT(*)")
        count_query = count_query.replace("ORDER BY user_id, week_start_date", "").replace("LIMIT :limit OFFSET :offset", "")
        total_count = db.execute(count_query, params).scalar()
        
        # Получаем диапазон дат
        date_range_query = """
            SELECT 
                MIN(week_start_date) as min_date,
                MAX(week_start_date) as max_date
            FROM ml_user_behavior_weekly
        """
        date_range_result = db.execute(date_range_query).fetchone()
        
        # Получаем сводную статистику
        stats_query = """
            SELECT 
                AVG(orders_count) as avg_orders,
                AVG(monetary_sum) as avg_monetary,
                AVG(categories_count) as avg_categories,
                AVG(aov_weekly) as avg_aov,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as total_weeks
            FROM ml_user_behavior_weekly
        """
        stats_result = db.execute(stats_query).fetchone()
        
        return BehaviorWeeklyResponse(
            data=data,
            total_count=total_count,
            date_range={
                "start": str(date_range_result.min_date) if date_range_result.min_date else None,
                "end": str(date_range_result.max_date) if date_range_result.max_date else None
            },
            summary_stats={
                "avg_orders": float(stats_result.avg_orders) if stats_result.avg_orders else 0,
                "avg_monetary": float(stats_result.avg_monetary) if stats_result.avg_monetary else 0,
                "avg_categories": float(stats_result.avg_categories) if stats_result.avg_categories else 0,
                "avg_aov": float(stats_result.avg_aov) if stats_result.avg_aov else 0,
                "unique_users": stats_result.unique_users,
                "total_weeks": stats_result.total_weeks
            }
        )
        
    except Exception as e:
        logger.error(f"Error in get_behavior_weekly: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/populate")
async def populate_behavior_weekly(db = Depends(get_db)):
    """Заполнение витрины недельного поведения"""
    try:
        # Проверяем существование функции
        function_exists = db.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.routines 
                WHERE routine_name = 'populate_behavior_weekly'
            );
        """).scalar()
        
        if not function_exists:
            raise HTTPException(
                status_code=404, 
                detail="Функция populate_behavior_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Выполняем функцию заполнения
        db.execute("SELECT populate_behavior_weekly();")
        db.commit()
        
        # Получаем статистику
        stats = db.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(week_start_date) as min_date,
                MAX(week_start_date) as max_date
            FROM ml_user_behavior_weekly
        """).fetchone()
        
        return {
            "success": True,
            "message": "Витрина успешно заполнена",
            "stats": {
                "total_records": stats.total_records,
                "unique_users": stats.unique_users,
                "date_range": {
                    "start": str(stats.min_date),
                    "end": str(stats.max_date)
                }
            }
        }
        
    except Exception as e:
        logger.error(f"Error in populate_behavior_weekly: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update")
async def update_behavior_weekly(db = Depends(get_db)):
    """Обновление витрины недельного поведения"""
    try:
        # Проверяем существование функции
        function_exists = db.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.routines 
                WHERE routine_name = 'update_behavior_weekly'
            );
        """).scalar()
        
        if not function_exists:
            raise HTTPException(
                status_code=404, 
                detail="Функция update_behavior_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Выполняем функцию обновления
        db.execute("SELECT update_behavior_weekly();")
        db.commit()
        
        return {
            "success": True,
            "message": "Витрина успешно обновлена"
        }
        
    except Exception as e:
        logger.error(f"Error in update_behavior_weekly: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/anomalies", response_model=AnomalyDetectionResponse)
async def detect_anomalies(
    request: AnomalyDetectionRequest,
    db = Depends(get_db)
):
    """Детекция аномалий в поведении пользователей"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ml_user_behavior_weekly'
            );
        """).scalar()
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail="Таблица ml_user_behavior_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Строим запрос для детекции аномалий
        query = """
            WITH user_stats AS (
                SELECT 
                    user_id,
                    week_start_date,
                    {metric},
                    AVG({metric}) OVER (
                        PARTITION BY user_id 
                        ORDER BY week_start_date 
                        ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW
                    ) as rolling_avg,
                    STDDEV({metric}) OVER (
                        PARTITION BY user_id 
                        ORDER BY week_start_date 
                        ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW
                    ) as rolling_std
                FROM ml_user_behavior_weekly
                WHERE user_id = :user_id OR :user_id IS NULL
            ),
            anomalies AS (
                SELECT 
                    user_id,
                    week_start_date,
                    {metric} as value,
                    rolling_avg,
                    rolling_std,
                    CASE 
                        WHEN rolling_std > 0 
                        THEN ABS({metric} - rolling_avg) / rolling_std
                        ELSE 0
                    END as z_score
                FROM user_stats
                WHERE rolling_std > 0
            )
            SELECT 
                user_id,
                week_start_date,
                value,
                rolling_avg,
                rolling_std,
                z_score
            FROM anomalies
            WHERE z_score >= :threshold
            ORDER BY z_score DESC
        """.format(
            metric=request.metric,
            window_size=request.window_size - 1
        )
        
        params = {
            'user_id': request.user_id,
            'threshold': request.threshold
        }
        
        result = db.execute(query, params).fetchall()
        
        # Преобразуем в список аномалий
        anomalies = []
        for row in result:
            anomalies.append({
                "user_id": row.user_id,
                "week_start_date": str(row.week_start_date),
                "value": float(row.value),
                "rolling_avg": float(row.rolling_avg),
                "rolling_std": float(row.rolling_std),
                "z_score": float(row.z_score),
                "anomaly_type": "high" if row.value > row.rolling_avg else "low"
            })
        
        # Получаем общую статистику
        total_records = db.execute("""
            SELECT COUNT(*) 
            FROM ml_user_behavior_weekly 
            WHERE user_id = :user_id OR :user_id IS NULL
        """, params).scalar()
        
        anomaly_rate = (len(anomalies) / total_records * 100) if total_records > 0 else 0
        
        return AnomalyDetectionResponse(
            anomalies=anomalies,
            total_anomalies=len(anomalies),
            anomaly_rate=anomaly_rate,
            threshold=request.threshold,
            metric=request.metric
        )
        
    except Exception as e:
        logger.error(f"Error in detect_anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_behavior_stats(db = Depends(get_db)):
    """Получение статистики по витрине поведения"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ml_user_behavior_weekly'
            );
        """).scalar()
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail="Таблица ml_user_behavior_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Получаем общую статистику
        stats = db.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(week_start_date) as min_date,
                MAX(week_start_date) as max_date,
                AVG(orders_count) as avg_orders,
                AVG(monetary_sum) as avg_monetary,
                AVG(categories_count) as avg_categories,
                AVG(aov_weekly) as avg_aov
            FROM ml_user_behavior_weekly
        """).fetchone()
        
        # Получаем топ пользователей по активности
        top_users = db.execute("""
            SELECT 
                user_id,
                COUNT(*) as weeks_active,
                SUM(orders_count) as total_orders,
                SUM(monetary_sum) as total_monetary
            FROM ml_user_behavior_weekly
            GROUP BY user_id
            ORDER BY total_orders DESC
            LIMIT 10
        """).fetchall()
        
        return {
            "table_stats": {
                "total_records": stats.total_records,
                "unique_users": stats.unique_users,
                "date_range": {
                    "start": str(stats.min_date),
                    "end": str(stats.max_date)
                },
                "averages": {
                    "orders_per_week": float(stats.avg_orders) if stats.avg_orders else 0,
                    "monetary_per_week": float(stats.avg_monetary) if stats.avg_monetary else 0,
                    "categories_per_week": float(stats.avg_categories) if stats.avg_categories else 0,
                    "aov_per_week": float(stats.avg_aov) if stats.avg_aov else 0
                }
            },
            "top_users": [
                {
                    "user_id": row.user_id,
                    "weeks_active": row.weeks_active,
                    "total_orders": row.total_orders,
                    "total_monetary": float(row.total_monetary)
                }
                for row in top_users
            ]
        }
        
    except Exception as e:
        logger.error(f"Error in get_behavior_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
