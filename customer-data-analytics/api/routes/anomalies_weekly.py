"""
Anomalies Weekly Routes
Маршруты для работы с недельными аномалиями поведения пользователей
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
from datetime import datetime, date
import logging
import sys
import os
from sqlalchemy import text

# Добавляем путь к shared модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from database.connection import SessionLocal

from models.analytics import (
    AnomaliesWeeklyRequest,
    AnomaliesWeeklyResponse,
    UserAnomalyWeekly,
    UserAnomaliesRequest,
    UserAnomaliesResponse,
    AnomalyStatsResponse
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/anomalies", tags=["anomalies-weekly"])


def get_db():
    """Получение сессии базы данных"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/weekly", response_model=AnomaliesWeeklyResponse)
async def get_anomalies_weekly(
    date: Optional[str] = Query(None, description="Дата недели (YYYY-MM-DD)"),
    min_score: float = Query(3.0, description="Минимальный anomaly_score"),
    limit: int = Query(100, description="Максимальное количество записей"),
    db = Depends(get_db)
):
    """Получение аномалий за конкретную неделю"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ml_user_anomalies_weekly'
            );
        """)).scalar()
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail="Таблица ml_user_anomalies_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Если дата не указана, берем последнюю неделю с аномалиями
        if not date:
            last_week = db.execute(text("""
                SELECT MAX(week_start) 
                FROM ml_user_anomalies_weekly 
                WHERE is_anomaly = true
            """)).scalar()
            if not last_week:
                raise HTTPException(
                    status_code=404, 
                    detail="Аномалии не найдены"
                )
            date = str(last_week)
        
        # Строим запрос
        query = text("""
            SELECT 
                user_id,
                week_start,
                anomaly_score,
                is_anomaly,
                triggers,
                insufficient_history
            FROM ml_user_anomalies_weekly
            WHERE week_start = :week_date
              AND is_anomaly = true
              AND anomaly_score >= :min_score
            ORDER BY anomaly_score DESC
            LIMIT :limit
        """)
        
        params = {
            'week_date': date,
            'min_score': min_score,
            'limit': limit
        }
        
        result = db.execute(query, params).fetchall()
        
        # Преобразуем в модели
        anomalies = []
        for row in result:
            anomalies.append(UserAnomalyWeekly(
                user_id=row.user_id,
                week_start=str(row.week_start),
                anomaly_score=float(row.anomaly_score),
                is_anomaly=row.is_anomaly,
                triggers=row.triggers,
                insufficient_history=row.insufficient_history
            ))
        
        # Получаем общее количество аномалий за неделю
        count_query = text("""
            SELECT COUNT(*) 
            FROM ml_user_anomalies_weekly 
            WHERE week_start = :week_date 
              AND is_anomaly = true 
              AND anomaly_score >= :min_score
        """)
        total_count = db.execute(count_query, params).scalar()
        
        # Получаем сводную статистику
        stats_query = text("""
            SELECT 
                COUNT(*) as total_anomalies,
                AVG(anomaly_score) as avg_score,
                MAX(anomaly_score) as max_score,
                COUNT(DISTINCT user_id) as unique_users
            FROM ml_user_anomalies_weekly
            WHERE week_start = :week_date 
              AND is_anomaly = true 
              AND anomaly_score >= :min_score
        """)
        stats_result = db.execute(stats_query, params).fetchone()
        
        return AnomaliesWeeklyResponse(
            anomalies=anomalies,
            total_count=total_count,
            week_date=date,
            summary_stats={
                "total_anomalies": stats_result.total_anomalies,
                "avg_score": float(stats_result.avg_score) if stats_result.avg_score else 0,
                "max_score": float(stats_result.max_score) if stats_result.max_score else 0,
                "unique_users": stats_result.unique_users
            }
        )
        
    except Exception as e:
        logger.error(f"Error in get_anomalies_weekly: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user/{user_id}", response_model=UserAnomaliesResponse)
async def get_user_anomalies(
    user_id: int,
    weeks: int = Query(12, description="Количество недель истории"),
    min_score: float = Query(0.0, description="Минимальный anomaly_score"),
    db = Depends(get_db)
):
    """Получение истории аномалий конкретного пользователя"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ml_user_anomalies_weekly'
            );
        """)).scalar()
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail="Таблица ml_user_anomalies_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Строим запрос
        query = text("""
            SELECT 
                user_id,
                week_start,
                anomaly_score,
                is_anomaly,
                triggers,
                insufficient_history
            FROM ml_user_anomalies_weekly
            WHERE user_id = :user_id
              AND anomaly_score >= :min_score
            ORDER BY week_start DESC
            LIMIT :weeks
        """)
        
        params = {
            'user_id': user_id,
            'min_score': min_score,
            'weeks': weeks
        }
        
        result = db.execute(query, params).fetchall()
        
        # Преобразуем в модели
        anomalies = []
        for row in result:
            anomalies.append(UserAnomalyWeekly(
                user_id=row.user_id,
                week_start=str(row.week_start),
                anomaly_score=float(row.anomaly_score),
                is_anomaly=row.is_anomaly,
                triggers=row.triggers,
                insufficient_history=row.insufficient_history
            ))
        
        # Получаем данные поведения пользователя
        behavior_query = text("""
            SELECT 
                week_start_date,
                orders_count,
                monetary_sum,
                categories_count,
                aov_weekly
            FROM ml_user_behavior_weekly
            WHERE user_id = :user_id
            ORDER BY week_start_date DESC
            LIMIT :weeks
        """)
        behavior_result = db.execute(behavior_query, {'user_id': user_id, 'weeks': weeks}).fetchall()
        
        # Преобразуем данные поведения
        behavior_data = []
        for row in behavior_result:
            behavior_data.append({
                "week_start": str(row.week_start_date),
                "orders_count": row.orders_count,
                "monetary_sum": float(row.monetary_sum),
                "categories_count": row.categories_count,
                "aov_weekly": float(row.aov_weekly) if row.aov_weekly else None
            })
        
        # Получаем статистику пользователя
        stats_query = text("""
            SELECT 
                COUNT(*) as total_weeks,
                COUNT(CASE WHEN is_anomaly = true THEN 1 END) as anomaly_weeks,
                AVG(anomaly_score) as avg_score,
                MAX(anomaly_score) as max_score
            FROM ml_user_anomalies_weekly
            WHERE user_id = :user_id
        """)
        stats_result = db.execute(stats_query, {'user_id': user_id}).fetchone()
        
        # Получаем топ триггеров
        triggers_query = text("""
            SELECT 
                trigger_name,
                COUNT(*) as count
            FROM (
                SELECT unnest(triggers) as trigger_name
                FROM ml_user_anomalies_weekly
                WHERE user_id = :user_id AND is_anomaly = true
            ) t
            GROUP BY trigger_name
            ORDER BY count DESC
            LIMIT 10
        """)
        triggers_result = db.execute(triggers_query, {'user_id': user_id}).fetchall()
        
        top_triggers = [
            {"trigger": row.trigger_name, "count": row.count}
            for row in triggers_result
        ]
        
        anomaly_rate = (stats_result.anomaly_weeks / stats_result.total_weeks * 100) if stats_result.total_weeks > 0 else 0
        
        return UserAnomaliesResponse(
            user_id=user_id,
            anomalies=anomalies,
            behavior_data=behavior_data,
            total_anomalies=stats_result.anomaly_weeks,
            anomaly_rate=anomaly_rate,
            top_triggers=top_triggers
        )
        
    except Exception as e:
        logger.error(f"Error in get_user_anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=AnomalyStatsResponse)
async def get_anomaly_stats(db = Depends(get_db)):
    """Получение общей статистики по аномалиям"""
    try:
        # Проверяем существование таблицы
        table_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ml_user_anomalies_weekly'
            );
        """)).scalar()
        
        if not table_exists:
            raise HTTPException(
                status_code=404, 
                detail="Таблица ml_user_anomalies_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Получаем общую статистику
        stats_query = text("""
            SELECT 
                COUNT(*) as total_weeks,
                COUNT(CASE WHEN is_anomaly = true THEN 1 END) as total_anomalies,
                AVG(CASE WHEN is_anomaly = true THEN anomaly_score END) as avg_anomaly_score,
                MAX(anomaly_score) as max_anomaly_score,
                COUNT(DISTINCT user_id) as unique_users
            FROM ml_user_anomalies_weekly
        """)
        stats_result = db.execute(stats_query).fetchone()
        
        # Получаем топ пользователей по аномалиям
        top_users_query = text("""
            SELECT 
                user_id,
                COUNT(*) as anomaly_count,
                AVG(anomaly_score) as avg_score,
                MAX(anomaly_score) as max_score
            FROM ml_user_anomalies_weekly
            WHERE is_anomaly = true
            GROUP BY user_id
            ORDER BY anomaly_count DESC
            LIMIT 10
        """)
        top_users_result = db.execute(top_users_query).fetchall()
        
        top_users = [
            {
                "user_id": row.user_id,
                "anomaly_count": row.anomaly_count,
                "avg_score": float(row.avg_score),
                "max_score": float(row.max_score)
            }
            for row in top_users_result
        ]
        
        # Получаем топ триггеров
        top_triggers_query = text("""
            SELECT 
                trigger_name,
                COUNT(*) as count
            FROM (
                SELECT unnest(triggers) as trigger_name
                FROM ml_user_anomalies_weekly
                WHERE is_anomaly = true
            ) t
            GROUP BY trigger_name
            ORDER BY count DESC
            LIMIT 10
        """)
        top_triggers_result = db.execute(top_triggers_query).fetchall()
        
        top_triggers = [
            {"trigger": row.trigger_name, "count": row.count}
            for row in top_triggers_result
        ]
        
        # Получаем распределение по неделям
        weekly_dist_query = text("""
            SELECT 
                week_start,
                COUNT(*) as anomaly_count
            FROM ml_user_anomalies_weekly
            WHERE is_anomaly = true
            GROUP BY week_start
            ORDER BY week_start DESC
            LIMIT 20
        """)
        weekly_dist_result = db.execute(weekly_dist_query).fetchall()
        
        weekly_distribution = [
            {
                "week_start": str(row.week_start),
                "anomaly_count": row.anomaly_count
            }
            for row in weekly_dist_result
        ]
        
        anomaly_rate = (stats_result.total_anomalies / stats_result.total_weeks * 100) if stats_result.total_weeks > 0 else 0
        
        return AnomalyStatsResponse(
            total_weeks=stats_result.total_weeks,
            total_anomalies=stats_result.total_anomalies,
            anomaly_rate=anomaly_rate,
            top_users=top_users,
            top_triggers=top_triggers,
            weekly_distribution=weekly_distribution
        )
        
    except Exception as e:
        logger.error(f"Error in get_anomaly_stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/detect")
async def detect_anomalies(db = Depends(get_db)):
    """Запуск детекции аномалий"""
    try:
        # Проверяем существование функции
        function_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.routines 
                WHERE routine_name = 'detect_anomalies_weekly'
            );
        """)).scalar()
        
        if not function_exists:
            raise HTTPException(
                status_code=404, 
                detail="Функция detect_anomalies_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Выполняем функцию детекции
        db.execute(text("SELECT detect_anomalies_weekly();"))
        db.commit()
        
        # Получаем статистику
        stats = db.execute(text("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN is_anomaly = true THEN 1 END) as anomaly_count,
                COUNT(CASE WHEN insufficient_history = true THEN 1 END) as insufficient_history_count
            FROM ml_user_anomalies_weekly
        """)).fetchone()
        
        return {
            "success": True,
            "message": "Детекция аномалий завершена",
            "stats": {
                "total_records": stats.total_records,
                "anomaly_count": stats.anomaly_count,
                "insufficient_history_count": stats.insufficient_history_count,
                "anomaly_rate": (stats.anomaly_count / stats.total_records * 100) if stats.total_records > 0 else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Error in detect_anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update")
async def update_anomalies(db = Depends(get_db)):
    """Обновление аномалий"""
    try:
        # Проверяем существование функции
        function_exists = db.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.routines 
                WHERE routine_name = 'update_anomalies_weekly'
            );
        """)).scalar()
        
        if not function_exists:
            raise HTTPException(
                status_code=404, 
                detail="Функция update_anomalies_weekly не найдена. Запустите скрипт создания таблицы."
            )
        
        # Выполняем функцию обновления
        db.execute(text("SELECT update_anomalies_weekly();"))
        db.commit()
        
        return {
            "success": True,
            "message": "Аномалии успешно обновлены"
        }
        
    except Exception as e:
        logger.error(f"Error in update_anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))
