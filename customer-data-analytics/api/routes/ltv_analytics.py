"""
LTV Analytics API Routes
FastAPI routes для LTV (Lifetime Value) аналитики
"""

from fastapi import APIRouter, HTTPException, Depends, Query, status
from typing import List, Optional
import logging
import time
from datetime import datetime, date

from models.ltv_models import (
    UserLTV, LTVSummaryResponse, UserLTVResponse, 
    LTVCalculationRequest, LTVCalculationResponse,
    LTVFilters, LTVDistributionResponse
)
from services.database import get_db_connection
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/summary",
    response_model=LTVSummaryResponse,
    summary="Get LTV summary statistics",
    description="Получение агрегированной статистики LTV по всем пользователям"
)
async def get_ltv_summary():
    """Получение агрегированной статистики LTV"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Получаем агрегированную статистику
                cur.execute("SELECT * FROM get_ltv_summary()")
                summary_data = cur.fetchall()
                
                # Получаем общее количество пользователей
                cur.execute("SELECT COUNT(*) as total_users FROM ml_user_ltv")
                total_users = cur.fetchone()['total_users']
                
                # Преобразуем данные в модели
                summary = []
                for row in summary_data:
                    summary.append({
                        "metric_name": row['metric_name'],
                        "value_3m": float(row['value_3m'] or 0),
                        "value_6m": float(row['value_6m'] or 0),
                        "value_12m": float(row['value_12m'] or 0),
                        "value_lifetime": float(row['value_lifetime'] or 0)
                    })
                
                return LTVSummaryResponse(
                    summary=summary,
                    total_users=total_users,
                    calculated_at=datetime.now()
                )
                
    except Exception as e:
        logger.error(f"Ошибка получения LTV статистики: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка получения LTV статистики: {str(e)}"
        )


@router.get(
    "/users",
    response_model=UserLTVResponse,
    summary="Get users LTV data",
    description="Получение LTV данных пользователей с пагинацией и фильтрацией"
)
async def get_users_ltv(
    page: int = Query(1, ge=1, description="Номер страницы"),
    page_size: int = Query(50, ge=1, le=100, description="Размер страницы"),
    min_revenue_6m: Optional[float] = Query(None, description="Минимальная выручка за 6 месяцев"),
    max_revenue_6m: Optional[float] = Query(None, description="Максимальная выручка за 6 месяцев"),
    min_revenue_12m: Optional[float] = Query(None, description="Минимальная выручка за 12 месяцев"),
    max_revenue_12m: Optional[float] = Query(None, description="Максимальная выручка за 12 месяцев"),
    min_orders_lifetime: Optional[int] = Query(None, description="Минимальное количество заказов"),
    max_orders_lifetime: Optional[int] = Query(None, description="Максимальное количество заказов"),
    signup_date_from: Optional[date] = Query(None, description="Дата регистрации с"),
    signup_date_to: Optional[date] = Query(None, description="Дата регистрации по"),
    sort_by: str = Query("lifetime_revenue", description="Поле для сортировки"),
    sort_order: str = Query("desc", description="Порядок сортировки (asc/desc)")
):
    """Получение LTV данных пользователей с фильтрацией и пагинацией"""
    try:
        # Валидация параметров сортировки
        valid_sort_fields = [
            "lifetime_revenue", "revenue_6m", "revenue_12m", 
            "orders_lifetime", "signup_date", "last_order_date"
        ]
        if sort_by not in valid_sort_fields:
            sort_by = "lifetime_revenue"
        
        if sort_order not in ["asc", "desc"]:
            sort_order = "desc"
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Строим WHERE условия
                where_conditions = []
                params = []
                
                if min_revenue_6m is not None:
                    where_conditions.append("revenue_6m >= %s")
                    params.append(min_revenue_6m)
                
                if max_revenue_6m is not None:
                    where_conditions.append("revenue_6m <= %s")
                    params.append(max_revenue_6m)
                
                if min_revenue_12m is not None:
                    where_conditions.append("revenue_12m >= %s")
                    params.append(min_revenue_12m)
                
                if max_revenue_12m is not None:
                    where_conditions.append("revenue_12m <= %s")
                    params.append(max_revenue_12m)
                
                if min_orders_lifetime is not None:
                    where_conditions.append("orders_lifetime >= %s")
                    params.append(min_orders_lifetime)
                
                if max_orders_lifetime is not None:
                    where_conditions.append("orders_lifetime <= %s")
                    params.append(max_orders_lifetime)
                
                if signup_date_from is not None:
                    where_conditions.append("signup_date >= %s")
                    params.append(signup_date_from)
                
                if signup_date_to is not None:
                    where_conditions.append("signup_date <= %s")
                    params.append(signup_date_to)
                
                # Строим запрос
                where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
                
                # Получаем общее количество записей
                count_query = f"SELECT COUNT(*) as total FROM ml_user_ltv {where_clause}"
                cur.execute(count_query, params)
                total_count = cur.fetchone()['total']
                
                # Получаем данные с пагинацией
                offset = (page - 1) * page_size
                data_query = f"""
                    SELECT * FROM ml_user_ltv 
                    {where_clause}
                    ORDER BY {sort_by} {sort_order.upper()}
                    LIMIT %s OFFSET %s
                """
                cur.execute(data_query, params + [page_size, offset])
                users_data = cur.fetchall()
                
                # Преобразуем данные в модели
                users = []
                for row in users_data:
                    users.append(UserLTV(
                        user_id=row['user_id'],
                        signup_date=row['signup_date'],
                        revenue_3m=float(row['revenue_3m']),
                        revenue_6m=float(row['revenue_6m']),
                        revenue_12m=float(row['revenue_12m']),
                        lifetime_revenue=float(row['lifetime_revenue']),
                        orders_3m=row['orders_3m'],
                        orders_6m=row['orders_6m'],
                        orders_12m=row['orders_12m'],
                        orders_lifetime=row['orders_lifetime'],
                        avg_order_value_3m=float(row['avg_order_value_3m']) if row['avg_order_value_3m'] else None,
                        avg_order_value_6m=float(row['avg_order_value_6m']) if row['avg_order_value_6m'] else None,
                        avg_order_value_12m=float(row['avg_order_value_12m']) if row['avg_order_value_12m'] else None,
                        avg_order_value_lifetime=float(row['avg_order_value_lifetime']) if row['avg_order_value_lifetime'] else None,
                        last_order_date=row['last_order_date'],
                        first_order_date=row['first_order_date'],
                        days_since_last_order=row['days_since_last_order'],
                        created_at=row['created_at'],
                        updated_at=row['updated_at']
                    ))
                
                return UserLTVResponse(
                    users=users,
                    total_count=total_count,
                    page=page,
                    page_size=page_size
                )
                
    except Exception as e:
        logger.error(f"Ошибка получения LTV данных пользователей: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка получения LTV данных: {str(e)}"
        )


@router.get(
    "/distribution",
    response_model=LTVDistributionResponse,
    summary="Get LTV distribution",
    description="Получение распределения LTV по диапазонам"
)
async def get_ltv_distribution(
    metric_type: str = Query("revenue_6m", description="Тип метрики (revenue_6m, revenue_12m, lifetime_revenue)"),
    bins: int = Query(10, ge=5, le=20, description="Количество диапазонов")
):
    """Получение распределения LTV по диапазонам"""
    try:
        # Валидация типа метрики
        valid_metrics = ["revenue_3m", "revenue_6m", "revenue_12m", "lifetime_revenue"]
        if metric_type not in valid_metrics:
            metric_type = "revenue_6m"
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Получаем min, max значения
                cur.execute(f"""
                    SELECT 
                        MIN({metric_type}) as min_value,
                        MAX({metric_type}) as max_value,
                        COUNT(*) as total_users
                    FROM ml_user_ltv 
                    WHERE {metric_type} > 0
                """)
                stats = cur.fetchone()
                
                if not stats or stats['total_users'] == 0:
                    return LTVDistributionResponse(
                        distribution=[],
                        total_users=0,
                        metric_type=metric_type
                    )
                
                min_value = float(stats['min_value'])
                max_value = float(stats['max_value'])
                total_users = stats['total_users']
                
                # Создаем диапазоны
                bin_size = (max_value - min_value) / bins
                distribution = []
                
                for i in range(bins):
                    range_min = min_value + (i * bin_size)
                    range_max = min_value + ((i + 1) * bin_size)
                    
                    # Последний диапазон включает максимальное значение
                    if i == bins - 1:
                        range_max = max_value
                    
                    # Подсчитываем количество пользователей в диапазоне
                    cur.execute(f"""
                        SELECT COUNT(*) as count
                        FROM ml_user_ltv 
                        WHERE {metric_type} >= %s AND {metric_type} < %s
                    """, [range_min, range_max])
                    
                    count = cur.fetchone()['count']
                    percentage = (count / total_users) * 100 if total_users > 0 else 0
                    
                    distribution.append({
                        "range_min": round(range_min, 2),
                        "range_max": round(range_max, 2),
                        "count": count,
                        "percentage": round(percentage, 2)
                    })
                
                return LTVDistributionResponse(
                    distribution=distribution,
                    total_users=total_users,
                    metric_type=metric_type
                )
                
    except Exception as e:
        logger.error(f"Ошибка получения распределения LTV: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка получения распределения LTV: {str(e)}"
        )


@router.post(
    "/calculate",
    response_model=LTVCalculationResponse,
    summary="Calculate LTV",
    description="Пересчет LTV метрик для пользователей"
)
async def calculate_ltv(request: LTVCalculationRequest):
    """Пересчет LTV метрик"""
    try:
        start_time = time.time()
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                if request.user_ids:
                    # Пересчет для конкретных пользователей
                    users_processed = 0
                    for user_id in request.user_ids:
                        # Удаляем существующую запись
                        cur.execute("DELETE FROM ml_user_ltv WHERE user_id = %s", [user_id])
                        
                        # Рассчитываем LTV для пользователя
                        cur.execute("SELECT * FROM calculate_user_ltv(%s)", [user_id])
                        ltv_data = cur.fetchone()
                        
                        if ltv_data:
                            # Вставляем новые данные
                            cur.execute("""
                                INSERT INTO ml_user_ltv (
                                    user_id, signup_date, revenue_3m, revenue_6m, revenue_12m, lifetime_revenue,
                                    orders_3m, orders_6m, orders_12m, orders_lifetime,
                                    avg_order_value_3m, avg_order_value_6m, avg_order_value_12m, avg_order_value_lifetime,
                                    last_order_date, first_order_date, days_since_last_order
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
                            """, ltv_data)
                            users_processed += 1
                else:
                    # Пересчет для всех пользователей
                    cur.execute("SELECT calculate_all_ltv()")
                    cur.execute("SELECT COUNT(*) FROM ml_user_ltv")
                    users_processed = cur.fetchone()[0]
                
                conn.commit()
        
        calculation_time = time.time() - start_time
        
        return LTVCalculationResponse(
            success=True,
            message=f"LTV успешно рассчитан для {users_processed} пользователей",
            users_processed=users_processed,
            calculation_time=round(calculation_time, 2)
        )
        
    except Exception as e:
        logger.error(f"Ошибка расчета LTV: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка расчета LTV: {str(e)}"
        )


@router.get(
    "/health",
    summary="LTV service health check",
    description="Проверка здоровья LTV сервиса"
)
async def ltv_health_check():
    """Проверка здоровья LTV сервиса"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Проверяем существование таблицы
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'ml_user_ltv'
                    )
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    return {
                        "status": "unhealthy",
                        "message": "LTV table does not exist",
                        "timestamp": datetime.now().isoformat()
                    }
                
                # Проверяем количество записей
                cur.execute("SELECT COUNT(*) FROM ml_user_ltv")
                record_count = cur.fetchone()[0]
                
                return {
                    "status": "healthy",
                    "message": "LTV service is operational",
                    "table_exists": table_exists,
                    "record_count": record_count,
                    "timestamp": datetime.now().isoformat()
                }
                
    except Exception as e:
        logger.error(f"Ошибка проверки здоровья LTV сервиса: {e}")
        return {
            "status": "unhealthy",
            "message": f"LTV service error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }
