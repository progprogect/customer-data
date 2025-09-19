#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Segments API endpoints for user clustering data
Author: Customer Data Analytics Team
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
import pytz
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from functools import lru_cache
import hashlib

from services.database import get_db_connection_dependency
from models.segments import (
    SegmentsDistributionResponse,
    SegmentsDynamicsResponse,
    SegmentsMigrationResponse,
    SegmentsMetaResponse,
    SegmentPoint,
    MigrationMatrix
)

# Настройка логирования
logger = logging.getLogger(__name__)

# Роутер для эндпоинтов сегментов
router = APIRouter(prefix="/segments", tags=["segments"])

# Часовой пояс
WARSAW_TZ = pytz.timezone('Europe/Warsaw')

def normalize_date(date_input: str) -> date:
    """
    Нормализация даты к локальной дате Варшавы
    
    Args:
        date_input: Дата в формате YYYY-MM-DD или ISO 8601
        
    Returns:
        date: Нормализованная дата
    """
    try:
        # Парсим дату
        if 'T' in date_input:
            # ISO 8601 формат
            dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            # Конвертируем в часовой пояс Варшавы
            if dt.tzinfo is None:
                dt = pytz.utc.localize(dt)
            dt_warsaw = dt.astimezone(WARSAW_TZ)
            return dt_warsaw.date()
        else:
            # Простой формат YYYY-MM-DD
            return datetime.strptime(date_input, '%Y-%m-%d').date()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {date_input}")

def validate_date_range(date_from: date, date_to: date) -> None:
    """
    Валидация диапазона дат
    
    Args:
        date_from: Начальная дата
        date_to: Конечная дата
    """
    if date_from > date_to:
        raise HTTPException(status_code=400, detail="from date must be <= to date")
    
    # Максимальный диапазон - 366 дней
    if (date_to - date_from).days > 366:
        raise HTTPException(status_code=400, detail="Date range cannot exceed 366 days")
    
    # Проверка на будущие даты
    today = datetime.now(WARSAW_TZ).date()
    if date_from > today or date_to > today:
        raise HTTPException(status_code=400, detail="Dates cannot be in the future")

@lru_cache(maxsize=128)
def get_cache_key(endpoint: str, **kwargs) -> str:
    """
    Генерация ключа кэша
    
    Args:
        endpoint: Название эндпоинта
        **kwargs: Параметры запроса
        
    Returns:
        str: Ключ кэша
    """
    key_data = f"{endpoint}:{sorted(kwargs.items())}"
    return hashlib.md5(key_data.encode()).hexdigest()

def get_cache_headers(is_today: bool = False) -> Dict[str, str]:
    """
    Получение заголовков кэширования
    
    Args:
        is_today: Является ли запрос за сегодняшний день
        
    Returns:
        Dict[str, str]: Заголовки кэширования
    """
    if is_today:
        return {
            "Cache-Control": "public, max-age=300, stale-while-revalidate=600",
            "ETag": f'"{datetime.now().strftime("%Y%m%d%H%M")}"'
        }
    else:
        return {
            "Cache-Control": "public, max-age=3600",
            "ETag": f'"{datetime.now().strftime("%Y%m%d")}"'
        }

@router.get("/distribution", response_model=SegmentsDistributionResponse)
async def get_segments_distribution(
    date: Optional[str] = Query(None, description="Date in YYYY-MM-DD format"),
    db_conn = Depends(get_db_connection_dependency)
):
    """
    Получение распределения сегментов на дату
    
    Args:
        date: Дата для получения данных (по умолчанию последняя доступная)
        db_conn: Подключение к БД
        
    Returns:
        SegmentsDistributionResponse: Распределение сегментов
    """
    try:
        # Нормализация даты
        target_date = None
        if date:
            target_date = normalize_date(date)
        
        # Проверка доступности данных
        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            if target_date:
                cur.execute("SELECT is_data_available(%s)", (target_date,))
                available = cur.fetchone()['is_data_available']
                if not available:
                    return JSONResponse(
                        status_code=200,
                        content={
                            "date": target_date.isoformat(),
                            "timezone": "Europe/Warsaw",
                            "available": False,
                            "total_users": 0,
                            "segments": []
                        },
                        headers=get_cache_headers()
                    )
            else:
                # Получаем последнюю доступную дату
                cur.execute("SELECT get_last_available_date()")
                result = cur.fetchone()
                if not result or not result['get_last_available_date']:
                    raise HTTPException(status_code=404, detail="No data available")
                target_date = result['get_last_available_date']
            
            # Получаем распределение сегментов
            cur.execute("SELECT * FROM get_segments_distribution(%s)", (target_date,))
            results = cur.fetchall()
            
            if not results:
                return JSONResponse(
                    status_code=200,
                    content={
                        "date": target_date.isoformat(),
                        "timezone": "Europe/Warsaw",
                        "available": False,
                        "total_users": 0,
                        "segments": []
                    },
                    headers=get_cache_headers()
                )
            
            # Формируем ответ
            total_users = results[0]['total_users'] if results else 0
            segments = []
            
            for row in results:
                segments.append({
                    "cluster_id": row['cluster_id'],
                    "users_count": row['users_count'],
                    "share": float(row['share'])
                })
            
            # Определяем, является ли запрос за сегодня
            today = datetime.now(WARSAW_TZ).date()
            is_today = target_date == today
            
            response_data = {
                "date": target_date.isoformat(),
                "timezone": "Europe/Warsaw",
                "available": True,
                "total_users": total_users,
                "segments": segments
            }
            
            return JSONResponse(
                content=response_data,
                headers=get_cache_headers(is_today)
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_segments_distribution: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/dynamics", response_model=SegmentsDynamicsResponse)
async def get_segments_dynamics(
    from_date: str = Query(..., alias="from", description="Start date in YYYY-MM-DD format"),
    to_date: str = Query(..., alias="to", description="End date in YYYY-MM-DD format"),
    granularity: str = Query("day", description="Granularity: day, week, or month"),
    db_conn = Depends(get_db_connection_dependency)
):
    """
    Получение динамики сегментов за период
    
    Args:
        from_date: Начальная дата
        to_date: Конечная дата
        granularity: Гранулярность (day, week, month)
        db_conn: Подключение к БД
        
    Returns:
        SegmentsDynamicsResponse: Динамика сегментов
    """
    try:
        # Нормализация дат
        date_from = normalize_date(from_date)
        date_to = normalize_date(to_date)
        
        # Валидация
        validate_date_range(date_from, date_to)
        
        if granularity not in ['day', 'week', 'month']:
            raise HTTPException(status_code=400, detail="Invalid granularity. Must be day, week, or month")
        
        # Получение данных
        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM get_segments_dynamics(%s, %s, %s)",
                (date_from, date_to, granularity)
            )
            results = cur.fetchall()
            
            # Группировка по кластерам
            cluster_data = {}
            for row in results:
                cluster_id = row['cluster_id']
                if cluster_id not in cluster_data:
                    cluster_data[cluster_id] = []
                
                cluster_data[cluster_id].append({
                    "date": row['date_point'].isoformat(),
                    "count": row['users_count']
                })
            
            # Формирование серий
            series = []
            for cluster_id in sorted(cluster_data.keys()):
                series.append({
                    "cluster_id": cluster_id,
                    "points": cluster_data[cluster_id]
                })
            
            response_data = {
                "from": date_from.isoformat(),
                "to": date_to.isoformat(),
                "timezone": "Europe/Warsaw",
                "series": series
            }
            
            return JSONResponse(
                content=response_data,
                headers=get_cache_headers()
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_segments_dynamics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/migration", response_model=SegmentsMigrationResponse)
async def get_segments_migration(
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    db_conn = Depends(get_db_connection_dependency)
):
    """
    Получение матрицы переходов между сегментами
    
    Args:
        date: Дата для анализа переходов
        db_conn: Подключение к БД
        
    Returns:
        SegmentsMigrationResponse: Матрица переходов
    """
    try:
        # Нормализация даты
        target_date = normalize_date(date)
        prev_date = target_date - timedelta(days=1)
        
        # Проверка доступности данных
        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Проверяем наличие данных за целевую дату
            cur.execute("SELECT is_data_available(%s)", (target_date,))
            today_available = cur.fetchone()['is_data_available']
            
            if not today_available:
                return JSONResponse(
                    status_code=200,
                    content={
                        "date": target_date.isoformat(),
                        "prev_date": prev_date.isoformat(),
                        "timezone": "Europe/Warsaw",
                        "available": False,
                        "matrix": [],
                        "note": "Target date data not available"
                    },
                    headers=get_cache_headers()
                )
            
            # Проверяем наличие данных за предыдущую дату
            cur.execute("SELECT is_data_available(%s)", (prev_date,))
            prev_available = cur.fetchone()['is_data_available']
            
            if not prev_available:
                return JSONResponse(
                    status_code=200,
                    content={
                        "date": target_date.isoformat(),
                        "prev_date": prev_date.isoformat(),
                        "timezone": "Europe/Warsaw",
                        "available": False,
                        "matrix": [],
                        "note": "Previous date data not available"
                    },
                    headers=get_cache_headers()
                )
            
            # Получаем матрицу переходов
            cur.execute("SELECT * FROM get_segments_migration(%s)", (target_date,))
            results = cur.fetchall()
            
            # Формируем матрицу
            matrix = []
            for row in results:
                matrix.append({
                    "from": row['from_cluster'],
                    "to": row['to_cluster'],
                    "count": row['users_moved']
                })
            
            response_data = {
                "date": target_date.isoformat(),
                "prev_date": prev_date.isoformat(),
                "timezone": "Europe/Warsaw",
                "available": True,
                "matrix": matrix
            }
            
            return JSONResponse(
                content=response_data,
                headers=get_cache_headers()
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_segments_migration: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/meta", response_model=SegmentsMetaResponse)
async def get_segments_meta(db_conn = Depends(get_db_connection_dependency)):
    """
    Получение метаданных о кластерах
    
    Args:
        db_conn: Подключение к БД
        
    Returns:
        SegmentsMetaResponse: Метаданные о кластерах
    """
    try:
        with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM get_segments_meta()")
            results = cur.fetchall()
            
            clusters = []
            for row in results:
                clusters.append({
                    "id": row['cluster_id'],
                    "name": row['cluster_name'],
                    "description": row['description']
                })
            
            response_data = {
                "clusters": clusters
            }
            
            return JSONResponse(
                content=response_data,
                headers=get_cache_headers()
            )
            
    except Exception as e:
        logger.error(f"Error in get_segments_meta: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
