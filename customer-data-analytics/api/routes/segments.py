from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import logging
from services.segments_service import SegmentsService

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/segments", tags=["segments"])

# Инициализируем сервис
segments_service = SegmentsService()


@router.get("/meta")
async def get_segments_meta():
    """Получить метаданные кластеров"""
    try:
        return segments_service.get_meta()
    except Exception as e:
        logger.error(f"Error in get_segments_meta: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения метаданных кластеров")


@router.get("/distribution")
async def get_segments_distribution(
    date: Optional[str] = Query(None, description="Дата в формате YYYY-MM-DD")
):
    """Получить распределение сегментов"""
    try:
        return segments_service.get_distribution(date)
    except Exception as e:
        logger.error(f"Error in get_segments_distribution: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения распределения сегментов")


@router.get("/dynamics")
async def get_segments_dynamics(
    from_date: str = Query(..., description="Начальная дата в формате YYYY-MM-DD"),
    to_date: str = Query(..., description="Конечная дата в формате YYYY-MM-DD"),
    granularity: str = Query("day", description="Гранулярность: day, week, month")
):
    """Получить динамику сегментов"""
    try:
        # Валидация дат
        try:
            datetime.strptime(from_date, '%Y-%m-%d')
            datetime.strptime(to_date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYY-MM-DD")
        
        if from_date > to_date:
            raise HTTPException(status_code=400, detail="Начальная дата не может быть больше конечной")
        
        return segments_service.get_dynamics(from_date, to_date, granularity)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_segments_dynamics: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения динамики сегментов")


@router.get("/migration")
async def get_segments_migration(
    date: str = Query(..., description="Дата в формате YYYY-MM-DD")
):
    """Получить матрицу переходов между сегментами (вчера→сегодня)"""
    try:
        # Валидация даты
        try:
            datetime.strptime(date, '%Y-%m-%d')
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты. Используйте YYYY-MM-DD")
        
        return segments_service.get_migration(date)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_segments_migration: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения матрицы переходов между сегментами")