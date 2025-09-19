"""
Users Routes
Маршруты для работы с пользователями
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime

from services.user_service import UserService

router = APIRouter()
user_service = UserService()


@router.get("/")
async def get_users(
    skip: int = Query(0, description="Количество пропускаемых записей"),
    limit: int = Query(100, description="Максимальное количество записей"),
    country: Optional[str] = Query(None, description="Фильтр по стране"),
    city: Optional[str] = Query(None, description="Фильтр по городу")
):
    """Получение списка пользователей"""
    try:
        users = await user_service.get_users(
            skip=skip,
            limit=limit,
            country=country,
            city=city
        )
        return users
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{user_id}")
async def get_user(user_id: int):
    """Получение информации о пользователе"""
    try:
        user = await user_service.get_user_by_id(user_id)
        if not user:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        return user
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{user_id}/profile")
async def get_user_profile(user_id: int):
    """Получение профиля пользователя с аналитикой"""
    try:
        profile = await user_service.get_user_profile(user_id)
        if not profile:
            raise HTTPException(status_code=404, detail="Пользователь не найден")
        return profile
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{user_id}/orders")
async def get_user_orders(
    user_id: int,
    skip: int = Query(0),
    limit: int = Query(50)
):
    """Получение заказов пользователя"""
    try:
        orders = await user_service.get_user_orders(
            user_id=user_id,
            skip=skip,
            limit=limit
        )
        return orders
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{user_id}/events")
async def get_user_events(
    user_id: int,
    event_type: Optional[str] = Query(None, description="Тип события"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    skip: int = Query(0),
    limit: int = Query(100)
):
    """Получение событий пользователя"""
    try:
        events = await user_service.get_user_events(
            user_id=user_id,
            event_type=event_type,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit
        )
        return events
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{user_id}/ltv")
async def calculate_user_ltv(user_id: int):
    """Расчет пожизненной ценности клиента"""
    try:
        ltv = await user_service.calculate_ltv(user_id)
        return {"user_id": user_id, "ltv": ltv}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/segments/")
async def get_user_segments():
    """Получение сегментов пользователей"""
    try:
        segments = await user_service.get_user_segments()
        return segments
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
