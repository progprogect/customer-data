"""
Recommendations Routes
Маршруты для рекомендательной системы
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel

from services.recommendation_service import RecommendationService

router = APIRouter()
recommendation_service = RecommendationService()


class RecommendationRequest(BaseModel):
    user_id: int
    n_recommendations: int = 5
    method: str = "hybrid"  # collaborative, content, hybrid


class RecommendationResponse(BaseModel):
    user_id: int
    recommendations: List[dict]
    method: str
    confidence: float


@router.post("/", response_model=RecommendationResponse)
async def get_recommendations(request: RecommendationRequest):
    """Получение рекомендаций для пользователя"""
    try:
        result = await recommendation_service.get_recommendations(
            user_id=request.user_id,
            n_recommendations=request.n_recommendations,
            method=request.method
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user/{user_id}")
async def get_user_recommendations(
    user_id: int,
    n_recommendations: int = Query(5, description="Количество рекомендаций"),
    method: str = Query("hybrid", description="Метод рекомендаций")
):
    """Получение рекомендаций для конкретного пользователя"""
    try:
        result = await recommendation_service.get_recommendations(
            user_id=user_id,
            n_recommendations=n_recommendations,
            method=method
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/similar-products/{product_id}")
async def get_similar_products(
    product_id: int,
    n_products: int = Query(5, description="Количество похожих товаров")
):
    """Получение похожих товаров"""
    try:
        similar_products = await recommendation_service.get_similar_products(
            product_id=product_id,
            n_products=n_products
        )
        return similar_products
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/train")
async def train_recommendation_model(
    method: str = "hybrid",
    force_retrain: bool = False
):
    """Обучение модели рекомендаций"""
    try:
        result = await recommendation_service.train_model(
            method=method,
            force_retrain=force_retrain
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/model-status")
async def get_model_status():
    """Получение статуса модели рекомендаций"""
    try:
        status = await recommendation_service.get_model_status()
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
