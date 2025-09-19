"""
Analytics Routes
Маршруты для аналитики и отчетов
"""

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime, timedelta
import pandas as pd

from services.analytics_service import AnalyticsService
from models.analytics import (
    UserSegmentationRequest,
    UserSegmentationResponse,
    ChurnPredictionRequest,
    ChurnPredictionResponse,
    PriceElasticityRequest,
    PriceElasticityResponse
)

router = APIRouter()
analytics_service = AnalyticsService()


@router.get("/dashboard")
async def get_dashboard_data(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None)
):
    """Получение данных для дашборда"""
    try:
        if not start_date:
            start_date = datetime.now() - timedelta(days=30)
        if not end_date:
            end_date = datetime.now()
        
        data = await analytics_service.get_dashboard_data(start_date, end_date)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/segmentation", response_model=UserSegmentationResponse)
async def segment_users(request: UserSegmentationRequest):
    """Сегментация пользователей"""
    try:
        result = await analytics_service.segment_users(
            algorithm=request.algorithm,
            n_clusters=request.n_clusters
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/churn-prediction", response_model=ChurnPredictionResponse)
async def predict_churn(request: ChurnPredictionRequest):
    """Предсказание оттока клиентов"""
    try:
        result = await analytics_service.predict_churn(
            user_ids=request.user_ids,
            features=request.features
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/price-elasticity", response_model=PriceElasticityResponse)
async def analyze_price_elasticity(request: PriceElasticityRequest):
    """Анализ ценовой эластичности"""
    try:
        result = await analytics_service.analyze_price_elasticity(
            product_ids=request.product_ids,
            price_changes=request.price_changes
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_metrics(
    metric_type: str = Query(..., description="Тип метрики: revenue, users, orders"),
    period: str = Query("30d", description="Период: 7d, 30d, 90d, 1y")
):
    """Получение различных метрик"""
    try:
        metrics = await analytics_service.get_metrics(metric_type, period)
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomaly-detection")
async def detect_anomalies(
    threshold: float = Query(0.1, description="Порог для детекции аномалий")
):
    """Детекция аномалий в поведении пользователей"""
    try:
        anomalies = await analytics_service.detect_anomalies(threshold)
        return anomalies
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
