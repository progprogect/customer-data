"""
Analytics Models
Pydantic модели для аналитики
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class UserSegmentationRequest(BaseModel):
    algorithm: str = Field("kmeans", description="Алгоритм кластеризации")
    n_clusters: int = Field(5, description="Количество кластеров")


class UserSegmentationResponse(BaseModel):
    segments: List[Dict[str, Any]]
    metrics: Dict[str, float]
    algorithm: str
    n_clusters: int


class ChurnPredictionRequest(BaseModel):
    user_ids: List[int] = Field(..., description="Список ID пользователей")
    features: Optional[Dict[str, Any]] = Field(None, description="Дополнительные признаки")


class ChurnPredictionResponse(BaseModel):
    predictions: List[Dict[str, Any]]
    model_metrics: Dict[str, float]
    feature_importance: List[Dict[str, Any]]


class PriceElasticityRequest(BaseModel):
    product_ids: List[int] = Field(..., description="Список ID товаров")
    price_changes: Optional[Dict[int, float]] = Field(None, description="Изменения цен")


class PriceElasticityResponse(BaseModel):
    elasticity_analysis: List[Dict[str, Any]]
    price_optimization: List[Dict[str, Any]]
    model_metrics: Dict[str, float]


class DashboardData(BaseModel):
    total_users: int
    total_orders: int
    total_revenue: float
    active_users: int
    conversion_rate: float
    avg_order_value: float
    top_products: List[Dict[str, Any]]
    user_growth: List[Dict[str, Any]]
    revenue_trend: List[Dict[str, Any]]


class AnomalyDetectionResponse(BaseModel):
    anomalies: List[Dict[str, Any]]
    threshold: float
    total_anomalies: int
    anomaly_rate: float
