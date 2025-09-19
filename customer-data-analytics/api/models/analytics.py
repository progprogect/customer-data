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


class RecommendationItem(BaseModel):
    """Модель рекомендованного товара"""
    product_id: int = Field(..., description="ID товара")
    title: str = Field(..., description="Название товара")
    brand: Optional[str] = Field(None, description="Бренд")
    category: str = Field(..., description="Категория")
    price: float = Field(..., description="Цена")
    score: float = Field(..., description="Скор рекомендации [0..1]")
    popularity_score: Optional[float] = Field(0, description="Популярность за 30д")
    rating: Optional[float] = Field(0, description="Рейтинг товара")
    recommendation_reason: str = Field(..., description="Причина рекомендации")


class RecommendationResponse(BaseModel):
    """Ответ с рекомендациями"""
    user_id: Optional[int] = Field(None, description="ID пользователя")
    algorithm: str = Field(..., description="Алгоритм рекомендаций")
    recommendations: List[RecommendationItem] = Field(..., description="Список рекомендаций")
    total_count: int = Field(..., description="Количество рекомендаций")
    processing_time_ms: float = Field(..., description="Время обработки в мс")
    generated_at: datetime = Field(..., description="Время генерации")


class SimilarItemsRequest(BaseModel):
    """Запрос похожих товаров"""
    product_id: int = Field(..., description="ID товара")
    k: int = Field(20, ge=1, le=50, description="Количество рекомендаций")
    same_category_only: bool = Field(True, description="Только в той же категории")


class UserRecommendationsRequest(BaseModel):
    """Запрос персональных рекомендаций"""
    user_id: int = Field(..., description="ID пользователя")
    k: int = Field(20, ge=1, le=50, description="Количество рекомендаций")
    exclude_purchased: bool = Field(True, description="Исключить уже купленные")
    price_range: Optional[Dict[str, float]] = Field(None, description="Диапазон цен")


class UserPurchaseItem(BaseModel):
    """Модель покупки пользователя"""
    product_id: int = Field(..., description="ID товара")
    title: str = Field(..., description="Название товара")
    brand: Optional[str] = Field(None, description="Бренд")
    category: str = Field(..., description="Категория")
    price: float = Field(..., description="Цена за единицу")
    amount: float = Field(..., description="Общая сумма покупки")
    quantity: int = Field(..., description="Количество")
    days_ago: int = Field(..., description="Дней назад")
    purchase_date: str = Field(..., description="Дата покупки")
