"""
Common Types
Общие типы данных для проекта
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field


class BaseResponse(BaseModel):
    """Базовый класс для ответов API"""
    success: bool = True
    message: str = "Success"
    data: Optional[Any] = None
    error: Optional[str] = None


class PaginationParams(BaseModel):
    """Параметры пагинации"""
    skip: int = Field(0, ge=0, description="Количество пропускаемых записей")
    limit: int = Field(100, ge=1, le=1000, description="Максимальное количество записей")


class DateRange(BaseModel):
    """Диапазон дат"""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class UserInfo(BaseModel):
    """Информация о пользователе"""
    user_id: int
    email: Optional[str] = None
    phone: Optional[str] = None
    country: Optional[str] = None
    city: Optional[str] = None
    registered_at: datetime


class ProductInfo(BaseModel):
    """Информация о товаре"""
    product_id: int
    title: str
    category: Optional[str] = None
    brand: Optional[str] = None
    price: float
    currency: str = "USD"


class OrderInfo(BaseModel):
    """Информация о заказе"""
    order_id: int
    user_id: int
    total_amount: float
    currency: str = "USD"
    status: str
    created_at: datetime


class EventInfo(BaseModel):
    """Информация о событии"""
    event_id: int
    user_id: int
    event_type: str
    event_time: datetime
    product_id: Optional[int] = None
    value: Optional[float] = None
    meta: Optional[Dict[str, Any]] = None


class MLModelInfo(BaseModel):
    """Информация о ML модели"""
    model_name: str
    model_type: str
    is_trained: bool
    accuracy: Optional[float] = None
    last_trained: Optional[datetime] = None
    features: List[str] = []


class RecommendationInfo(BaseModel):
    """Информация о рекомендации"""
    product_id: int
    title: str
    score: float
    confidence: float
    reason: Optional[str] = None


class SegmentationInfo(BaseModel):
    """Информация о сегментации"""
    segment_id: int
    name: str
    size: int
    description: str
    characteristics: Dict[str, Any] = {}


class ChurnPredictionInfo(BaseModel):
    """Информация о предсказании оттока"""
    user_id: int
    churn_probability: float
    risk_level: str
    factors: List[str] = []
    recommendations: List[str] = []


class PriceElasticityInfo(BaseModel):
    """Информация о ценовой эластичности"""
    product_id: int
    elasticity: float
    price_sensitivity: str
    optimal_price: Optional[float] = None
    current_price: float


class AnomalyInfo(BaseModel):
    """Информация об аномалии"""
    anomaly_id: str
    type: str
    severity: str
    description: str
    detected_at: datetime
    data: Dict[str, Any] = {}


class DashboardMetrics(BaseModel):
    """Метрики дашборда"""
    total_users: int
    total_orders: int
    total_revenue: float
    active_users: int
    conversion_rate: float
    avg_order_value: float
    period: str = "30d"


class APIError(BaseModel):
    """Ошибка API"""
    error_code: str
    error_message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)

