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


# =========================
# Behavior Weekly Models
# =========================

class UserBehaviorWeekly(BaseModel):
    """Модель недельного поведения пользователя"""
    user_id: int = Field(..., description="ID пользователя")
    week_start_date: str = Field(..., description="Начало недели (YYYY-MM-DD)")
    orders_count: int = Field(..., description="Количество заказов за неделю")
    monetary_sum: float = Field(..., description="Сумма покупок за неделю")
    categories_count: int = Field(..., description="Количество уникальных категорий")
    aov_weekly: Optional[float] = Field(None, description="Средний чек за неделю")


class BehaviorWeeklyRequest(BaseModel):
    """Запрос данных недельного поведения"""
    user_id: Optional[int] = Field(None, description="ID пользователя (опционально)")
    start_date: Optional[str] = Field(None, description="Начальная дата (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="Конечная дата (YYYY-MM-DD)")
    min_orders: Optional[int] = Field(0, description="Минимальное количество заказов")
    min_monetary: Optional[float] = Field(0, description="Минимальная сумма покупок")


class BehaviorWeeklyResponse(BaseModel):
    """Ответ с данными недельного поведения"""
    data: List[UserBehaviorWeekly] = Field(..., description="Данные поведения пользователей")
    total_count: int = Field(..., description="Общее количество записей")
    date_range: Dict[str, str] = Field(..., description="Диапазон дат")
    summary_stats: Dict[str, Any] = Field(..., description="Сводная статистика")


class AnomalyDetectionRequest(BaseModel):
    """Запрос детекции аномалий"""
    user_id: Optional[int] = Field(None, description="ID пользователя (опционально)")
    metric: str = Field("orders_count", description="Метрика для анализа")
    threshold: float = Field(2.0, description="Порог для детекции аномалий (z-score)")
    window_size: int = Field(4, description="Размер окна для rolling average")


class AnomalyDetectionResponse(BaseModel):
    """Ответ с обнаруженными аномалиями"""
    anomalies: List[Dict[str, Any]] = Field(..., description="Список аномалий")
    total_anomalies: int = Field(..., description="Общее количество аномалий")
    anomaly_rate: float = Field(..., description="Процент аномалий")
    threshold: float = Field(..., description="Использованный порог")
    metric: str = Field(..., description="Анализируемая метрика")


# =========================
# Anomalies Weekly Models
# =========================

class UserAnomalyWeekly(BaseModel):
    """Модель недельной аномалии пользователя"""
    user_id: int = Field(..., description="ID пользователя")
    week_start: str = Field(..., description="Начало недели (YYYY-MM-DD)")
    anomaly_score: float = Field(..., description="Максимальный z-score или ln(ratio)")
    is_anomaly: bool = Field(..., description="Флаг аномалии")
    triggers: List[str] = Field(..., description="Список сработавших триггеров")
    insufficient_history: bool = Field(..., description="Недостаточно истории (< 4 недель)")


class AnomaliesWeeklyRequest(BaseModel):
    """Запрос аномалий за неделю"""
    date: Optional[str] = Field(None, description="Дата недели (YYYY-MM-DD)")
    min_score: float = Field(3.0, description="Минимальный anomaly_score")
    limit: int = Field(100, description="Максимальное количество записей")


class AnomaliesWeeklyResponse(BaseModel):
    """Ответ с аномалиями за неделю"""
    anomalies: List[UserAnomalyWeekly] = Field(..., description="Список аномалий")
    total_count: int = Field(..., description="Общее количество аномалий")
    week_date: str = Field(..., description="Дата недели")
    summary_stats: Dict[str, Any] = Field(..., description="Сводная статистика")


class UserAnomaliesRequest(BaseModel):
    """Запрос истории аномалий пользователя"""
    user_id: int = Field(..., description="ID пользователя")
    weeks: int = Field(12, description="Количество недель истории")
    min_score: float = Field(0.0, description="Минимальный anomaly_score")


class UserBehaviorData(BaseModel):
    """Модель данных поведения пользователя"""
    week_start: str = Field(..., description="Начало недели (YYYY-MM-DD)")
    orders_count: int = Field(..., description="Количество заказов за неделю")
    monetary_sum: float = Field(..., description="Сумма покупок за неделю")
    categories_count: int = Field(..., description="Количество уникальных категорий")
    aov_weekly: Optional[float] = Field(None, description="Средний чек за неделю")


class UserAnomaliesResponse(BaseModel):
    """Ответ с историей аномалий пользователя"""
    user_id: int = Field(..., description="ID пользователя")
    anomalies: List[UserAnomalyWeekly] = Field(..., description="История аномалий")
    behavior_data: List[UserBehaviorData] = Field(..., description="Данные поведения")
    total_anomalies: int = Field(..., description="Общее количество аномалий")
    anomaly_rate: float = Field(..., description="Процент аномальных недель")
    top_triggers: List[Dict[str, Any]] = Field(..., description="Топ триггеров")


class AnomalyStatsResponse(BaseModel):
    """Ответ со статистикой аномалий"""
    total_weeks: int = Field(..., description="Общее количество недель")
    total_anomalies: int = Field(..., description="Общее количество аномалий")
    anomaly_rate: float = Field(..., description="Процент аномалий")
    top_users: List[Dict[str, Any]] = Field(..., description="Топ пользователей по аномалиям")
    top_triggers: List[Dict[str, Any]] = Field(..., description="Топ триггеров")
    weekly_distribution: List[Dict[str, Any]] = Field(..., description="Распределение по неделям")
