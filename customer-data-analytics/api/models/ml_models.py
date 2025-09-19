"""
ML Models for Purchase Probability API
Pydantic модели для API контракта ML inference

Author: Customer Data Analytics Team
"""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import date
import re


class UserFeatures(BaseModel):
    """Модель признаков пользователя для предсказания"""
    
    recency_days: Optional[float] = Field(
        None, 
        description="Дни с последней покупки",
        ge=0
    )
    frequency_90d: Optional[int] = Field(
        None,
        description="Количество заказов за последние 90 дней", 
        ge=0
    )
    monetary_180d: Optional[float] = Field(
        None,
        description="Потраченная сумма за последние 180 дней",
        ge=0
    )
    aov_180d: Optional[float] = Field(
        None,
        description="Средний чек за последние 180 дней",
        ge=0
    )
    orders_lifetime: Optional[int] = Field(
        None,
        description="Общее количество заказов за всё время",
        ge=0
    )
    revenue_lifetime: Optional[float] = Field(
        None,
        description="Общая выручка за всё время",
        ge=0
    )
    categories_unique: Optional[int] = Field(
        None,
        description="Количество уникальных категорий покупок",
        ge=0
    )

    class Config:
        json_schema_extra = {
            "example": {
                "recency_days": 12.0,
                "frequency_90d": 3,
                "monetary_180d": 1480.5,
                "aov_180d": 493.5,
                "orders_lifetime": 7,
                "revenue_lifetime": 5340.2,
                "categories_unique": 4
            }
        }


class PredictionRow(BaseModel):
    """Модель одной строки для предсказания"""
    
    user_id: int = Field(..., description="ID пользователя", gt=0)
    snapshot_date: date = Field(..., description="Дата снапшота (ISO format)")
    features: UserFeatures = Field(..., description="Признаки пользователя")

    @validator('snapshot_date')
    def validate_snapshot_date(cls, v):
        """Валидация даты снапшота"""
        # Проверяем что дата не в далёком будущем
        from datetime import date, timedelta
        max_date = date.today() + timedelta(days=30)
        if v > max_date:
            raise ValueError(f"Snapshot date cannot be more than 30 days in future: {v}")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 123,
                "snapshot_date": "2025-09-18",
                "features": {
                    "recency_days": 12.0,
                    "frequency_90d": 3,
                    "monetary_180d": 1480.5,
                    "aov_180d": 493.5,
                    "orders_lifetime": 7,
                    "revenue_lifetime": 5340.2,
                    "categories_unique": 4
                }
            }
        }


class PredictionRequest(BaseModel):
    """Модель запроса на предсказание"""
    
    model_version: Optional[str] = Field(
        None,
        description="Версия модели (если не указана, используется текущая prod)",
        pattern=r"^\d{8}_\d{6}$"
    )
    rows: List[PredictionRow] = Field(
        ...,
        description="Массив строк для предсказания",
        min_items=1,
        max_items=1000
    )

    @validator('model_version')
    def validate_model_version(cls, v):
        """Валидация версии модели"""
        if v is not None:
            pattern = r"^\d{8}_\d{6}$"
            if not re.match(pattern, v):
                raise ValueError("Model version must be in format YYYYMMDD_HHMMSS")
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "model_version": "20250919_160301",
                "rows": [
                    {
                        "user_id": 123,
                        "snapshot_date": "2025-09-18",
                        "features": {
                            "recency_days": 12.0,
                            "frequency_90d": 3,
                            "monetary_180d": 1480.5,
                            "aov_180d": 493.5,
                            "orders_lifetime": 7,
                            "revenue_lifetime": 5340.2,
                            "categories_unique": 4
                        }
                    }
                ]
            }
        }


class PredictionResult(BaseModel):
    """Модель результата предсказания для одного пользователя"""
    
    user_id: int = Field(..., description="ID пользователя")
    snapshot_date: date = Field(..., description="Дата снапшота")
    prob_next_30d: Optional[float] = Field(
        None,
        description="Вероятность покупки в следующие 30 дней",
        ge=0.0,
        le=1.0
    )
    threshold_applied: bool = Field(
        False,
        description="Был ли применён порог (для endpoint с порогом)"
    )
    will_target: Optional[bool] = Field(
        None,
        description="Рекомендация таргетинга (только если threshold_applied=true)"
    )
    error: Optional[str] = Field(
        None,
        description="Описание ошибки для данной строки (если есть)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": 123,
                "snapshot_date": "2025-09-18",
                "prob_next_30d": 0.8123,
                "threshold_applied": False,
                "will_target": None,
                "error": None
            }
        }


class PredictionResponse(BaseModel):
    """Модель ответа API"""
    
    model_version: str = Field(..., description="Версия модели, которая была использована")
    count: int = Field(..., description="Количество обработанных строк")
    successful_predictions: int = Field(
        ...,
        description="Количество успешных предсказаний"
    )
    failed_predictions: int = Field(
        ...,
        description="Количество неудачных предсказаний"
    )
    processing_time_ms: float = Field(
        ...,
        description="Время обработки в миллисекундах"
    )
    results: List[PredictionResult] = Field(
        ...,
        description="Массив результатов предсказаний"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "model_version": "20250919_160301",
                "count": 1,
                "successful_predictions": 1,
                "failed_predictions": 0,
                "processing_time_ms": 45.2,
                "results": [
                    {
                        "user_id": 123,
                        "snapshot_date": "2025-09-18",
                        "prob_next_30d": 0.8123,
                        "threshold_applied": False,
                        "will_target": None,
                        "error": None
                    }
                ]
            }
        }


class ThresholdRequest(BaseModel):
    """Модель запроса с применением порога"""
    
    model_version: Optional[str] = Field(
        None,
        description="Версия модели",
        pattern=r"^\d{8}_\d{6}$"
    )
    threshold: float = Field(
        ...,
        description="Порог вероятности для принятия решения",
        ge=0.0,
        le=1.0
    )
    rows: List[PredictionRow] = Field(
        ...,
        description="Массив строк для предсказания",
        min_items=1,
        max_items=1000
    )

    class Config:
        json_schema_extra = {
            "example": {
                "model_version": "20250919_160301",
                "threshold": 0.65,
                "rows": [
                    {
                        "user_id": 123,
                        "snapshot_date": "2025-09-18",
                        "features": {
                            "recency_days": 12.0,
                            "frequency_90d": 3,
                            "monetary_180d": 1480.5,
                            "aov_180d": 493.5,
                            "orders_lifetime": 7,
                            "revenue_lifetime": 5340.2,
                            "categories_unique": 4
                        }
                    }
                ]
            }
        }


class ModelInfo(BaseModel):
    """Модель информации о загруженной модели"""
    
    model_version: str = Field(..., description="Версия модели")
    load_timestamp: str = Field(..., description="Время загрузки модели")
    feature_names: List[str] = Field(..., description="Названия признаков")
    feature_count: int = Field(..., description="Количество признаков")
    model_performance: Dict[str, Any] = Field(..., description="Метрики модели")
    
    class Config:
        json_schema_extra = {
            "example": {
                "model_version": "20250919_160301",
                "load_timestamp": "2025-09-19T16:05:00Z",
                "feature_names": [
                    "recency_days", "frequency_90d", "monetary_180d", 
                    "aov_180d", "orders_lifetime", "revenue_lifetime", "categories_unique"
                ],
                "feature_count": 7,
                "model_performance": {
                    "test_precision": 0.641,
                    "test_recall": 0.687,
                    "test_f1": 0.663
                }
            }
        }
