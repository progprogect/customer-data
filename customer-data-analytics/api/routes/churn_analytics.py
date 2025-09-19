"""
Churn Analytics API Routes
FastAPI routes для аналитики оттока клиентов

Author: Customer Data Analytics Team
"""

from fastapi import APIRouter, HTTPException, Depends, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime, date
from pydantic import BaseModel, Field

from services.churn_service import churn_service
from services.ml_service import ml_service

logger = logging.getLogger(__name__)

# Security scheme для API ключей
security = HTTPBearer()

router = APIRouter()


def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """Проверка API ключа"""
    valid_tokens = [
        "dev-token-12345",
        "prod-token-67890"
    ]
    
    if credentials.credentials not in valid_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return credentials.credentials


class UserFeatures(BaseModel):
    """Модель фич пользователя"""
    user_id: int
    snapshot_date: date
    recency_days: Optional[float]
    frequency_90d: int
    monetary_180d: float
    aov_180d: Optional[float]
    orders_lifetime: int
    revenue_lifetime: float
    categories_unique: int


class ChurnPrediction(BaseModel):
    """Модель предсказания оттока"""
    user_id: int
    snapshot_date: date
    prob_churn_next_60d: float = Field(ge=0.0, le=1.0)
    will_churn: bool
    top_reasons: List[str]


class UserWithPrediction(BaseModel):
    """Модель пользователя с предсказанием"""
    user_info: Optional[Dict[str, Any]]
    features: UserFeatures
    prediction: ChurnPrediction


class ChurnStatistics(BaseModel):
    """Модель статистики оттока"""
    total_records: int
    churn_count: int
    retention_count: int
    churn_rate_percent: float
    unique_users: int
    date_range: Dict[str, Optional[str]]


class HighRiskUsersRequest(BaseModel):
    """Модель запроса для получения пользователей с высоким риском"""
    limit: int = Field(20, ge=1, le=100, description="Количество пользователей")
    threshold: float = Field(0.6, ge=0.0, le=1.0, description="Порог вероятности оттока")
    snapshot_date: Optional[str] = Field(None, description="Дата снапшота (YYYY-MM-DD)")


@router.get(
    "/statistics",
    response_model=ChurnStatistics,
    summary="Get churn statistics",
    description="Получение статистики по оттоку клиентов"
)
async def get_churn_statistics(
    api_key: str = Depends(verify_api_key)
) -> ChurnStatistics:
    """Получение статистики оттока"""
    try:
        logger.info("📊 Запрос статистики оттока")
        
        statistics = churn_service.get_churn_statistics()
        
        return ChurnStatistics(**statistics)
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения статистики: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/high-risk-users",
    response_model=List[UserWithPrediction],
    summary="Get high risk users",
    description="Получение пользователей с высоким риском оттока"
)
async def get_high_risk_users(
    limit: int = Query(20, ge=1, le=100, description="Количество пользователей"),
    threshold: float = Query(0.6, ge=0.0, le=1.0, description="Порог вероятности оттока"),
    snapshot_date: str = Query(None, description="Дата снапшота (YYYY-MM-DD)"),
    api_key: str = Depends(verify_api_key)
) -> List[UserWithPrediction]:
    """Получение пользователей с высоким риском оттока"""
    try:
        logger.info(f"👥 Запрос пользователей с высоким риском: limit={limit}, threshold={threshold}")
        
        # Проверяем что churn модель загружена
        if not ml_service.is_churn_model_loaded():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Churn prediction model is not loaded"
            )
        
        # Получаем пользователей с высоким риском (уже с примененной ML моделью)
        high_risk_users_data = churn_service.get_high_risk_users(limit=limit, threshold=threshold, snapshot_date=snapshot_date)
        
        if not high_risk_users_data:
            logger.warning(f"Не найдены пользователи с риском оттока ≥{threshold}")
            return []
        
        # Конвертируем в формат API
        high_risk_users = []
        for user_data in high_risk_users_data:
            try:
                # Получаем информацию о пользователе
                user_info = churn_service.get_user_info(user_data['user_id'])
                
                # Создаем объекты
                user_features = UserFeatures(
                    user_id=user_data['user_id'],
                    snapshot_date=user_data['snapshot_date'],
                    recency_days=user_data['recency_days'],
                    frequency_90d=user_data['frequency_90d'],
                    monetary_180d=user_data['monetary_180d'],
                    aov_180d=user_data['aov_180d'],
                    orders_lifetime=user_data['orders_lifetime'],
                    revenue_lifetime=user_data['revenue_lifetime'],
                    categories_unique=user_data['categories_unique']
                )
                
                prediction = ChurnPrediction(
                    user_id=user_data['user_id'],
                    snapshot_date=user_data['snapshot_date'],
                    prob_churn_next_60d=user_data['prob_churn_next_60d'],
                    will_churn=user_data['will_churn'],
                    top_reasons=user_data['top_reasons']
                )
                
                high_risk_users.append(UserWithPrediction(
                    user_info=user_info,
                    features=user_features,
                    prediction=prediction
                ))
                
            except Exception as e:
                logger.error(f"❌ Ошибка конвертации пользователя {user_data['user_id']}: {e}")
                continue
        
        logger.info(f"✅ Возвращено {len(high_risk_users)} пользователей с высоким риском")
        return high_risk_users
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей с высоким риском: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/high-risk-users",
    response_model=List[UserWithPrediction],
    summary="Get high risk users (POST)",
    description="Получение пользователей с высоким риском оттока через POST запрос"
)
async def get_high_risk_users_post(
    request: HighRiskUsersRequest,
    api_key: str = Depends(verify_api_key)
) -> List[UserWithPrediction]:
    """Получение пользователей с высоким риском оттока через POST"""
    try:
        logger.info(f"👥 POST запрос пользователей с высоким риском: limit={request.limit}, threshold={request.threshold}")
        
        # Проверяем что churn модель загружена
        if not ml_service.is_churn_model_loaded():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Churn prediction model is not loaded"
            )
        
        # Получаем пользователей с высоким риском (уже с примененной ML моделью)
        high_risk_users_data = churn_service.get_high_risk_users(
            limit=request.limit, 
            threshold=request.threshold, 
            snapshot_date=request.snapshot_date
        )
        
        if not high_risk_users_data:
            logger.warning(f"Не найдены пользователи с риском оттока ≥{request.threshold}")
            return []
        
        # Конвертируем в формат API
        high_risk_users = []
        for user_data in high_risk_users_data:
            try:
                # Получаем информацию о пользователе
                user_info = churn_service.get_user_info(user_data['user_id'])
                
                # Создаем объекты
                user_features = UserFeatures(
                    user_id=user_data['user_id'],
                    snapshot_date=user_data['snapshot_date'],
                    recency_days=user_data['recency_days'],
                    frequency_90d=user_data['frequency_90d'],
                    monetary_180d=user_data['monetary_180d'],
                    aov_180d=user_data['aov_180d'],
                    orders_lifetime=user_data['orders_lifetime'],
                    revenue_lifetime=user_data['revenue_lifetime'],
                    categories_unique=user_data['categories_unique']
                )
                
                prediction = ChurnPrediction(
                    user_id=user_data['user_id'],
                    snapshot_date=user_data['snapshot_date'],
                    prob_churn_next_60d=user_data['prob_churn_next_60d'],
                    will_churn=user_data['will_churn'],
                    top_reasons=user_data['top_reasons']
                )
                
                high_risk_users.append(UserWithPrediction(
                    user_info=user_info,
                    features=user_features,
                    prediction=prediction
                ))
                
            except Exception as e:
                logger.error(f"❌ Ошибка конвертации пользователя {user_data['user_id']}: {e}")
                continue
        
        logger.info(f"✅ Возвращено {len(high_risk_users)} пользователей с высоким риском")
        return high_risk_users
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка получения пользователей с высоким риском: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/user/{user_id}/features",
    response_model=UserFeatures,
    summary="Get user features",
    description="Получение фич пользователя для churn prediction"
)
async def get_user_features(
    user_id: int,
    snapshot_date: date,
    api_key: str = Depends(verify_api_key)
) -> UserFeatures:
    """Получение фич пользователя"""
    try:
        logger.info(f"👤 Запрос фич пользователя {user_id} на {snapshot_date}")
        
        features = churn_service.get_user_features(user_id, snapshot_date)
        
        if features is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Features not found for user {user_id} on {snapshot_date}"
            )
        
        # Добавляем user_id и snapshot_date
        features['user_id'] = user_id
        features['snapshot_date'] = snapshot_date
        
        return UserFeatures(**features)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Ошибка получения фич пользователя {user_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/health",
    summary="Health check",
    description="Проверка здоровья churn analytics сервиса"
)
async def churn_health_check():
    """Health check для churn analytics"""
    try:
        model_loaded = ml_service.is_churn_model_loaded()
        
        return {
            "status": "healthy" if model_loaded else "unhealthy",
            "churn_model_loaded": model_loaded,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Churn analytics health check error: {e}")
        return {
            "status": "unhealthy",
            "reason": f"error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }
