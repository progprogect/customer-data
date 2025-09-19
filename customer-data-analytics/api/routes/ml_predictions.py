"""
ML Predictions API Routes
FastAPI routes для предсказания покупок

Author: Customer Data Analytics Team
"""

from fastapi import APIRouter, HTTPException, Depends, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional
import logging
import time
import numpy as np
from datetime import datetime

from models.ml_models import (
    PredictionRequest, PredictionResponse, PredictionResult,
    ThresholdRequest, ModelInfo
)
from services.ml_service import ml_service

logger = logging.getLogger(__name__)

# Security scheme для API ключей
security = HTTPBearer()

router = APIRouter()


def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    """
    Простая проверка API ключа
    В production стоит использовать более серьёзную аутентификацию
    """
    # Простая проверка токена (в production должно быть сложнее)
    valid_tokens = [
        "dev-token-12345",  # Для разработки
        "prod-token-67890"  # Для продакшена
    ]
    
    if credentials.credentials not in valid_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return credentials.credentials


@router.post(
    "/purchase-probability",
    response_model=PredictionResponse,
    summary="Predict purchase probability",
    description="Предсказание вероятности покупки в следующие 30 дней для batch пользователей"
)
async def predict_purchase_probability(
    request: PredictionRequest,
    api_key: str = Depends(verify_api_key)
) -> PredictionResponse:
    """
    Основной endpoint для предсказания вероятности покупки
    
    Args:
        request: Запрос с данными пользователей
        api_key: API ключ для аутентификации
        
    Returns:
        PredictionResponse: Результаты предсказания
    """
    start_time = time.time()
    
    try:
        # Проверяем что модель загружена
        if not ml_service.is_model_loaded():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ML model is not loaded. Please contact administrator."
            )
        
        # Логируем запрос
        logger.info(f"🔮 Purchase probability prediction request: {len(request.rows)} rows")
        
        # Определяем версию модели
        used_model_version = request.model_version or ml_service.model_version
        
        # Валидация версии модели
        if request.model_version and request.model_version != ml_service.model_version:
            logger.warning(f"⚠️ Requested model version {request.model_version} != loaded {ml_service.model_version}")
            # В простой реализации используем загруженную модель
        
        # Подготовка данных для предсказания
        results = []
        successful_predictions = 0
        failed_predictions = 0
        
        # Batch обработка для эффективности
        batch_features = []
        batch_metadata = []
        
        for row in request.rows:
            try:
                # Валидация признаков
                feature_errors = ml_service.validate_features(row.features.dict())
                if feature_errors:
                    # Добавляем ошибку для этой строки
                    results.append(PredictionResult(
                        user_id=row.user_id,
                        snapshot_date=row.snapshot_date,
                        prob_next_30d=None,
                        threshold_applied=False,
                        error=f"Feature validation failed: {'; '.join(feature_errors)}"
                    ))
                    failed_predictions += 1
                    continue
                
                # Добавляем в batch
                batch_features.append(row.features.dict())
                batch_metadata.append({
                    'user_id': row.user_id,
                    'snapshot_date': row.snapshot_date
                })
                
            except Exception as e:
                logger.error(f"❌ Error processing row for user {row.user_id}: {e}")
                results.append(PredictionResult(
                    user_id=row.user_id,
                    snapshot_date=row.snapshot_date,
                    prob_next_30d=None,
                    threshold_applied=False,
                    error=f"Processing error: {str(e)}"
                ))
                failed_predictions += 1
        
        # Выполняем batch предсказание если есть валидные данные
        processing_time_ms = 0.0
        
        if batch_features:
            try:
                probabilities, batch_processing_time = ml_service.predict_batch(batch_features)
                processing_time_ms = batch_processing_time
                
                # Добавляем успешные результаты
                for i, (prob, metadata) in enumerate(zip(probabilities, batch_metadata)):
                    results.append(PredictionResult(
                        user_id=metadata['user_id'],
                        snapshot_date=metadata['snapshot_date'],
                        prob_next_30d=float(prob),
                        threshold_applied=False,
                        error=None
                    ))
                    successful_predictions += 1
                
                # Логируем статистику batch
                logger.info(f"📊 Batch prediction stats:")
                logger.info(f"   Size: {len(probabilities)}")
                logger.info(f"   Min prob: {np.min(probabilities):.3f}")
                logger.info(f"   Median prob: {np.median(probabilities):.3f}")
                logger.info(f"   Max prob: {np.max(probabilities):.3f}")
                logger.info(f"   Processing time: {processing_time_ms:.1f}ms")
                
            except Exception as e:
                logger.error(f"❌ Batch prediction error: {e}")
                # Добавляем ошибки для всех в batch
                for metadata in batch_metadata:
                    results.append(PredictionResult(
                        user_id=metadata['user_id'],
                        snapshot_date=metadata['snapshot_date'],
                        prob_next_30d=None,
                        threshold_applied=False,
                        error=f"Batch prediction failed: {str(e)}"
                    ))
                    failed_predictions += 1
        
        # Общее время выполнения
        total_time_ms = (time.time() - start_time) * 1000
        
        # Формируем ответ
        response = PredictionResponse(
            model_version=used_model_version,
            count=len(request.rows),
            successful_predictions=successful_predictions,
            failed_predictions=failed_predictions,
            processing_time_ms=processing_time_ms,
            results=results
        )
        
        logger.info(f"✅ Prediction completed: {successful_predictions} success, {failed_predictions} failed, {total_time_ms:.1f}ms total")
        
        return response
        
    except HTTPException:
        # Повторно выбрасываем HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in prediction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.post(
    "/purchase-probability/apply-threshold",
    response_model=PredictionResponse,
    summary="Predict with threshold",
    description="Предсказание с применением порога для принятия решения о таргетинге"
)
async def predict_with_threshold(
    request: ThresholdRequest,
    api_key: str = Depends(verify_api_key)
) -> PredictionResponse:
    """
    Endpoint для предсказания с применением порога
    
    Args:
        request: Запрос с данными и порогом
        api_key: API ключ для аутентификации
        
    Returns:
        PredictionResponse: Результаты с флагом таргетинга
    """
    start_time = time.time()
    
    try:
        # Логируем запрос
        logger.info(f"🎯 Threshold prediction request: {len(request.rows)} rows, threshold={request.threshold}")
        
        # Выполняем обычное предсказание
        base_request = PredictionRequest(
            model_version=request.model_version,
            rows=request.rows
        )
        
        # Получаем результаты без порога
        base_response = await predict_purchase_probability(base_request, api_key)
        
        # Применяем порог
        for result in base_response.results:
            if result.prob_next_30d is not None:
                result.threshold_applied = True
                result.will_target = result.prob_next_30d >= request.threshold
            else:
                result.threshold_applied = False
                result.will_target = None
        
        # Обновляем время
        total_time_ms = (time.time() - start_time) * 1000
        base_response.processing_time_ms = total_time_ms
        
        # Логируем статистику с порогом
        successful_results = [r for r in base_response.results if r.prob_next_30d is not None]
        if successful_results:
            targeted_count = sum(1 for r in successful_results if r.will_target)
            targeting_rate = targeted_count / len(successful_results) * 100
            logger.info(f"🎯 Threshold {request.threshold}: {targeted_count}/{len(successful_results)} users targeted ({targeting_rate:.1f}%)")
        
        return base_response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error in threshold prediction: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/model-info",
    response_model=ModelInfo,
    summary="Get model information",
    description="Получение информации о загруженной модели"
)
async def get_model_info(
    api_key: str = Depends(verify_api_key)
) -> ModelInfo:
    """
    Endpoint для получения информации о модели
    
    Args:
        api_key: API ключ для аутентификации
        
    Returns:
        ModelInfo: Информация о модели
    """
    try:
        if not ml_service.is_model_loaded():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ML model is not loaded"
            )
        
        model_info = ml_service.get_model_info()
        
        return ModelInfo(
            model_version=model_info['model_version'],
            load_timestamp=model_info['load_timestamp'],
            feature_names=model_info['feature_names'],
            feature_count=model_info['feature_count'],
            model_performance=model_info['model_performance']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error getting model info: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        )


@router.get(
    "/health",
    summary="Health check",
    description="Проверка здоровья ML сервиса"
)
async def ml_health_check():
    """Health check для ML сервиса"""
    try:
        if not ml_service.is_model_loaded():
            return {
                "status": "unhealthy",
                "reason": "model_not_loaded",
                "timestamp": datetime.now().isoformat()
            }
        
        model_info = ml_service.get_model_info()
        
        return {
            "status": "healthy",
            "model_version": model_info['model_version'],
            "model_loaded": True,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ ML health check error: {e}")
        return {
            "status": "unhealthy",
            "reason": f"error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }
