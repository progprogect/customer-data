"""
Telegram Bot API Routes - Fixed Version
Показывает реальные данные или "нет данных"
"""

from fastapi import APIRouter, HTTPException
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
import httpx
import asyncio
import os

router = APIRouter(prefix="/api/v1/telegram", tags=["telegram"])
logger = logging.getLogger(__name__)

API_URL = os.getenv("API_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "dev-token-12345")


def analyze_user_segment_by_ltv(ltv: float) -> str:
    """Анализ сегмента пользователя на основе LTV"""
    if ltv >= 5000:
        return "VIP"
    elif ltv >= 1000:
        return "Обычные"
    else:
        return "Новые/Неактивные"


@router.get("/user-summary/{user_id}")
async def get_user_summary_for_telegram(user_id: int) -> Dict[str, Any]:
    """
    Получение полной сводки пользователя для Telegram бота
    Показывает реальные данные или "нет данных"
    """
    try:
        user_summary = {
            "user_id": user_id,
            "segment": "Новые/Неактивные",
            "ltv_12m": 0.0,
            "last_order_days": None,
            "prob_purchase_30d": None,
            "prob_churn_60d": None,
            "recommendations": [],
            "anomalies": [],
            "features": {},
            "snapshot_date": datetime.now().isoformat()
        }
        
        async with httpx.AsyncClient() as client:
            headers = {"Authorization": f"Bearer {API_KEY}"}
            
            # 1. LTV Data
            try:
                ltv_response = await client.get(f"{API_URL}/api/v1/users/{user_id}/ltv", headers=headers, timeout=10.0)
                ltv_response.raise_for_status()
                ltv_data = ltv_response.json()
                if ltv_data:
                    ltv_value = ltv_data.get("ltv", 0)
                    user_summary["ltv_12m"] = float(ltv_value)
                    user_summary["segment"] = analyze_user_segment_by_ltv(ltv_value)
                    logger.info(f"Got LTV for user {user_id}: {ltv_value}")
            except httpx.HTTPStatusError as e:
                logger.warning(f"LTV API error for user {user_id}: {e.response.status_code}")
            except httpx.RequestError as e:
                logger.error(f"LTV API request error for user {user_id}: {e}")
            
            # 2. Recommendations
            try:
                reco_response = await client.get(f"{API_URL}/api/v1/reco/user-hybrid?user_id={user_id}&k=3", headers=headers, timeout=10.0)
                reco_response.raise_for_status()
                reco_data = reco_response.json()
                if reco_data:
                    if isinstance(reco_data, list):
                        user_summary["recommendations"] = reco_data[:3]
                    elif isinstance(reco_data, dict) and 'recommendations' in reco_data:
                        user_summary["recommendations"] = reco_data['recommendations'][:3]
                    logger.info(f"Got recommendations for user {user_id}: {len(user_summary['recommendations'])} items")
            except httpx.HTTPStatusError as e:
                logger.warning(f"Recommendations API error for user {user_id}: {e.response.status_code}")
            except httpx.RequestError as e:
                logger.error(f"Recommendations API request error for user {user_id}: {e}")
            
            # 3. Anomalies
            try:
                anomalies_response = await client.get(f"{API_URL}/api/v1/anomalies/weekly?min_score=3.0&limit=100", headers=headers, timeout=10.0)
                anomalies_response.raise_for_status()
                anomalies_data = anomalies_response.json()
                if anomalies_data and 'anomalies' in anomalies_data:
                    user_anomalies = [
                        a for a in anomalies_data['anomalies']
                        if a.get('user_id') == user_id
                    ]
                    user_summary["anomalies"] = user_anomalies[:5]
                    logger.info(f"Got anomalies for user {user_id}: {len(user_summary['anomalies'])} items")
            except httpx.HTTPStatusError as e:
                logger.warning(f"Anomalies API error for user {user_id}: {e.response.status_code}")
            except httpx.RequestError as e:
                logger.error(f"Anomalies API request error for user {user_id}: {e}")
            
            # 4. Features for ML predictions
            user_features = None
            for attempt in range(3):  # Попробуем 3 раза
                try:
                    features_response = await client.get(f"{API_URL}/api/v1/direct-users?limit=500", headers=headers, timeout=10.0)
                    features_response.raise_for_status()
                    features_data = features_response.json()
                    
                    if features_data and 'users' in features_data:
                        for user_f in features_data['users']:
                            if user_f.get('user_id') == user_id:
                                user_features = user_f.get('features', {})
                                user_summary["features"] = user_features
                                logger.info(f"Found features for user {user_id} on attempt {attempt + 1}")
                                break
                    
                    if user_features:
                        break
                    
                    if attempt < 2:
                        logger.warning(f"User {user_id} not found in features on attempt {attempt + 1}, retrying...")
                    
                except httpx.HTTPStatusError as e:
                    logger.warning(f"Features API error for user {user_id} on attempt {attempt + 1}: {e.response.status_code}")
                except httpx.RequestError as e:
                    logger.error(f"Features API request error for user {user_id} on attempt {attempt + 1}: {e}")
                
                # Небольшая задержка перед повторной попыткой
                if attempt < 2:
                    await asyncio.sleep(0.5)
                
                if user_features:
                    # Calculate ML predictions based on features
                    recency_days = user_features.get("recency_days", 999)
                    frequency_90d = user_features.get("frequency_90d", 0)
                    
                    # Simple heuristic for purchase probability
                    if frequency_90d == 0:
                        prob_purchase = 0.1
                    elif recency_days <= 7:
                        prob_purchase = 0.8
                    elif recency_days <= 30:
                        prob_purchase = 0.6
                    elif recency_days <= 90:
                        prob_purchase = 0.4
                    else:
                        prob_purchase = 0.2
                    
                    # Adjust based on frequency
                    if frequency_90d >= 5:
                        prob_purchase = min(0.9, prob_purchase + 0.2)
                    elif frequency_90d >= 3:
                        prob_purchase = min(0.8, prob_purchase + 0.1)
                    
                    # Simple heuristic for churn probability
                    if recency_days <= 7:
                        prob_churn = 0.1
                    elif recency_days <= 30:
                        prob_churn = 0.3
                    elif recency_days <= 90:
                        prob_churn = 0.6
                    else:
                        prob_churn = 0.8
                    
                    # Adjust based on frequency
                    if frequency_90d >= 5:
                        prob_churn = max(0.1, prob_churn - 0.3)
                    elif frequency_90d >= 3:
                        prob_churn = max(0.2, prob_churn - 0.2)
                    
                    user_summary["prob_purchase_30d"] = round(prob_purchase, 2)
                    user_summary["prob_churn_60d"] = round(prob_churn, 2)
                    
                    logger.info(f"Calculated ML predictions for user {user_id}: purchase={prob_purchase:.2f}, churn={prob_churn:.2f}")
                else:
                    logger.info(f"No features found for user {user_id}, keeping predictions as None")
                    
            except httpx.HTTPStatusError as e:
                logger.warning(f"Features API error for user {user_id}: {e.response.status_code}")
            except httpx.RequestError as e:
                logger.error(f"Features API request error for user {user_id}: {e}")
        
        logger.info(f"Generated user summary for user {user_id}")
        return user_summary
        
    except Exception as e:
        logger.error(f"Error generating user summary for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения сводки пользователя: {str(e)}")


@router.get("/users/available")
async def get_available_users() -> Dict[str, Any]:
    """
    Получение списка доступных пользователей для тестирования
    """
    try:
        # Возвращаем список тестовых пользователей
        users = []
        for i in range(1, 21):
            users.append({
                "user_id": i,
                "orders_count": i * 2,
                "total_revenue": float(i * 500),
                "last_order_date": "2025-09-14T10:30:00"
            })
        
        return {
            "users": users,
            "total_count": len(users),
            "snapshot_date": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting available users: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка получения списка пользователей: {str(e)}")
