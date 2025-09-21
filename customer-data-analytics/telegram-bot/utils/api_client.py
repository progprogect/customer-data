"""
API Client
Универсальный клиент для интеграции с Customer Data Analytics API
"""

import httpx
import asyncio
import logging
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, date
import json

from utils.config import get_config

logger = logging.getLogger(__name__)
config = get_config()


class RateLimiter:
    """Rate limiter для предотвращения спама API запросов"""
    
    def __init__(self, max_calls: int = 1, time_window: int = 2):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = {}
    
    async def wait_if_needed(self, user_id: int):
        """Проверяет и ждет если нужно для соблюдения rate limit"""
        now = datetime.now().timestamp()
        user_key = str(user_id)
        
        if user_key not in self.calls:
            self.calls[user_key] = []
        
        # Удаляем старые вызовы
        self.calls[user_key] = [
            call_time for call_time in self.calls[user_key] 
            if now - call_time < self.time_window
        ]
        
        # Если превышен лимит, ждем
        if len(self.calls[user_key]) >= self.max_calls:
            sleep_time = self.time_window - (now - self.calls[user_key][0])
            if sleep_time > 0:
                logger.info(f"Rate limiting user {user_id}, waiting {sleep_time:.1f}s")
                await asyncio.sleep(sleep_time)
        
        # Добавляем текущий вызов
        self.calls[user_key].append(now)


class APIClient:
    """Клиент для работы с Customer Data Analytics API"""
    
    def __init__(self):
        self.base_url = config.API_URL.rstrip('/')
        self.api_key = config.API_KEY
        self.rate_limiter = RateLimiter(
            max_calls=1, 
            time_window=config.RATE_LIMIT_SECONDS
        )
        self.timeout = httpx.Timeout(10.0)
    
    async def _make_request(
        self, 
        method: str, 
        endpoint: str, 
        user_id: int,
        params: Optional[Dict] = None,
        data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Универсальный метод для API запросов"""
        
        # Rate limiting
        await self.rate_limiter.wait_if_needed(user_id)
        
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Telegram-Marketing-Bot/1.0"
        }
        
        # Добавляем API ключ если есть
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"API request: {method} {url}")
                
                if method.upper() == "GET":
                    response = await client.get(url, headers=headers, params=params)
                elif method.upper() == "POST":
                    response = await client.post(url, headers=headers, json=data, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                result = response.json()
                
                logger.info(f"API response: {response.status_code}")
                return result
                
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e.response.text}")
            raise APIClientError(f"API error {e.response.status_code}: {e.response.text}")
        except httpx.TimeoutException:
            logger.error(f"API timeout for {url}")
            raise APIClientError("API timeout - попробуйте позже")
        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            raise APIClientError("Ошибка подключения к API")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise APIClientError("Неожиданная ошибка API")
    
    # LTV Analytics
    async def get_ltv_summary(self, user_id: int) -> Dict[str, Any]:
        """Получение сводки LTV"""
        return await self._make_request("GET", "/api/v1/ltv/summary", user_id)
    
    async def get_ltv_users(
        self, 
        user_id: int, 
        page: int = 1, 
        page_size: int = None,
        **filters
    ) -> Dict[str, Any]:
        """Получение пользователей с LTV данными"""
        if page_size is None:
            page_size = config.PAGE_SIZE
            
        params = {
            "page": page,
            "page_size": page_size,
            **filters
        }
        return await self._make_request("GET", "/api/v1/ltv/users", user_id, params=params)
    
    async def get_ltv_distribution(self, user_id: int) -> Dict[str, Any]:
        """Получение распределения LTV"""
        return await self._make_request("GET", "/api/v1/ltv/distribution", user_id)
    
    # ML Predictions
    async def predict_purchase_probability(
        self, 
        user_id: int, 
        user_ids: List[int]
    ) -> Dict[str, Any]:
        """Предсказание вероятности покупки"""
        data = {
            "user_ids": user_ids,
            "threshold": config.DEFAULT_PROBABILITY_THRESHOLD
        }
        return await self._make_request("POST", "/api/v1/ml/purchase-probability", user_id, data=data)
    
    async def get_top_purchase_probability_users(
        self, 
        user_id: int, 
        threshold: float = 0.7,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Получение топ пользователей по вероятности покупки"""
        try:
            # Получаем список пользователей с реальными фичами
            users_data = await self.get_users_with_features(user_id, limit=limit * 2)
            if not users_data:
                logger.warning("Нет данных пользователей с фичами")
                return []
            
            # Пока ML API требует аутентификацию, используем упрощенную логику
            # Сортируем пользователей по активности (частота + давность) как индикатор вероятности покупки
            scored_users = []
            for user in users_data:
                if isinstance(user, dict) and 'user_id' in user and 'features' in user:
                    features = user['features']
                    
                    # Простая эвристика: высокая частота + низкая давность = высокая вероятность покупки
                    frequency = features.get("frequency_90d", 0)
                    recency = features.get("recency_days", 999)
                    
                    # Чем больше частота и чем меньше давность, тем выше "вероятность"
                    # Нормализуем: частота 0-10, давность 0-90 дней
                    frequency_score = min(frequency / 10.0, 1.0)
                    recency_score = max(0, (90 - recency) / 90.0)
                    
                    # Взвешенная оценка
                    probability = (frequency_score * 0.6 + recency_score * 0.4)
                    
                    scored_users.append({
                        "user_id": user['user_id'],
                        "prob_next_30d": probability,
                        "features": features,
                        "snapshot_date": user.get("snapshot_date", "2025-09-15")
                    })
            
            # Сортируем по "вероятности" и фильтруем по порогу
            scored_users.sort(key=lambda x: x.get('prob_next_30d', 0), reverse=True)
            filtered_users = [u for u in scored_users if u['prob_next_30d'] >= threshold]
            
            logger.info(f"Найдено {len(filtered_users)} пользователей с вероятностью >= {threshold}")
            return filtered_users[:limit]
            
        except Exception as e:
            logger.error(f"Ошибка получения топ пользователей по вероятности покупки: {e}")
            return []
    
    async def predict_churn(
        self, 
        user_id: int, 
        user_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Предсказание оттока клиента"""
        return await self._make_request("POST", "/api/v1/ml/churn-prediction", user_id, data=user_data)
    
    # Segments
    async def get_segments_distribution(
        self, 
        user_id: int, 
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Получение распределения сегментов"""
        params = {}
        if date:
            params["date"] = date
        return await self._make_request("GET", "/api/v1/segments/distribution", user_id, params=params)
    
    async def get_segments_meta(self, user_id: int) -> Dict[str, Any]:
        """Получение метаданных сегментов"""
        return await self._make_request("GET", "/api/v1/segments/meta", user_id)
    
    # Recommendations
    async def get_user_recommendations(
        self, 
        user_id: int, 
        target_user_id: int, 
        k: int = 5
    ) -> Dict[str, Any]:
        """Получение рекомендаций для пользователя"""
        params = {"user_id": target_user_id, "k": k}
        return await self._make_request("GET", "/api/v1/reco/user-hybrid", user_id, params=params)
    
    # Anomalies
    async def get_anomalies_weekly(
        self, 
        user_id: int, 
        date: Optional[str] = None,
        min_score: float = 3.0,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Получение недельных аномалий"""
        params = {
            "min_score": min_score,
            "limit": limit
        }
        if date:
            params["date"] = date
        return await self._make_request("GET", "/api/v1/anomalies/weekly", user_id, params=params)
    
    # Price Elasticity
    async def get_price_elasticity_categories(
        self, 
        user_id: int, 
        limit: int = 3
    ) -> Dict[str, Any]:
        """Получение категорий для анализа эластичности"""
        params = {"limit": limit}
        return await self._make_request("GET", "/api/v1/price-elasticity/categories", user_id, params=params)
    
    async def get_price_elasticity_data(
        self, 
        user_id: int, 
        category: str
    ) -> Dict[str, Any]:
        """Получение данных эластичности для категории"""
        params = {"category": category}
        return await self._make_request("GET", "/api/v1/price-elasticity/data", user_id, params=params)
    
    # Users
    async def get_user_ltv(self, user_id: int, target_user_id: int) -> Dict[str, Any]:
        """Получение LTV конкретного пользователя"""
        return await self._make_request("GET", f"/api/v1/users/{target_user_id}/ltv", user_id)
    
    async def get_user_events(
        self, 
        user_id: int, 
        target_user_id: int,
        limit: int = 10
    ) -> Dict[str, Any]:
        """Получение событий пользователя"""
        params = {"limit": limit}
        return await self._make_request("GET", f"/api/v1/users/{target_user_id}/events", user_id, params=params)
    
    async def get_simple_users(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Получение списка пользователей с базовыми данными"""
        params = {"limit": limit}
        return await self._make_request("GET", "/api/v1/users/", user_id, params=params)
    
    async def get_users_with_features(self, user_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """Получение списка пользователей с фичами для ML"""
        params = {"limit": limit}
        response = await self._make_request("GET", "/api/v1/direct-users", user_id, params=params)
        if isinstance(response, dict) and 'users' in response:
            return response['users'][:limit]
        elif isinstance(response, list):
            return response[:limit]
        return []
    
    # Telegram Bot API
    async def get_telegram_user_summary(self, user_id: int, target_user_id: int) -> Dict[str, Any]:
        """Получение полной сводки пользователя для Telegram бота"""
        return await self._make_request("GET", f"/api/v1/telegram-db/user-summary/{target_user_id}", user_id)
    
    async def get_telegram_available_users(self, user_id: int, limit: int = 20) -> Dict[str, Any]:
        """Получение списка доступных пользователей для тестирования"""
        params = {"limit": limit}
        return await self._make_request("GET", "/api/v1/telegram/users/available", user_id, params=params)
    
    # Analytics Dashboard
    async def get_dashboard_data(self, user_id: int) -> Dict[str, Any]:
        """Получение данных дашборда"""
        return await self._make_request("GET", "/api/v1/analytics/dashboard", user_id)
    
    # ML Predictions with API Key
    async def get_user_purchase_probability(
        self, 
        target_user_id: int, 
        features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Получение готового предсказания вероятности покупки для пользователя"""
        try:
            # Подготавливаем данные для ML API
            prediction_request = {
                "rows": [{
                    "user_id": target_user_id,
                    "snapshot_date": "2025-09-21",
                    "features": features
                }],
                "model_version": None
            }
            
            # Делаем запрос к ML API с API key
            headers = {"Authorization": f"Bearer {config.API_KEY}"}
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{config.API_URL}/api/v1/ml/purchase-probability",
                    json=prediction_request,
                    headers=headers,
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("results") and len(result["results"]) > 0:
                        return result["results"][0]  # Возвращаем первый результат
                    else:
                        logger.warning(f"No prediction results for user {target_user_id}")
                        return None
                else:
                    logger.error(f"ML API error {response.status_code}: {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting purchase probability for user {target_user_id}: {e}")
            return None
    
    async def get_user_churn_prediction(
        self, 
        target_user_id: int, 
        features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Получение готового предсказания оттока для пользователя"""
        try:
            # Подготавливаем данные для churn ML API
            churn_request = {
                "user_id": target_user_id,
                "snapshot_date": "2025-09-21",
                "recency_days": features.get("recency_days", 0),
                "frequency_90d": features.get("frequency_90d", 0),
                "monetary_180d": features.get("monetary_180d", 0),
                "aov_180d": features.get("aov_180d", 0),
                "orders_lifetime": features.get("orders_lifetime", 0),
                "revenue_lifetime": features.get("revenue_lifetime", 0),
                "categories_unique": features.get("categories_unique", 0)
            }
            
            # Делаем запрос к churn ML API с API key
            headers = {"Authorization": f"Bearer {config.API_KEY}"}
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{config.API_URL}/api/v1/ml/churn-prediction",
                    json=churn_request,
                    headers=headers,
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"Churn ML API error {response.status_code}: {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting churn prediction for user {target_user_id}: {e}")
            return None


class APIClientError(Exception):
    """Исключение для ошибок API клиента"""
    pass


# Глобальный экземпляр клиента
api_client = APIClient()
