"""
Churn Prediction Service
Сервис для получения фич пользователей и предсказания оттока

Author: Customer Data Analytics Team
"""

import psycopg2
import pandas as pd
import logging
from typing import Dict, List, Optional, Any
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class ChurnService:
    """Сервис для работы с churn prediction"""
    
    def __init__(self):
        self.db_config = {
            'host': 'localhost',
            'database': 'customer_data',
            'user': 'mikitavalkunovich',
            'port': '5432'
        }
        logger.info("💔 Churn Service инициализирован")
    
    def get_user_features(self, user_id: int, snapshot_date: date) -> Optional[Dict[str, Any]]:
        """
        Получение фич пользователя для churn prediction
        
        Args:
            user_id: ID пользователя
            snapshot_date: Дата снапшота
            
        Returns:
            Dict с фичами или None если не найдено
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Получаем фичи из ml_user_features_daily_all
            query = """
            SELECT 
                recency_days,
                frequency_90d,
                monetary_180d,
                aov_180d,
                orders_lifetime,
                revenue_lifetime,
                categories_unique
            FROM ml_user_features_daily_all
            WHERE user_id = %s AND snapshot_date = %s
            """
            
            cursor.execute(query, (user_id, snapshot_date))
            result = cursor.fetchone()
            
            if result is None:
                logger.warning(f"Фичи не найдены для user_id={user_id}, snapshot_date={snapshot_date}")
                return None
            
            features = {
                'recency_days': result[0],
                'frequency_90d': result[1],
                'monetary_180d': float(result[2]),
                'aov_180d': result[3],
                'orders_lifetime': result[4],
                'revenue_lifetime': float(result[5]),
                'categories_unique': result[6]
            }
            
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Фичи получены для user_id={user_id}")
            return features
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения фич для user_id={user_id}: {e}")
            return None
    
    def get_high_risk_users(self, limit: int = 20, threshold: float = 0.6, snapshot_date: str = None) -> List[Dict[str, Any]]:
        """
        Получение пользователей с высоким риском оттока
        
        Args:
            limit: Количество пользователей
            threshold: Порог вероятности оттока
            snapshot_date: Дата снапшота (если не указана, берется последняя)
            
        Returns:
            Список пользователей с высоким риском
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Определяем дату снапшота
            if not snapshot_date:
                latest_snapshot_query = """
                SELECT MAX(snapshot_date) 
                FROM ml_user_features_daily_all
                """
                cursor.execute(latest_snapshot_query)
                snapshot_date = cursor.fetchone()[0]
            
            if not snapshot_date:
                logger.warning("Не найдены снапшоты в ml_user_features_daily_all")
                return []
            
            # Получаем пользователей с фичами
            features_query = """
            SELECT 
                user_id,
                snapshot_date,
                recency_days,
                frequency_90d,
                monetary_180d,
                aov_180d,
                orders_lifetime,
                revenue_lifetime,
                categories_unique
            FROM ml_user_features_daily_all
            WHERE snapshot_date = %s
            ORDER BY user_id
            LIMIT %s
            """
            
            cursor.execute(features_query, (snapshot_date, limit * 5))  # Берем больше для фильтрации
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            # Конвертируем в список словарей и применяем ML модель
            import sys
            import os
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from services.ml_service import ml_service
            
            high_risk_users = []
            for row in results:
                user_features = {
                    'recency_days': float(row[2]) if row[2] is not None else 999.0,
                    'frequency_90d': int(row[3]) if row[3] is not None else 0,
                    'monetary_180d': float(row[4]) if row[4] is not None else 0.0,
                    'aov_180d': float(row[5]) if row[5] is not None else 0.0,
                    'orders_lifetime': int(row[6]) if row[6] is not None else 0,
                    'revenue_lifetime': float(row[7]) if row[7] is not None else 0.0,
                    'categories_unique': int(row[8]) if row[8] is not None else 0
                }
                
                # Применяем ML модель для предсказания
                try:
                    churn_probability, will_churn, top_reasons = ml_service.predict_churn(user_features)
                    
                    # Добавляем только пользователей с высоким риском
                    if churn_probability >= threshold:
                        high_risk_users.append({
                            'user_id': row[0],
                            'snapshot_date': row[1].isoformat(),
                            'recency_days': row[2],
                            'frequency_90d': row[3],
                            'monetary_180d': float(row[4]),
                            'aov_180d': row[5],
                            'orders_lifetime': row[6],
                            'revenue_lifetime': float(row[7]),
                            'categories_unique': row[8],
                            'prob_churn_next_60d': churn_probability,
                            'will_churn': will_churn,
                            'top_reasons': top_reasons
                        })
                except Exception as e:
                    logger.warning(f"Ошибка предсказания для user_id={row[0]}: {e}")
                    continue
            
            # Сортируем по вероятности оттока (убывание)
            high_risk_users.sort(key=lambda x: x['prob_churn_next_60d'], reverse=True)
            
            logger.info(f"✅ Найдено {len(high_risk_users)} пользователей с высоким риском (≥{threshold})")
            return high_risk_users[:limit]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения пользователей с высоким риском: {e}")
            return []
    
    def get_churn_statistics(self) -> Dict[str, Any]:
        """
        Получение статистики по оттоку клиентов
        
        Returns:
            Словарь со статистикой
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Получаем статистику из ml_training_dataset_churn
            stats_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE target = TRUE) as churn_count,
                COUNT(*) FILTER (WHERE target = FALSE) as retention_count,
                COUNT(DISTINCT user_id) as unique_users,
                MIN(snapshot_date) as earliest_date,
                MAX(snapshot_date) as latest_date
            FROM ml_training_dataset_churn
            """
            
            cursor.execute(stats_query)
            result = cursor.fetchone()
            
            if result:
                total_records = result[0]
                churn_count = result[1]
                retention_count = result[2]
                unique_users = result[3]
                earliest_date = result[4]
                latest_date = result[5]
                
                churn_rate = (churn_count / total_records * 100) if total_records > 0 else 0
                
                statistics = {
                    'total_records': total_records,
                    'churn_count': churn_count,
                    'retention_count': retention_count,
                    'churn_rate_percent': round(churn_rate, 2),
                    'unique_users': unique_users,
                    'date_range': {
                        'earliest': earliest_date.isoformat() if earliest_date else None,
                        'latest': latest_date.isoformat() if latest_date else None
                    }
                }
            else:
                statistics = {
                    'total_records': 0,
                    'churn_count': 0,
                    'retention_count': 0,
                    'churn_rate_percent': 0,
                    'unique_users': 0,
                    'date_range': {'earliest': None, 'latest': None}
                }
            
            cursor.close()
            conn.close()
            
            logger.info(f"✅ Статистика оттока получена: {churn_rate:.1f}% churn rate")
            return statistics
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики оттока: {e}")
            return {
                'total_records': 0,
                'churn_count': 0,
                'retention_count': 0,
                'churn_rate_percent': 0,
                'unique_users': 0,
                'date_range': {'earliest': None, 'latest': None}
            }
    
    def get_user_info(self, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Получение базовой информации о пользователе
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Информация о пользователе или None
        """
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Получаем базовую информацию из таблицы users
            user_query = """
            SELECT 
                user_id,
                email,
                created_at,
                last_login_at
            FROM users
            WHERE user_id = %s
            """
            
            cursor.execute(user_query, (user_id,))
            result = cursor.fetchone()
            
            if result:
                user_info = {
                    'user_id': result[0],
                    'email': result[1],
                    'created_at': result[2].isoformat() if result[2] else None,
                    'last_login_at': result[3].isoformat() if result[3] else None
                }
            else:
                user_info = None
            
            cursor.close()
            conn.close()
            
            return user_info
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения информации о пользователе {user_id}: {e}")
            return None


# Глобальный экземпляр сервиса
churn_service = ChurnService()
