"""
Segments Service
Сервис для работы с данными сегментации пользователей
"""

from typing import Dict, List, Any, Optional
from datetime import datetime, date, timedelta
from sqlalchemy import text
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from database.connection import SessionLocal
import logging

logger = logging.getLogger(__name__)


class SegmentsService:
    """Сервис для работы с сегментами пользователей"""
    
    def __init__(self):
        self.cluster_names = {
            0: "Новые/Неактивные клиенты",
            1: "Обычные клиенты", 
            2: "VIP / Лояльные клиенты"
        }
        self.cluster_descriptions = {
            0: "Пользователи с низкой активностью",
            1: "Среднеактивные пользователи",
            2: "Высокоактивные и лояльные пользователи"
        }
    
    def get_meta(self) -> List[Dict[str, Any]]:
        """Получить метаданные кластеров"""
        return [
            {
                "id": cluster_id,
                "name": name,
                "description": self.cluster_descriptions[cluster_id]
            }
            for cluster_id, name in self.cluster_names.items()
        ]
    
    def get_distribution(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """Получить распределение сегментов на дату"""
        db = SessionLocal()
        try:
            # Если дата не указана, берем последнюю доступную
            if target_date is None:
                result = db.execute(text("""
                    SELECT MAX(snapshot_date) as latest_date 
                    FROM user_segments_kmeans
                """)).fetchone()
                target_date = result.latest_date.strftime('%Y-%m-%d')
            
            # Получаем распределение на указанную дату
            result = db.execute(text("""
                SELECT 
                    cluster_id,
                    COUNT(*) as users_count
                FROM user_segments_kmeans 
                WHERE snapshot_date = :date
                GROUP BY cluster_id
                ORDER BY cluster_id
            """), {"date": target_date}).fetchall()
            
            if not result:
                return {
                    "date": target_date,
                    "timezone": "Europe/Warsaw",
                    "available": False,
                    "total_users": 0,
                    "segments": [],
                    "note": f"Данные за {target_date} недоступны"
                }
            
            total_users = sum(row.users_count for row in result)
            segments = [
                {
                    "cluster_id": row.cluster_id,
                    "users_count": row.users_count,
                    "share": row.users_count / total_users if total_users > 0 else 0
                }
                for row in result
            ]
            
            # Получаем последнюю доступную дату для last_available_date
            latest_result = db.execute(text("""
                SELECT MAX(snapshot_date) as latest_date 
                FROM user_segments_kmeans
            """)).fetchone()
            
            return {
                "date": target_date,
                "timezone": "Europe/Warsaw", 
                "available": True,
                "total_users": total_users,
                "segments": segments,
                "last_available_date": latest_result.latest_date.strftime('%Y-%m-%d')
            }
            
        except Exception as e:
            logger.error(f"Error getting distribution: {e}")
            return {
                "date": target_date or "unknown",
                "timezone": "Europe/Warsaw",
                "available": False,
                "total_users": 0,
                "segments": [],
                "note": f"Ошибка загрузки данных: {str(e)}"
            }
        finally:
            db.close()
    
    def get_dynamics(self, from_date: str, to_date: str, granularity: str = "day") -> Dict[str, Any]:
        """Получить динамику сегментов за период"""
        db = SessionLocal()
        try:
            # Базовый запрос для получения данных по дням
            result = db.execute(text("""
                SELECT 
                    snapshot_date,
                    cluster_id,
                    COUNT(*) as users_count
                FROM user_segments_kmeans 
                WHERE snapshot_date BETWEEN :from_date AND :to_date
                GROUP BY snapshot_date, cluster_id
                ORDER BY snapshot_date, cluster_id
            """), {"from_date": from_date, "to_date": to_date}).fetchall()
            
            if not result:
                return {
                    "from_date": from_date,
                    "to_date": to_date, 
                    "granularity": granularity,
                    "timezone": "Europe/Warsaw",
                    "available": False,
                    "series": [],
                    "note": f"Данные за период {from_date} - {to_date} недоступны"
                }
            
            # Группируем данные по кластерам и применяем гранулярность
            from datetime import datetime, timedelta
            import calendar
            
            series_data = {}
            
            # Группируем по гранулярности на стороне Python
            grouped_data = {}
            for row in result:
                date_obj = row.snapshot_date
                cluster_id = row.cluster_id
                
                # Определяем ключ группировки в зависимости от гранулярности
                if granularity == "week":
                    # Начало недели (понедельник)
                    start_of_week = date_obj - timedelta(days=date_obj.weekday())
                    group_key = start_of_week.strftime('%Y-%m-%d')
                elif granularity == "month":
                    # Начало месяца
                    group_key = date_obj.strftime('%Y-%m-01')
                else:  # day
                    group_key = date_obj.strftime('%Y-%m-%d')
                
                if (cluster_id, group_key) not in grouped_data:
                    grouped_data[(cluster_id, group_key)] = 0
                grouped_data[(cluster_id, group_key)] += row.users_count
            
            # Преобразуем в нужный формат
            for (cluster_id, date_key), count in grouped_data.items():
                if cluster_id not in series_data:
                    series_data[cluster_id] = []
                
                series_data[cluster_id].append({
                    "date": date_key,
                    "count": count
                })
            
            # Сортируем точки по датам
            for cluster_id in series_data:
                series_data[cluster_id].sort(key=lambda x: x["date"])
            
            # Формируем series в нужном формате
            series = [
                {
                    "cluster_id": cluster_id,
                    "points": points
                }
                for cluster_id, points in series_data.items()
            ]
            
            return {
                "from_date": from_date,
                "to_date": to_date,
                "granularity": granularity,
                "timezone": "Europe/Warsaw",
                "available": True,
                "series": series
            }
            
        except Exception as e:
            logger.error(f"Error getting dynamics: {e}")
            return {
                "from_date": from_date,
                "to_date": to_date,
                "granularity": granularity, 
                "timezone": "Europe/Warsaw",
                "available": False,
                "series": [],
                "note": f"Ошибка загрузки данных: {str(e)}"
            }
        finally:
            db.close()
    
    def get_migration(self, target_date: str) -> Dict[str, Any]:
        """Получить матрицу переходов между сегментами (вчера -> сегодня)"""
        db = SessionLocal()
        try:
            # Вычисляем вчерашнюю дату
            target_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
            yesterday = target_dt - timedelta(days=1)
            yesterday_str = yesterday.strftime('%Y-%m-%d')
            
            # Получаем переходы пользователей между кластерами
            result = db.execute(text("""
                WITH yesterday_clusters AS (
                    SELECT user_id, cluster_id as from_cluster
                    FROM user_segments_kmeans 
                    WHERE snapshot_date = :yesterday
                ),
                today_clusters AS (
                    SELECT user_id, cluster_id as to_cluster
                    FROM user_segments_kmeans 
                    WHERE snapshot_date = :today
                )
                SELECT 
                    y.from_cluster,
                    t.to_cluster,
                    COUNT(*) as transition_count
                FROM yesterday_clusters y
                INNER JOIN today_clusters t ON y.user_id = t.user_id
                GROUP BY y.from_cluster, t.to_cluster
                ORDER BY y.from_cluster, t.to_cluster
            """), {"yesterday": yesterday_str, "today": target_date}).fetchall()
            
            if not result:
                return {
                    "date": target_date,
                    "timezone": "Europe/Warsaw",
                    "available": False,
                    "matrix": [],
                    "note": f"Данные о переходах за {target_date} недоступны (нет данных за {yesterday_str})"
                }
            
            # Формируем матрицу переходов
            matrix = [
                {
                    "from_cluster": row.from_cluster,
                    "to_cluster": row.to_cluster,
                    "count": row.transition_count
                }
                for row in result
            ]
            
            return {
                "date": target_date,
                "timezone": "Europe/Warsaw",
                "available": True,
                "matrix": matrix
            }
            
        except Exception as e:
            logger.error(f"Error getting migration: {e}")
            return {
                "date": target_date,
                "timezone": "Europe/Warsaw", 
                "available": False,
                "matrix": [],
                "note": f"Ошибка загрузки данных: {str(e)}"
            }
        finally:
            db.close()
