#!/usr/bin/env python3
"""
Debug API
Детальная проверка всех API endpoints и их данных
"""

import asyncio
import logging
import sys
import os
import json

# Добавляем путь к модулям
sys.path.append(os.path.dirname(__file__))

from utils.api_client import api_client, APIClientError
from utils.formatters import *
from utils.config import get_config

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def debug_ltv_summary():
    """Детальная проверка LTV Summary"""
    logger.info("🔍 === DEBUG: LTV Summary ===")
    
    try:
        data = await api_client.get_ltv_summary(1)
        logger.info(f"📊 Raw LTV Summary data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        if data and 'summary' in data:
            logger.info(f"📈 Summary items count: {len(data['summary'])}")
            for i, item in enumerate(data['summary']):
                logger.info(f"  Item {i}: {item}")
        else:
            logger.warning("⚠️ No 'summary' key in LTV data")
            
    except Exception as e:
        logger.error(f"❌ LTV Summary error: {e}")


async def debug_segments_distribution():
    """Детальная проверка Segments Distribution"""
    logger.info("🔍 === DEBUG: Segments Distribution ===")
    
    try:
        data = await api_client.get_segments_distribution(1)
        logger.info(f"👥 Raw Segments data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        if data and 'segments' in data:
            logger.info(f"📊 Segments count: {len(data['segments'])}")
            for i, segment in enumerate(data['segments']):
                logger.info(f"  Segment {i}: {segment}")
                
            # Тестируем форматирование
            formatted = format_segments_table(data['segments'])
            logger.info(f"📝 Formatted segments:")
            logger.info(formatted)
        else:
            logger.warning("⚠️ No 'segments' key in segments data")
            
    except Exception as e:
        logger.error(f"❌ Segments error: {e}")


async def debug_anomalies_weekly():
    """Детальная проверка Anomalies Weekly"""
    logger.info("🔍 === DEBUG: Anomalies Weekly ===")
    
    try:
        data = await api_client.get_anomalies_weekly(1, limit=3)
        logger.info(f"🚨 Raw Anomalies data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        if data and 'anomalies' in data:
            logger.info(f"📊 Anomalies count: {len(data['anomalies'])}")
            for i, anomaly in enumerate(data['anomalies']):
                logger.info(f"  Anomaly {i}: {anomaly}")
                
            # Тестируем форматирование
            formatted = format_anomalies_table(data['anomalies'])
            logger.info(f"📝 Formatted anomalies:")
            logger.info(formatted)
        else:
            logger.warning("⚠️ No 'anomalies' key in anomalies data")
            
    except Exception as e:
        logger.error(f"❌ Anomalies error: {e}")


async def debug_user_ltv():
    """Детальная проверка User LTV"""
    logger.info("🔍 === DEBUG: User LTV ===")
    
    try:
        data = await api_client.get_user_ltv(1, 1)
        logger.info(f"👤 Raw User LTV data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        # Проверяем структуру
        if isinstance(data, dict):
            logger.info(f"📊 User LTV keys: {list(data.keys())}")
            if 'ltv' in data:
                logger.info(f"💰 LTV value: {data['ltv']}")
            else:
                logger.warning("⚠️ No 'ltv' key in user data")
        else:
            logger.warning(f"⚠️ Unexpected data type: {type(data)}")
            
    except Exception as e:
        logger.error(f"❌ User LTV error: {e}")


async def debug_user_events():
    """Детальная проверка User Events"""
    logger.info("🔍 === DEBUG: User Events ===")
    
    try:
        data = await api_client.get_user_events(1, 1, limit=5)
        logger.info(f"📅 Raw User Events data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        # Проверяем структуру
        if isinstance(data, list):
            logger.info(f"📊 Events count: {len(data)}")
            for i, event in enumerate(data):
                logger.info(f"  Event {i}: {event}")
        elif isinstance(data, dict):
            logger.info(f"📊 Events dict keys: {list(data.keys())}")
            if 'events' in data:
                logger.info(f"📊 Events count: {len(data['events'])}")
                for i, event in enumerate(data['events']):
                    logger.info(f"  Event {i}: {event}")
        else:
            logger.warning(f"⚠️ Unexpected data type: {type(data)}")
            
    except Exception as e:
        logger.error(f"❌ User Events error: {e}")


async def debug_user_recommendations():
    """Детальная проверка User Recommendations"""
    logger.info("🔍 === DEBUG: User Recommendations ===")
    
    try:
        data = await api_client.get_user_recommendations(1, 1, k=3)
        logger.info(f"🎯 Raw User Recommendations data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        # Проверяем структуру
        if isinstance(data, list):
            logger.info(f"📊 Recommendations count: {len(data)}")
            for i, rec in enumerate(data):
                logger.info(f"  Recommendation {i}: {rec}")
                
            # Тестируем форматирование
            formatted = format_recommendations_list(data)
            logger.info(f"📝 Formatted recommendations:")
            logger.info(formatted)
        elif isinstance(data, dict):
            logger.info(f"📊 Recommendations dict keys: {list(data.keys())}")
            if 'recommendations' in data:
                logger.info(f"📊 Recommendations count: {len(data['recommendations'])}")
                for i, rec in enumerate(data['recommendations']):
                    logger.info(f"  Recommendation {i}: {rec}")
                    
                # Тестируем форматирование
                formatted = format_recommendations_list(data['recommendations'])
                logger.info(f"📝 Formatted recommendations:")
                logger.info(formatted)
        else:
            logger.warning(f"⚠️ Unexpected data type: {type(data)}")
            
    except Exception as e:
        logger.error(f"❌ User Recommendations error: {e}")


async def debug_dashboard_data():
    """Детальная проверка Dashboard Data"""
    logger.info("🔍 === DEBUG: Dashboard Data ===")
    
    try:
        data = await api_client.get_dashboard_data(1)
        logger.info(f"📈 Raw Dashboard data:")
        logger.info(json.dumps(data, indent=2, ensure_ascii=False))
        
        if isinstance(data, dict):
            logger.info(f"📊 Dashboard keys: {list(data.keys())}")
            for key, value in data.items():
                logger.info(f"  {key}: {value}")
        else:
            logger.warning(f"⚠️ Unexpected data type: {type(data)}")
            
    except Exception as e:
        logger.error(f"❌ Dashboard error: {e}")


async def test_multiple_users():
    """Тестирование нескольких пользователей"""
    logger.info("🔍 === DEBUG: Multiple Users ===")
    
    test_users = [1, 2, 3, 100, 999]
    
    for user_id in test_users:
        logger.info(f"👤 Testing user {user_id}:")
        
        try:
            # LTV
            ltv_data = await api_client.get_user_ltv(1, user_id)
            ltv_value = ltv_data.get('ltv', 0) if ltv_data else 0
            
            # Events
            events_data = await api_client.get_user_events(1, user_id, limit=3)
            events_count = len(events_data) if isinstance(events_data, list) else len(events_data.get('events', [])) if isinstance(events_data, dict) else 0
            
            # Recommendations
            rec_data = await api_client.get_user_recommendations(1, user_id, k=3)
            rec_count = len(rec_data) if isinstance(rec_data, list) else len(rec_data.get('recommendations', [])) if isinstance(rec_data, dict) else 0
            
            logger.info(f"  ✅ LTV: ${ltv_value}, Events: {events_count}, Recs: {rec_count}")
            
        except Exception as e:
            logger.warning(f"  ⚠️ User {user_id} error: {e}")


async def debug_user_summary_flow():
    """Тестирование полного flow пользователя"""
    logger.info("🔍 === DEBUG: User Summary Flow ===")
    
    try:
        # Имитируем полный flow из user_handler.py
        user_data = {
            "user_id": 1,
            "segment": "неизвестно",
            "ltv_12m": 0,
            "last_order_days": None,
            "prob_purchase": 0,
            "prob_churn": 0,
            "recommendations": [],
            "anomalies": []
        }
        
        # Получаем LTV данные
        ltv_data = await api_client.get_user_ltv(1, 1)
        if ltv_data:
            # Проверяем разные структуры данных
            if 'ltv' in ltv_data:
                if isinstance(ltv_data['ltv'], dict):
                    # Структура: {"ltv": {"revenue_12m": 100, "days_since_last_order": 5}}
                    user_data["ltv_12m"] = ltv_data['ltv'].get('revenue_12m', 0)
                    user_data["last_order_days"] = ltv_data['ltv'].get('days_since_last_order', None)
                else:
                    # Структура: {"ltv": 100.0}
                    user_data["ltv_12m"] = float(ltv_data['ltv'])
                    user_data["last_order_days"] = None
        
        # Получаем рекомендации
        rec_data = await api_client.get_user_recommendations(1, 1, k=3)
        if rec_data:
            if isinstance(rec_data, list):
                user_data["recommendations"] = rec_data[:3]
            elif isinstance(rec_data, dict) and 'recommendations' in rec_data:
                user_data["recommendations"] = rec_data['recommendations'][:3]
            elif isinstance(rec_data, dict):
                user_data["recommendations"] = [rec_data]
        
        # Получаем события для анализа сегмента
        events_data = await api_client.get_user_events(1, 1, limit=5)
        if events_data:
            if isinstance(events_data, list):
                user_data["segment"] = analyze_user_segment(events_data)
            elif isinstance(events_data, dict) and 'events' in events_data:
                user_data["segment"] = analyze_user_segment(events_data['events'])
        
        logger.info(f"📊 Final user_data: {user_data}")
        
        # Форматируем сводку
        formatted_summary = format_user_summary(user_data)
        logger.info(f"📝 Formatted user summary:")
        logger.info(formatted_summary)
        
    except Exception as e:
        logger.error(f"❌ User summary flow error: {e}")


def analyze_user_segment(events: list) -> str:
    """Простой анализ сегмента пользователя на основе событий"""
    if not events:
        return "Новый"
    
    # Подсчитываем типы событий
    event_types = {}
    for event in events:
        event_type = event.get('event_type', '')
        event_types[event_type] = event_types.get(event_type, 0) + 1
    
    # Простая логика определения сегмента
    purchase_events = event_types.get('purchase', 0)
    view_events = event_types.get('view_product', 0)
    
    if purchase_events >= 10:
        return "VIP"
    elif purchase_events >= 5:
        return "Высокий"
    elif purchase_events >= 2:
        return "Средний"
    elif purchase_events >= 1:
        return "Низкий"
    elif view_events >= 10:
        return "Заинтересованный"
    else:
        return "Новый"


async def main():
    """Главная функция отладки"""
    logger.info("🚀 Запуск детальной отладки API")
    
    try:
        # 1. Проверяем все основные endpoints
        await debug_ltv_summary()
        await debug_segments_distribution()
        await debug_anomalies_weekly()
        await debug_user_ltv()
        await debug_user_events()
        await debug_user_recommendations()
        await debug_dashboard_data()
        
        # 2. Тестируем несколько пользователей
        await test_multiple_users()
        
        # 3. Тестируем полный flow
        await debug_user_summary_flow()
        
        logger.info("🎉 Детальная отладка завершена!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отладки: {e}")


if __name__ == "__main__":
    asyncio.run(main())
