#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI main application
Author: Customer Data Analytics Team
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
import uvicorn

from routes import segments
from services.database import init_connection_pool, close_connection_pool

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="Customer Data Analytics API",
    description="API for user segmentation and analytics",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В продакшене указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров
app.include_router(segments.router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    logger.info("Starting Customer Data Analytics API")
    try:
        init_connection_pool()
        logger.info("API startup completed successfully")
    except Exception as e:
        logger.error(f"Failed to start API: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке"""
    logger.info("Shutting down Customer Data Analytics API")
    try:
        close_connection_pool()
        logger.info("API shutdown completed successfully")
    except Exception as e:
        logger.error(f"Error during API shutdown: {e}")

@app.get("/")
async def root():
    """Корневой эндпоинт"""
    return {
        "message": "Customer Data Analytics API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """Проверка здоровья API"""
    try:
        # Проверка подключения к БД
        from services.database import get_db_connection, return_db_connection
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return_db_connection(conn)
        
        return {
            "status": "healthy",
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

@app.get("/api/v1/status")
async def api_status():
    """Статус API"""
    return {
        "api": "running",
        "version": "1.0.0",
        "endpoints": {
            "segments": {
                "distribution": "/api/v1/segments/distribution",
                "dynamics": "/api/v1/segments/dynamics",
                "migration": "/api/v1/segments/migration",
                "meta": "/api/v1/segments/meta"
            }
        }
    }

@app.get("/api/v1/analytics/dashboard")
async def get_dashboard_data():
    """Получение данных для дашборда"""
    try:
        from services.database import get_db_connection, return_db_connection
        
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Получаем базовые метрики
            cur.execute("""
                SELECT 
                    COUNT(DISTINCT user_id) as total_users,
                    COUNT(*) as total_orders,
                    COALESCE(SUM(total_amount), 0) as total_revenue,
                    COUNT(DISTINCT CASE WHEN created_at >= CURRENT_DATE - INTERVAL '30 days' THEN user_id END) as active_users
                FROM orders 
                WHERE status IN ('paid', 'shipped', 'completed')
            """)
            metrics = cur.fetchone()
            
            # Получаем топ продукты
            cur.execute("""
                SELECT p.title, COUNT(*) as orders_count
                FROM order_items oi
                JOIN products p ON p.product_id = oi.product_id
                JOIN orders o ON o.order_id = oi.order_id
                WHERE o.status IN ('paid', 'shipped', 'completed')
                GROUP BY p.product_id, p.title
                ORDER BY orders_count DESC
                LIMIT 5
            """)
            top_products = cur.fetchall()
            
        return_db_connection(conn)
        
        # Вычисляем дополнительные метрики
        conversion_rate = 0.0
        avg_order_value = 0.0
        if metrics[1] > 0:  # total_orders
            conversion_rate = (metrics[1] / metrics[0]) * 100 if metrics[0] > 0 else 0
            avg_order_value = metrics[2] / metrics[1] if metrics[1] > 0 else 0
        
        return {
            "total_users": metrics[0] or 0,
            "total_orders": metrics[1] or 0,
            "total_revenue": float(metrics[2] or 0),
            "active_users": metrics[3] or 0,
            "conversion_rate": round(conversion_rate, 2),
            "avg_order_value": round(avg_order_value, 2),
            "top_products": [
                {"name": product[0], "orders": product[1]} 
                for product in top_products
            ],
            "user_growth": [],  # Пока пустой массив
            "revenue_trend": []  # Пока пустой массив
        }
        
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}")
        raise HTTPException(status_code=500, detail="Failed to get dashboard data")

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
