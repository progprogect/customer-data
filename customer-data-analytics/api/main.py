"""
FastAPI Main Application
Главный файл API сервера
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn

from routes import analytics, recommendations, users, products, segments, ml_predictions, churn_analytics, real_users, simple_real_users, direct_db, price_elasticity, behavior_weekly, anomalies_weekly, ltv_analytics, telegram_db
from middleware.logging import setup_logging
from services.ml_service import ml_service
import sys
import os

# Добавляем путь к shared модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'shared'))
from database.connection import SessionLocal


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    setup_logging()
    
    # Загрузка ML модели
    print("🤖 Загрузка ML модели...")
    model_loaded = ml_service.load_model()
    if model_loaded:
        print(f"✅ ML модель загружена: {ml_service.model_version}")
    else:
        print("⚠️ ML модель не загружена - ML endpoints будут недоступны")
    
    # Загрузка Churn модели
    print("💔 Загрузка Churn модели...")
    churn_model_loaded = ml_service.load_churn_model()
    if churn_model_loaded:
        print(f"✅ Churn модель загружена: {ml_service.churn_model_version}")
    else:
        print("⚠️ Churn модель не загружена - Churn endpoints будут недоступны")
    
    yield
    
    # Shutdown
    print("🔄 Завершение работы API...")
    pass


app = FastAPI(
    title="Customer Data Analytics API",
    description="API для системы аналитики и персонализированных рекомендаций",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],  # Vite frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(recommendations.router, prefix="/api/v1/reco", tags=["recommendations"])
app.include_router(users.router, prefix="/api/v1/users", tags=["users"])
app.include_router(products.router, prefix="/api/v1/products", tags=["products"])
app.include_router(segments.router, tags=["segments"])
app.include_router(ml_predictions.router, prefix="/api/v1/ml", tags=["machine-learning"])
app.include_router(churn_analytics.router, prefix="/api/v1/churn", tags=["churn-analytics"])
app.include_router(real_users.router, prefix="/api/v1", tags=["real-data"])
app.include_router(simple_real_users.router, prefix="/api/v1", tags=["simple-real-data"])
app.include_router(direct_db.router, prefix="/api/v1", tags=["direct-db"])
app.include_router(price_elasticity.router, prefix="/api/v1/price-elasticity", tags=["price-elasticity"])
app.include_router(behavior_weekly.router, tags=["behavior-weekly"])
app.include_router(anomalies_weekly.router, tags=["anomalies-weekly"])
app.include_router(ltv_analytics.router, prefix="/api/v1/ltv", tags=["ltv-analytics"])
app.include_router(telegram_db.router, tags=["telegram-db"])


@app.get("/")
async def root():
    """Корневой endpoint"""
    return {"message": "Customer Data Analytics API", "version": "1.0.0"}


@app.get("/health")
async def health_check():
    """Проверка здоровья API"""
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
