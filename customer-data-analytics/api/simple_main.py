"""
Упрощенная версия FastAPI без ML моделей
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn

# Простые маршруты без сложных зависимостей
from routes.segments import router as segments_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    # Startup
    print("Starting API...")
    yield
    # Shutdown
    print("Shutting down API...")

app = FastAPI(
    title="Customer Data Analytics API",
    description="API для системы аналитики и персонализированных рекомендаций",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(segments_router, tags=["segments"])

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
        "simple_main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
