"""
Минимальный API только с segments endpoints
"""

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI(
    title="Customer Data Analytics API",
    description="API для системы аналитики и персонализированных рекомендаций",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    """Корневой endpoint"""
    return {"message": "Customer Data Analytics API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Проверка здоровья API"""
    return {"status": "healthy"}

# Segments endpoints
@app.get("/api/v1/segments/meta")
async def get_segments_meta():
    """Метаданные кластеров"""
    return {
        "clusters": [
            {"id": 0, "name": "Спящие клиенты", "description": "Неактивные пользователи"},
            {"id": 1, "name": "VIP клиенты", "description": "Высокоценные покупатели"},
            {"id": 2, "name": "Новые клиенты", "description": "Недавно зарегистрированные"}
        ]
    }

@app.get("/api/v1/segments/distribution")
async def get_segments_distribution(date: str = None):
    """Распределение сегментов"""
    return {
        "date": date or "2025-09-18",
        "timezone": "Europe/Warsaw",
        "available": True,
        "total_users": 1500,
        "segments": [
            {"cluster_id": 0, "users_count": 600, "share": 0.4},
            {"cluster_id": 1, "users_count": 450, "share": 0.3},
            {"cluster_id": 2, "users_count": 450, "share": 0.3}
        ]
    }

@app.get("/api/v1/segments/dynamics")
async def get_segments_dynamics(
    from_date: str = Query(..., alias="from"),
    to_date: str = Query(..., alias="to"),
    granularity: str = "day",
):
    """Динамика сегментов"""
    return {
        "from": from_date,
        "to": to_date,
        "granularity": granularity,
        "timezone": "Europe/Warsaw",
        "available": True,
        "series": [
            {
                "cluster_id": 0,
                "points": [
                    {"date": "2025-09-15", "users_count": 580, "share": 0.39},
                    {"date": "2025-09-16", "users_count": 590, "share": 0.40},
                    {"date": "2025-09-17", "users_count": 595, "share": 0.40},
                    {"date": "2025-09-18", "users_count": 600, "share": 0.40}
                ]
            },
            {
                "cluster_id": 1,
                "points": [
                    {"date": "2025-09-15", "users_count": 440, "share": 0.29},
                    {"date": "2025-09-16", "users_count": 445, "share": 0.30},
                    {"date": "2025-09-17", "users_count": 447, "share": 0.30},
                    {"date": "2025-09-18", "users_count": 450, "share": 0.30}
                ]
            },
            {
                "cluster_id": 2,
                "points": [
                    {"date": "2025-09-15", "users_count": 480, "share": 0.32},
                    {"date": "2025-09-16", "users_count": 470, "share": 0.32},
                    {"date": "2025-09-17", "users_count": 465, "share": 0.31},
                    {"date": "2025-09-18", "users_count": 450, "share": 0.30}
                ]
            }
        ]
    }

if __name__ == "__main__":
    uvicorn.run(
        "minimal_api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )
