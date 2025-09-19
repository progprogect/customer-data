"""
Price Elasticity API Routes
FastAPI routes для анализа ценовой эластичности

Author: Customer Data Analytics Team
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime, date
from pydantic import BaseModel, Field
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score
import sys
import os

# Добавляем путь к shared модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
from database.connection import get_db
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

router = APIRouter()


class PriceElasticityRequest(BaseModel):
    """Запрос для анализа ценовой эластичности"""
    category: Optional[str] = Field(None, description="Категория товаров")
    start_date: Optional[date] = Field(None, description="Начальная дата")
    end_date: Optional[date] = Field(None, description="Конечная дата")
    min_units_sold: int = Field(10, description="Минимальное количество продаж для анализа")


class PriceElasticityResponse(BaseModel):
    """Ответ с результатами анализа эластичности"""
    category: str
    elasticity_coefficient: float
    r_squared: float
    data_points: int
    price_range: Dict[str, float]
    demand_range: Dict[str, float]
    correlation: float
    interpretation: str
    recommendations: List[str]


class PriceElasticityData(BaseModel):
    """Данные для визуализации эластичности"""
    week_start: date
    avg_price: float
    units_sold: int
    revenue: float


@router.get("/categories", response_model=List[str])
async def get_available_categories(db: Session = Depends(get_db)):
    """Получить список доступных категорий для анализа эластичности"""
    try:
        query = text("""
            SELECT DISTINCT category 
            FROM ml_price_elasticity 
            WHERE units_sold > 0 
            ORDER BY category
        """)
        result = db.execute(query)
        categories = [row[0] for row in result.fetchall()]
        return categories
    except Exception as e:
        logger.error(f"Error getting categories: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения категорий")


@router.get("/data/{category}", response_model=List[PriceElasticityData])
async def get_elasticity_data(
    category: str,
    start_date: Optional[date] = Query(None, description="Начальная дата"),
    end_date: Optional[date] = Query(None, description="Конечная дата"),
    min_units_sold: int = Query(10, description="Минимальное количество продаж"),
    db: Session = Depends(get_db)
):
    """Получить данные для анализа эластичности по категории"""
    try:
        # Строим запрос с фильтрами
        where_conditions = ["category = :category", "units_sold >= :min_units_sold"]
        params = {"category": category, "min_units_sold": min_units_sold}
        
        if start_date:
            where_conditions.append("week_start >= :start_date")
            params["start_date"] = start_date
            
        if end_date:
            where_conditions.append("week_start <= :end_date")
            params["end_date"] = end_date
        
        query = text(f"""
            SELECT week_start, avg_price, units_sold, revenue
            FROM ml_price_elasticity
            WHERE {' AND '.join(where_conditions)}
            ORDER BY week_start
        """)
        
        result = db.execute(query, params)
        data = []
        
        for row in result.fetchall():
            data.append(PriceElasticityData(
                week_start=row[0],
                avg_price=float(row[1]) if row[1] else 0.0,
                units_sold=int(row[2]) if row[2] else 0,
                revenue=float(row[3]) if row[3] else 0.0
            ))
        
        return data
        
    except Exception as e:
        logger.error(f"Error getting elasticity data: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения данных эластичности")


@router.post("/analyze", response_model=PriceElasticityResponse)
async def analyze_price_elasticity(
    request: PriceElasticityRequest,
    db: Session = Depends(get_db)
):
    """Анализ ценовой эластичности для категории"""
    try:
        # Получаем данные
        where_conditions = ["units_sold >= :min_units_sold"]
        params = {"min_units_sold": request.min_units_sold}
        
        if request.category:
            where_conditions.append("category = :category")
            params["category"] = request.category
            
        if request.start_date:
            where_conditions.append("week_start >= :start_date")
            params["start_date"] = request.start_date
            
        if request.end_date:
            where_conditions.append("week_start <= :end_date")
            params["end_date"] = request.end_date
        
        query = text(f"""
            SELECT category, week_start, avg_price, units_sold, revenue
            FROM ml_price_elasticity
            WHERE {' AND '.join(where_conditions)}
            ORDER BY category, week_start
        """)
        
        result = db.execute(query, params)
        df = pd.DataFrame(result.fetchall(), columns=['category', 'week_start', 'avg_price', 'units_sold', 'revenue'])
        
        if df.empty:
            raise HTTPException(status_code=404, detail="Недостаточно данных для анализа")
        
        # Анализируем каждую категорию
        category_results = []
        
        for category in df['category'].unique():
            cat_data = df[df['category'] == category].copy()
            
            if len(cat_data) < 5:  # минимум 5 точек для анализа
                continue
                
            # Подготавливаем данные для логарифмической регрессии (для эластичности)
            prices = cat_data['avg_price'].astype(float).values
            quantities = cat_data['units_sold'].astype(float).values
            
            # Убираем нулевые значения
            mask = (prices > 0) & (quantities > 0)
            prices_clean = prices[mask]
            quantities_clean = quantities[mask]
            
            if len(prices_clean) < 3:
                continue
            
            # Логарифмическая регрессия: log(Q) = a + b*log(P)
            # Эластичность = коэффициент b
            log_prices = np.log(prices_clean)
            log_quantities = np.log(quantities_clean)
            
            X_log = log_prices.reshape(-1, 1)
            y_log = log_quantities
            
            # Строим логарифмическую регрессию
            model = LinearRegression()
            model.fit(X_log, y_log)
            
            # Вычисляем метрики
            y_pred_log = model.predict(X_log)
            r2 = r2_score(y_log, y_pred_log)
            
            # Коэффициент эластичности = коэффициент регрессии в логарифмической модели
            elasticity = model.coef_[0]
            
            # Средние значения для отображения
            avg_price = np.mean(prices_clean)
            avg_quantity = np.mean(quantities_clean)
            
            # Корреляция
            correlation = np.corrcoef(prices_clean, quantities_clean)[0, 1]
            
            category_results.append({
                'category': category,
                'elasticity': elasticity,
                'r_squared': r2,
                'data_points': len(prices_clean),
                'price_range': {
                    'min': float(np.min(prices_clean)),
                    'max': float(np.max(prices_clean)),
                    'avg': float(avg_price)
                },
                'demand_range': {
                    'min': int(np.min(quantities_clean)),
                    'max': int(np.max(quantities_clean)),
                    'avg': float(avg_quantity)
                },
                'correlation': float(correlation)
            })
        
        if not category_results:
            raise HTTPException(status_code=404, detail="Недостаточно данных для анализа эластичности")
        
        # Берем первую категорию (или самую репрезентативную)
        result = category_results[0]
        
        # Интерпретация результатов
        elasticity = result['elasticity']
        if abs(elasticity) < 0.1:
            interpretation = "Неэластичный спрос - цена слабо влияет на спрос"
        elif abs(elasticity) < 1:
            interpretation = "Неэластичный спрос - спрос менее чувствителен к цене"
        elif abs(elasticity) < 2:
            interpretation = "Эластичный спрос - спрос чувствителен к цене"
        else:
            interpretation = "Высокоэластичный спрос - спрос очень чувствителен к цене"
        
        # Рекомендации
        recommendations = []
        if elasticity < -1:
            recommendations.append("Снижение цены может увеличить выручку")
        elif elasticity > -0.5:
            recommendations.append("Повышение цены может увеличить выручку")
        else:
            recommendations.append("Цена оптимальна для текущего спроса")
        
        if result['r_squared'] < 0.3:
            recommendations.append("Низкое качество модели - нужны дополнительные факторы")
        
        return PriceElasticityResponse(
            category=result['category'],
            elasticity_coefficient=round(elasticity, 3),
            r_squared=round(result['r_squared'], 3),
            data_points=result['data_points'],
            price_range=result['price_range'],
            demand_range=result['demand_range'],
            correlation=round(result['correlation'], 3),
            interpretation=interpretation,
            recommendations=recommendations
        )
        
    except Exception as e:
        logger.error(f"Error analyzing price elasticity: {e}")
        raise HTTPException(status_code=500, detail=f"Ошибка анализа эластичности: {str(e)}")


@router.get("/summary")
async def get_elasticity_summary(db: Session = Depends(get_db)):
    """Получить сводку по эластичности для всех категорий"""
    try:
        query = text("""
            SELECT 
                category,
                COUNT(*) as total_weeks,
                COUNT(CASE WHEN units_sold > 0 THEN 1 END) as weeks_with_sales,
                ROUND(AVG(avg_price), 2) as avg_price,
                SUM(units_sold) as total_units_sold,
                ROUND(SUM(revenue), 2) as total_revenue,
                ROUND(AVG(units_sold), 2) as avg_units_per_week
            FROM ml_price_elasticity
            GROUP BY category
            ORDER BY total_units_sold DESC
        """)
        
        result = db.execute(query)
        summary = []
        
        for row in result.fetchall():
            summary.append({
                'category': row[0],
                'total_weeks': row[1],
                'weeks_with_sales': row[2],
                'avg_price': float(row[3]) if row[3] else 0.0,
                'total_units_sold': int(row[4]) if row[4] else 0,
                'total_revenue': float(row[5]) if row[5] else 0.0,
                'avg_units_per_week': float(row[6]) if row[6] else 0.0
            })
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting elasticity summary: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения сводки эластичности")


@router.get("/top-categories", response_model=List[Dict[str, Any]])
async def get_top_categories_by_elasticity(
    limit: int = Query(3, description="Количество категорий в ТОП"),
    db: Session = Depends(get_db)
):
    """Получить ТОП категорий по чувствительности к цене"""
    try:
        # Получаем все категории
        categories_query = text("SELECT DISTINCT category FROM ml_price_elasticity ORDER BY category")
        categories_result = db.execute(categories_query)
        categories = [row[0] for row in categories_result.fetchall()]
        
        top_categories = []
        
        for category in categories:
            # Получаем данные для категории
            data_query = text("""
                SELECT week_start, avg_price, units_sold, revenue
                FROM ml_price_elasticity
                WHERE category = :category AND units_sold > 0 AND avg_price > 0
                ORDER BY week_start
            """)
            data_result = db.execute(data_query, {"category": category})
            data = data_result.fetchall()
            
            if len(data) < 5:  # минимум 5 точек для анализа
                continue
                
            # Подготавливаем данные для логарифмической регрессии
            prices = np.array([float(row[1]) for row in data])
            quantities = np.array([float(row[2]) for row in data])
            
            # Логарифмическая регрессия: log(Q) = a + b*log(P)
            log_prices = np.log(prices)
            log_quantities = np.log(quantities)
            
            X_log = log_prices.reshape(-1, 1)
            y_log = log_quantities
            
            model = LinearRegression()
            model.fit(X_log, y_log)
            
            # Коэффициент эластичности = коэффициент регрессии в логарифмической модели
            elasticity = model.coef_[0]
            
            # Средние значения для отображения
            avg_price = np.mean(prices)
            avg_quantity = np.mean(quantities)
            
            # R-squared
            y_pred_log = model.predict(X_log)
            r2 = r2_score(y_log, y_pred_log)
            
            # Интерпретация
            abs_elasticity = abs(elasticity)
            if abs_elasticity > 1.5:
                sensitivity_level = "Высокая"
            elif abs_elasticity > 1.0:
                sensitivity_level = "Средняя"
            else:
                sensitivity_level = "Низкая"
            
            top_categories.append({
                "category": category,
                "elasticity_coefficient": round(elasticity, 3),
                "r_squared": round(r2, 3),
                "data_points": len(data),
                "sensitivity_level": sensitivity_level
            })
        
        # Сортируем по модулю эластичности (по убыванию)
        top_categories.sort(key=lambda x: abs(x["elasticity_coefficient"]), reverse=True)
        
        return top_categories[:limit]
        
    except Exception as e:
        logger.error(f"Error getting top categories: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения ТОП категорий")


@router.get("/scenarios/{category}", response_model=Dict[str, Any])
async def get_price_scenarios(
    category: str,
    db: Session = Depends(get_db)
):
    """Получить сценарии изменения цен для категории"""
    try:
        # Получаем данные для категории
        data_query = text("""
            SELECT week_start, avg_price, units_sold, revenue
            FROM ml_price_elasticity
            WHERE category = :category AND units_sold > 0 AND avg_price > 0
            ORDER BY week_start
        """)
        data_result = db.execute(data_query, {"category": category})
        data = data_result.fetchall()
        
        if len(data) < 5:
            raise HTTPException(status_code=404, detail="Недостаточно данных для анализа")
            
        # Подготавливаем данные для логарифмической регрессии
        prices = np.array([float(row[1]) for row in data])
        quantities = np.array([float(row[2]) for row in data])
        
        # Логарифмическая регрессия: log(Q) = a + b*log(P)
        log_prices = np.log(prices)
        log_quantities = np.log(quantities)
        
        X_log = log_prices.reshape(-1, 1)
        y_log = log_quantities
        
        model = LinearRegression()
        model.fit(X_log, y_log)
        
        # Коэффициент эластичности = коэффициент регрессии в логарифмической модели
        elasticity = model.coef_[0]
        
        # Средние значения для отображения
        avg_price = np.mean(prices)
        avg_quantity = np.mean(quantities)
        
        # Рассчитываем сценарии
        scenarios = {}
        price_changes = [-15, -10, -5, 5, 10, 15]  # Проценты изменения цены
        
        for price_change in price_changes:
            # Формула: ΔQ% ≈ E × ΔP%
            demand_change = elasticity * price_change
            scenarios[f"price_{price_change:+d}%"] = round(demand_change, 1)
        
        # Интерпретация
        abs_elasticity = abs(elasticity)
        if abs_elasticity > 1.5:
            interpretation = "Высокоэластичный спрос - спрос очень чувствителен к цене"
            recommendation = "Количество продаж сильно меняется в зависимости от цены - стоит аккуратно менять цены"
        elif abs_elasticity > 1.0:
            interpretation = "Эластичный спрос - спрос чувствителен к цене"
            recommendation = "Спрос реагирует на изменения цены - осторожно с корректировками"
        elif abs_elasticity > 0.5:
            interpretation = "Умеренно эластичный спрос - цена влияет на спрос"
            recommendation = "Спрос частично реагирует на цену - можно осторожно корректировать"
        else:
            interpretation = "Неэластичный спрос - цена слабо влияет на спрос"
            recommendation = "Спрос мало зависит от цены - можно корректировать цены"
        
        return {
            "category": category,
            "elasticity_coefficient": round(elasticity, 3),
            "scenarios": scenarios,
            "interpretation": interpretation,
            "recommendation": recommendation,
            "sensitivity_level": "Высокая" if abs_elasticity > 1.5 else "Средняя" if abs_elasticity > 1.0 else "Низкая"
        }
        
    except Exception as e:
        logger.error(f"Error getting price scenarios: {e}")
        raise HTTPException(status_code=500, detail="Ошибка получения сценариев цен")


@router.get("/health")
async def health_check():
    """Проверка здоровья API эластичности"""
    return {"status": "healthy", "service": "price_elasticity"}
