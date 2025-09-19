import logging
from typing import List, Dict, Any, Tuple
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import text
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

logger = logging.getLogger(__name__)

class PriceElasticityService:
    async def get_available_categories(self) -> List[str]:
        """Получает список категорий, для которых есть данные по эластичности."""
        # В реальном приложении это может быть запрос к БД
        # или кэшированный список
        return ["Accessories", "Laptops", "Smartphones", "Sports", "Toys"]

    async def get_elasticity_data(self, db: Session, category: str) -> List[Dict[str, Any]]:
        """Получает агрегированные данные цена-спрос для заданной категории."""
        try:
            query = text(f"""
                SELECT
                    category,
                    week_start,
                    avg_price,
                    units_sold,
                    revenue,
                    price_changes_count
                FROM ml_price_elasticity
                WHERE category = :category
                ORDER BY week_start;
            """)
            result = db.execute(query, {"category": category}).fetchall()
            return [row._asdict() for row in result]
        except Exception as e:
            logger.error(f"Error fetching elasticity data for category {category}: {e}")
            raise

    def calculate_elasticity_scenarios(self, elasticity_coefficient: float) -> Dict[str, float]:
        """Рассчитывает сценарии изменения продаж для стандартных изменений цен."""
        scenarios = {}
        price_changes = [-15, -10, -5, 5, 10, 15]  # Проценты изменения цены
        
        for price_change in price_changes:
            # Формула: ΔQ% ≈ E × ΔP%
            demand_change = elasticity_coefficient * price_change
            scenarios[f"price_{price_change:+d}%"] = round(demand_change, 1)
            
        return scenarios

    def interpret_elasticity(self, elasticity_coefficient: float) -> Dict[str, str]:
        """Интерпретирует коэффициент эластичности с бизнес-контекстом."""
        abs_elasticity = abs(elasticity_coefficient)
        
        if abs_elasticity > 1.5:
            interpretation = "Высокоэластичный спрос - спрос очень чувствителен к цене"
            recommendation = "Использовать акции и скидки, так как спрос чувствителен к цене"
        elif abs_elasticity > 1.0:
            interpretation = "Эластичный спрос - спрос чувствителен к цене"
            recommendation = "Осторожно с повышением цен, рассмотреть промо-акции"
        elif abs_elasticity > 0.5:
            interpretation = "Умеренно эластичный спрос - цена влияет на спрос"
            recommendation = "Можно корректировать цены, но с осторожностью"
        else:
            interpretation = "Неэластичный спрос - цена слабо влияет на спрос"
            recommendation = "Можно повышать цены без сильного падения спроса"
            
        return {
            "interpretation": interpretation,
            "recommendation": recommendation,
            "sensitivity_level": "Высокая" if abs_elasticity > 1.5 else "Средняя" if abs_elasticity > 1.0 else "Низкая"
        }

    async def get_top_categories_by_elasticity(self, db: Session, limit: int = 3) -> List[Dict[str, Any]]:
        """Получает ТОП категорий по чувствительности к цене (по модулю эластичности)."""
        try:
            # Получаем все категории с их данными
            categories = await self.get_available_categories()
            top_categories = []
            
            for category in categories:
                data = await self.get_elasticity_data(db, category)
                if len(data) < 5:  # минимум 5 точек для анализа
                    continue
                    
                # Рассчитываем эластичность для категории
                elasticity_result = await self.calculate_category_elasticity(data)
                if elasticity_result:
                    top_categories.append({
                        "category": category,
                        "elasticity_coefficient": elasticity_result["elasticity_coefficient"],
                        "r_squared": elasticity_result["r_squared"],
                        "data_points": elasticity_result["data_points"],
                        "sensitivity_level": self.interpret_elasticity(elasticity_result["elasticity_coefficient"])["sensitivity_level"]
                    })
            
            # Сортируем по модулю эластичности (по убыванию)
            top_categories.sort(key=lambda x: abs(x["elasticity_coefficient"]), reverse=True)
            
            return top_categories[:limit]
            
        except Exception as e:
            logger.error(f"Error getting top categories by elasticity: {e}")
            raise

    async def calculate_category_elasticity(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Рассчитывает эластичность для конкретной категории."""
        try:
            if len(data) < 5:
                return None
                
            # Подготавливаем данные
            prices = np.array([float(d['avg_price']) for d in data if d['avg_price'] > 0])
            quantities = np.array([float(d['units_sold']) for d in data if d['units_sold'] > 0])
            
            if len(prices) < 3 or len(quantities) < 3:
                return None
                
            # Убираем нулевые значения
            mask = (prices > 0) & (quantities > 0)
            X = prices[mask].reshape(-1, 1)
            y = quantities[mask]
            
            if len(X) < 3:
                return None
                
            # Строим регрессию
            model = LinearRegression()
            model.fit(X, y)
            
            # Рассчитываем эластичность (коэффициент регрессии)
            elasticity_coefficient = model.coef_[0]
            r_squared = r2_score(y, model.predict(X))
            
            return {
                "elasticity_coefficient": round(elasticity_coefficient, 3),
                "r_squared": round(r_squared, 3),
                "data_points": len(X)
            }
            
        except Exception as e:
            logger.error(f"Error calculating elasticity for category: {e}")
            return None
