"""
Price Elasticity Model
Модель анализа чувствительности к цене
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from .base_model import BaseMLModel


class PriceElasticityModel(BaseMLModel):
    """Модель анализа ценовой эластичности"""
    
    def __init__(self):
        super().__init__("price_elasticity")
        self.scaler = StandardScaler()
        self.elasticity_coefficients = None
        
    def train(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Dict[str, float]:
        """Обучение модели ценовой эластичности"""
        if y is None:
            raise ValueError("Для обучения модели эластичности необходим целевой признак")
        
        # Подготовка данных
        if 'price' not in X.columns:
            raise ValueError("В данных должен быть столбец 'price'")
        
        # Логарифмическое преобразование для расчета эластичности
        X_log = X.copy()
        X_log['log_price'] = np.log(X_log['price'])
        
        # Нормализация признаков
        feature_columns = [col for col in X_log.columns if col != 'price']
        X_scaled = self.scaler.fit_transform(X_log[feature_columns])
        
        # Обучение модели
        self.model = LinearRegression()
        self.model.fit(X_scaled, y)
        
        # Расчет коэффициентов эластичности
        self.elasticity_coefficients = pd.DataFrame({
            'feature': feature_columns,
            'coefficient': self.model.coef_,
            'elasticity': self.model.coef_ * (X_log[feature_columns].mean() / y.mean())
        })
        
        # Метрики
        y_pred = self.model.predict(X_scaled)
        r2_score = self.model.score(X_scaled, y)
        
        self.metrics = {
            "r2_score": r2_score,
            "n_features": len(feature_columns),
            "mean_elasticity": self.elasticity_coefficients['elasticity'].mean(),
            "price_elasticity": self.elasticity_coefficients[
                self.elasticity_coefficients['feature'] == 'log_price'
            ]['elasticity'].iloc[0] if 'log_price' in self.elasticity_coefficients['feature'].values else 0
        }
        
        self.is_trained = True
        return self.metrics
    
    def predict(self, X: pd.DataFrame) -> List[Dict]:
        """Предсказание спроса при изменении цены"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        # Подготовка данных
        X_log = X.copy()
        X_log['log_price'] = np.log(X_log['price'])
        
        feature_columns = [col for col in X_log.columns if col != 'price']
        X_scaled = self.scaler.transform(X_log[feature_columns])
        
        # Предсказания
        predictions = self.model.predict(X_scaled)
        
        results = []
        for i, pred in enumerate(predictions):
            results.append({
                "product_id": X.index[i] if hasattr(X, 'index') else i,
                "predicted_demand": float(pred),
                "current_price": float(X.iloc[i]['price']),
                "price_sensitivity": self._calculate_price_sensitivity(X.iloc[i]['price'])
            })
        
        return results
    
    def _calculate_price_sensitivity(self, price: float) -> str:
        """Расчет чувствительности к цене"""
        if 'log_price' in self.elasticity_coefficients['feature'].values:
            elasticity = self.elasticity_coefficients[
                self.elasticity_coefficients['feature'] == 'log_price'
            ]['elasticity'].iloc[0]
            
            if abs(elasticity) < 0.5:
                return "low"
            elif abs(elasticity) < 1.5:
                return "medium"
            else:
                return "high"
        
        return "unknown"
    
    def get_elasticity_coefficients(self) -> pd.DataFrame:
        """Получение коэффициентов эластичности"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        return self.elasticity_coefficients
    
    def optimize_price(self, product_id: int, current_price: float, target_demand: float) -> Dict:
        """Оптимизация цены для достижения целевого спроса"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        # Упрощенная оптимизация цены
        price_elasticity = self.elasticity_coefficients[
            self.elasticity_coefficients['feature'] == 'log_price'
        ]['elasticity'].iloc[0] if 'log_price' in self.elasticity_coefficients['feature'].values else -1
        
        # Расчет оптимальной цены
        if price_elasticity != 0:
            optimal_price = current_price * (target_demand / current_price) ** (1 / price_elasticity)
        else:
            optimal_price = current_price
        
        return {
            "product_id": product_id,
            "current_price": current_price,
            "optimal_price": optimal_price,
            "target_demand": target_demand,
            "price_change_percent": ((optimal_price - current_price) / current_price) * 100
        }
