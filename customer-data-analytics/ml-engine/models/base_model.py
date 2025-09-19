"""
Base ML Model
Базовый класс для всех ML моделей
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
import pandas as pd
import joblib
from pathlib import Path


class BaseMLModel(ABC):
    """Базовый класс для всех ML моделей"""
    
    def __init__(self, model_name: str):
        self.model_name = model_name
        self.model = None
        self.is_trained = False
        self.metrics = {}
        
    @abstractmethod
    def train(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Dict[str, float]:
        """Обучение модели"""
        pass
    
    @abstractmethod
    def predict(self, X: pd.DataFrame) -> Any:
        """Предсказание модели"""
        pass
    
    def save_model(self, path: str) -> None:
        """Сохранение модели"""
        if self.model is not None:
            joblib.dump(self.model, path)
        else:
            raise ValueError("Модель не обучена")
    
    def load_model(self, path: str) -> None:
        """Загрузка модели"""
        self.model = joblib.load(path)
        self.is_trained = True
    
    def get_metrics(self) -> Dict[str, float]:
        """Получение метрик модели"""
        return self.metrics
