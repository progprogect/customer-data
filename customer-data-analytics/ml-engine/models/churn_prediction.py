"""
Churn Prediction Model
Модель предсказания оттока клиентов
"""

from typing import Dict, List, Optional
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from .base_model import BaseMLModel


class ChurnPredictionModel(BaseMLModel):
    """Модель предсказания оттока клиентов"""
    
    def __init__(self, n_estimators: int = 100):
        super().__init__("churn_prediction")
        self.n_estimators = n_estimators
        self.feature_importance = None
        
    def train(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Dict[str, float]:
        """Обучение модели предсказания оттока"""
        if y is None:
            raise ValueError("Для обучения модели оттока необходим целевой признак")
        
        # Разделение на обучающую и тестовую выборки
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Обучение модели
        self.model = RandomForestClassifier(
            n_estimators=self.n_estimators,
            random_state=42,
            class_weight='balanced'
        )
        
        self.model.fit(X_train, y_train)
        
        # Предсказания и метрики
        y_pred = self.model.predict(X_test)
        y_pred_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Сохранение важности признаков
        self.feature_importance = pd.DataFrame({
            'feature': X.columns,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        # Метрики
        self.metrics = {
            "accuracy": (y_pred == y_test).mean(),
            "roc_auc": roc_auc_score(y_test, y_pred_proba),
            "n_features": len(X.columns),
            "n_estimators": self.n_estimators
        }
        
        self.is_trained = True
        return self.metrics
    
    def predict(self, X: pd.DataFrame) -> List[Dict]:
        """Предсказание вероятности оттока"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        probabilities = self.model.predict_proba(X)[:, 1]
        predictions = self.model.predict(X)
        
        results = []
        for i, (prob, pred) in enumerate(zip(probabilities, predictions)):
            results.append({
                "user_id": X.index[i] if hasattr(X, 'index') else i,
                "churn_probability": float(prob),
                "churn_prediction": bool(pred),
                "risk_level": self._get_risk_level(prob)
            })
        
        return results
    
    def _get_risk_level(self, probability: float) -> str:
        """Определение уровня риска оттока"""
        if probability < 0.3:
            return "low"
        elif probability < 0.7:
            return "medium"
        else:
            return "high"
    
    def get_feature_importance(self) -> pd.DataFrame:
        """Получение важности признаков"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        return self.feature_importance
