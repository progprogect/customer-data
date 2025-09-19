#!/usr/bin/env python3
"""
Production Model Usage Example
Пример использования обученной XGBoost модели

Model Version: 20250919_160301
"""

import joblib
import pandas as pd
import numpy as np

# Загрузка модели и скейлера
model = joblib.load('production_model_20250919_160301/xgboost_model.pkl')
scaler = joblib.load('production_model_20250919_160301/scaler.pkl')

# Параметры для заполнения NaN
FILL_VALUES = {
    "recency_days": 23.0,
    "frequency_90d": 1.0,
    "monetary_180d": 316.61,
    "aov_180d": 597.46,
    "orders_lifetime": 1.0,
    "revenue_lifetime": 335.73,
    "categories_unique": 2.0
}

FEATURE_NAMES = ['recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d', 'orders_lifetime', 'revenue_lifetime', 'categories_unique']

def predict_purchase_probability(user_features: dict) -> dict:
    """
    Предсказание вероятности покупки для пользователя
    
    Args:
        user_features: Словарь с признаками пользователя
        
    Returns:
        dict: Результат предсказания
    """
    # Создание DataFrame
    df = pd.DataFrame([user_features])
    
    # Заполнение отсутствующих признаков
    for feature in FEATURE_NAMES:
        if feature not in df.columns:
            df[feature] = FILL_VALUES[feature]
        df[feature].fillna(FILL_VALUES[feature], inplace=True)
    
    # Применение скейлера
    X_scaled = scaler.transform(df[FEATURE_NAMES])
    
    # Предсказание
    probability = model.predict_proba(X_scaled)[0, 1]
    prediction = model.predict(X_scaled)[0]
    
    return {
        'probability': float(probability),
        'prediction': bool(prediction),
        'confidence': 'high' if probability > 0.8 or probability < 0.2 else 'medium'
    }

# Пример использования
if __name__ == "__main__":
    # Тестовый пользователь
    test_user = {
        'recency_days': 15.0,
        'frequency_90d': 3,
        'monetary_180d': 500.0,
        'aov_180d': 167.0,
        'orders_lifetime': 10,
        'revenue_lifetime': 2500.0,
        'categories_unique': 5
    }
    
    result = predict_purchase_probability(test_user)
    print(f"Prediction: {result}")
