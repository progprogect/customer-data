#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prepare RFM data for K-means clustering
Author: Customer Data Analytics Team
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import logging
from typing import Tuple, Optional
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from load_rfm_data import load_rfm_data, connect_to_db

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def select_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Выбор признаков для кластеризации
    
    Args:
        df: DataFrame с RFM данными
        
    Returns:
        Tuple[pd.DataFrame, pd.Series]: (X - признаки, user_ids - идентификаторы)
    """
    # Признаки для кластеризации (5 RFM-признаков)
    features = [
        "recency_days",
        "frequency_90d", 
        "monetary_180d",
        "aov_180d",
        "categories_unique"
    ]
    
    # Проверяем наличие всех признаков
    missing_features = set(features) - set(df.columns)
    if missing_features:
        raise ValueError(f"Отсутствуют признаки: {missing_features}")
    
    # Выделяем признаки и user_id
    X = df[features].copy()
    user_ids = df['user_id'].copy()
    
    logger.info(f"Выбрано {len(features)} признаков для кластеризации: {features}")
    logger.info(f"Размер данных: {X.shape[0]} пользователей × {X.shape[1]} признаков")
    
    return X, user_ids

def apply_log_transformation(X: pd.DataFrame) -> pd.DataFrame:
    """
    Применение лог-трансформации к денежным признакам
    
    Args:
        X: DataFrame с признаками
        
    Returns:
        pd.DataFrame: DataFrame с лог-трансформированными признаками
    """
    # Признаки для лог-трансформации (денежные)
    log_features = ["monetary_180d", "aov_180d"]
    
    X_transformed = X.copy()
    
    for col in log_features:
        if col in X_transformed.columns:
            # Проверяем на отрицательные значения
            if (X_transformed[col] < 0).any():
                logger.warning(f"Найдены отрицательные значения в {col}, заменяем на 0")
                X_transformed[col] = np.maximum(X_transformed[col], 0)
            
            # Применяем log1p (log(1+x))
            X_transformed[col] = np.log1p(X_transformed[col])
            logger.info(f"Применена лог-трансформация к {col}")
    
    return X_transformed

def apply_scaling(X: pd.DataFrame) -> Tuple[np.ndarray, StandardScaler]:
    """
    Применение стандартизации (z-score нормализация)
    
    Args:
        X: DataFrame с признаками
        
    Returns:
        Tuple[np.ndarray, StandardScaler]: (масштабированные данные, объект scaler)
    """
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    logger.info("Применена стандартизация (StandardScaler)")
    logger.info(f"Средние значения после масштабирования: {X_scaled.mean(axis=0)}")
    logger.info(f"Стандартные отклонения после масштабирования: {X_scaled.std(axis=0)}")
    
    return X_scaled, scaler

def validate_preprocessing(X_scaled: np.ndarray, user_ids: pd.Series) -> bool:
    """
    Валидация результатов предобработки
    
    Args:
        X_scaled: Масштабированные данные
        user_ids: Идентификаторы пользователей
        
    Returns:
        bool: True если валидация прошла успешно
    """
    # Проверка размерности
    if X_scaled.shape[0] != len(user_ids):
        logger.error(f"Несоответствие размеров: X_scaled {X_scaled.shape[0]} vs user_ids {len(user_ids)}")
        return False
    
    # Проверка масштабирования (mean ≈ 0, std ≈ 1)
    means = X_scaled.mean(axis=0)
    stds = X_scaled.std(axis=0)
    
    if not np.allclose(means, 0, atol=1e-10):
        logger.warning(f"Средние значения не равны 0: {means}")
    
    if not np.allclose(stds, 1, atol=1e-10):
        logger.warning(f"Стандартные отклонения не равны 1: {stds}")
    
    # Проверка на NaN и Inf
    if np.isnan(X_scaled).any():
        logger.error("Найдены NaN значения в масштабированных данных")
        return False
    
    if np.isinf(X_scaled).any():
        logger.error("Найдены Inf значения в масштабированных данных")
        return False
    
    logger.info("Валидация предобработки прошла успешно")
    return True

def print_preprocessing_summary(X_original: pd.DataFrame, X_scaled: np.ndarray, 
                              user_ids: pd.Series, scaler: StandardScaler) -> None:
    """
    Вывод сводной информации о предобработке
    
    Args:
        X_original: Исходные данные
        X_scaled: Масштабированные данные
        user_ids: Идентификаторы пользователей
        scaler: Объект StandardScaler
    """
    print("\n" + "="*70)
    print("СВОДНАЯ ИНФОРМАЦИЯ О ПРЕДОБРАБОТКЕ ДАННЫХ")
    print("="*70)
    
    print(f"Количество пользователей: {len(user_ids)}")
    print(f"Количество признаков: {X_scaled.shape[1]}")
    print(f"Размерность данных: {X_scaled.shape}")
    
    print("\nСтатистики ДО предобработки:")
    print("-" * 50)
    print(X_original.describe())
    
    print("\nСтатистики ПОСЛЕ предобработки:")
    print("-" * 50)
    feature_names = ["recency_days", "frequency_90d", "monetary_180d", "aov_180d", "categories_unique"]
    scaled_df = pd.DataFrame(X_scaled, columns=feature_names)
    print(scaled_df.describe())
    
    print("\nПараметры масштабирования:")
    print("-" * 50)
    print("Средние значения (mean_):", scaler.mean_)
    print("Стандартные отклонения (scale_):", scaler.scale_)
    
    print("\nПример масштабированных данных (первые 5 строк):")
    print("-" * 50)
    print(scaled_df.head())

def main():
    """
    Основная функция для предобработки данных
    """
    logger.info("Начало предобработки данных для K-means кластеризации")
    
    # Подключение к БД и загрузка данных
    conn = connect_to_db()
    if conn is None:
        logger.error("Не удалось подключиться к базе данных")
        sys.exit(1)
    
    try:
        # Загрузка RFM данных
        df = load_rfm_data(conn)
        if df is None:
            logger.error("Не удалось загрузить данные")
            sys.exit(1)
        
        # 1. Выбор признаков
        X, user_ids = select_features(df)
        
        # 2. Лог-трансформация
        X_transformed = apply_log_transformation(X)
        
        # 3. Масштабирование
        X_scaled, scaler = apply_scaling(X_transformed)
        
        # 4. Валидация
        if not validate_preprocessing(X_scaled, user_ids):
            logger.error("Валидация предобработки не прошла")
            sys.exit(1)
        
        # 5. Вывод сводной информации
        print_preprocessing_summary(X, X_scaled, user_ids, scaler)
        
        logger.info("Предобработка данных завершена успешно")
        
        return X_scaled, user_ids, scaler
        
    except Exception as e:
        logger.error(f"Ошибка при предобработке данных: {e}")
        sys.exit(1)
    
    finally:
        if conn:
            conn.close()
            logger.info("Подключение к базе данных закрыто")

if __name__ == "__main__":
    X_scaled, user_ids, scaler = main()

