"""
ML Service for Purchase Probability Predictions
Сервис для загрузки модели и выполнения inference

Author: Customer Data Analytics Team
"""

import joblib
import json
import pandas as pd
import numpy as np
import logging
import os
import time
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)


class MLModelService:
    """Singleton сервис для работы с ML моделью"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MLModelService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.model = None
            self.scaler = None
            self.metadata = None
            self.model_version = None
            self.feature_names = None
            self.fill_values = None
            self.load_timestamp = None
            self._initialized = True
            logger.info("🤖 ML Model Service инициализирован")
    
    def load_model(self, model_path: Optional[str] = None) -> bool:
        """
        Загрузка модели и артефактов
        
        Args:
            model_path: Путь к директории с моделью (если не указан, ищем последнюю)
            
        Returns:
            bool: Успешность загрузки
        """
        try:
            if model_path is None:
                model_path = self._find_latest_model()
            
            if not model_path:
                raise FileNotFoundError("Не найдена директория с моделью")
            
            logger.info(f"📦 Загрузка модели из: {model_path}")
            
            # Пути к файлам
            model_file = Path(model_path) / "xgboost_model.pkl"
            scaler_file = Path(model_path) / "scaler.pkl"
            metadata_file = Path(model_path) / "model_metadata.json"
            
            # Проверка существования файлов
            for file_path in [model_file, scaler_file, metadata_file]:
                if not file_path.exists():
                    raise FileNotFoundError(f"Файл не найден: {file_path}")
            
            # Загрузка модели
            logger.info("🔄 Загрузка XGBoost модели...")
            self.model = joblib.load(model_file)
            
            # Загрузка скейлера
            logger.info("🔄 Загрузка StandardScaler...")
            self.scaler = joblib.load(scaler_file)
            
            # Загрузка метаданных
            logger.info("🔄 Загрузка метаданных...")
            with open(metadata_file, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
            
            # Извлечение важной информации
            self.model_version = self.metadata.get('model_version', 'unknown')
            self.feature_names = self.metadata.get('feature_names', [])
            self.fill_values = self.metadata.get('fill_values', {})
            self.load_timestamp = datetime.now().isoformat()
            
            # Валидация загруженных данных
            self._validate_loaded_model()
            
            logger.info("✅ Модель успешно загружена!")
            logger.info(f"   📌 Версия: {self.model_version}")
            logger.info(f"   📊 Признаков: {len(self.feature_names)}")
            logger.info(f"   🕐 Время загрузки: {self.load_timestamp}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки модели: {e}")
            self._reset_model()
            return False
    
    def _find_latest_model(self) -> Optional[str]:
        """Поиск последней версии модели"""
        try:
            # Ищем в ml-engine/scripts директории
            base_path = Path(__file__).parent.parent.parent / "ml-engine" / "scripts"
            
            if not base_path.exists():
                logger.warning(f"Директория не найдена: {base_path}")
                return None
            
            # Ищем папки с паттерном production_model_*
            model_dirs = list(base_path.glob("production_model_*"))
            
            if not model_dirs:
                logger.warning("Не найдены директории с моделями")
                return None
            
            # Сортируем по имени (версии) и берём последнюю
            latest_model = sorted(model_dirs, reverse=True)[0]
            logger.info(f"🔍 Найдена последняя модель: {latest_model}")
            
            return str(latest_model)
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска модели: {e}")
            return None
    
    def _validate_loaded_model(self):
        """Валидация загруженной модели"""
        if self.model is None:
            raise ValueError("Модель не загружена")
        
        if self.scaler is None:
            raise ValueError("Скейлер не загружен")
        
        if not self.feature_names:
            raise ValueError("Не определены имена признаков")
        
        if not self.fill_values:
            raise ValueError("Не определены значения для заполнения NaN")
        
        # Проверяем что у модели есть нужные методы
        if not hasattr(self.model, 'predict_proba'):
            raise ValueError("Модель не поддерживает predict_proba")
        
        if not hasattr(self.scaler, 'transform'):
            raise ValueError("Скейлер не поддерживает transform")
        
        logger.info("✅ Валидация модели прошла успешно")
    
    def _reset_model(self):
        """Сброс загруженной модели"""
        self.model = None
        self.scaler = None
        self.metadata = None
        self.model_version = None
        self.feature_names = None
        self.fill_values = None
        self.load_timestamp = None
    
    def is_model_loaded(self) -> bool:
        """Проверка загружена ли модель"""
        return self.model is not None and self.scaler is not None
    
    def get_model_info(self) -> Dict[str, Any]:
        """Получение информации о загруженной модели"""
        if not self.is_model_loaded():
            return {"status": "not_loaded"}
        
        return {
            "status": "loaded",
            "model_version": self.model_version,
            "load_timestamp": self.load_timestamp,
            "feature_names": self.feature_names,
            "feature_count": len(self.feature_names),
            "model_performance": self.metadata.get('test_results', {}).get('metrics', {}),
            "fill_values": self.fill_values
        }
    
    def prepare_features(self, features_list: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Подготовка признаков для inference
        
        Args:
            features_list: Список словарей с признаками
            
        Returns:
            pd.DataFrame: Подготовленные признаки
        """
        if not self.is_model_loaded():
            raise RuntimeError("Модель не загружена")
        
        # Создаем DataFrame
        df = pd.DataFrame(features_list)
        
        # Добавляем отсутствующие колонки и заполняем NaN
        for feature_name in self.feature_names:
            if feature_name not in df.columns:
                df[feature_name] = self.fill_values.get(feature_name, 0.0)
            else:
                # Заполняем NaN значениями из обучения
                fill_value = self.fill_values.get(feature_name, 0.0)
                df[feature_name] = df[feature_name].fillna(fill_value)
        
        # Приводим к правильному порядку колонок
        df_ordered = df[self.feature_names].copy()
        
        # Применяем скейлер
        df_scaled = pd.DataFrame(
            self.scaler.transform(df_ordered),
            columns=self.feature_names,
            index=df_ordered.index
        )
        
        return df_scaled
    
    def predict_probabilities(self, prepared_features: pd.DataFrame) -> np.ndarray:
        """
        Выполнение inference
        
        Args:
            prepared_features: Подготовленные признаки
            
        Returns:
            np.ndarray: Вероятности класса 1
        """
        if not self.is_model_loaded():
            raise RuntimeError("Модель не загружена")
        
        # Получаем вероятности
        probabilities = self.model.predict_proba(prepared_features)
        
        # Возвращаем вероятности класса 1 (purchase = True)
        return probabilities[:, 1]
    
    def predict_batch(self, features_list: List[Dict[str, Any]]) -> Tuple[np.ndarray, float]:
        """
        Batch предсказание с измерением времени
        
        Args:
            features_list: Список признаков для предсказания
            
        Returns:
            Tuple[np.ndarray, float]: Вероятности и время обработки в мс
        """
        start_time = time.time()
        
        try:
            # Подготовка признаков
            prepared_features = self.prepare_features(features_list)
            
            # Inference
            probabilities = self.predict_probabilities(prepared_features)
            
            # Время обработки
            processing_time_ms = (time.time() - start_time) * 1000
            
            return probabilities, processing_time_ms
            
        except Exception as e:
            processing_time_ms = (time.time() - start_time) * 1000
            logger.error(f"❌ Ошибка batch предсказания: {e}")
            raise
    
    def validate_features(self, features: Dict[str, Any]) -> List[str]:
        """
        Валидация признаков
        
        Args:
            features: Словарь с признаками
            
        Returns:
            List[str]: Список ошибок валидации
        """
        errors = []
        
        for feature_name in self.feature_names:
            if feature_name in features:
                value = features[feature_name]
                
                # Проверяем что значение числовое или None
                if value is not None and not isinstance(value, (int, float)):
                    errors.append(f"Feature '{feature_name}' must be numeric, got {type(value).__name__}")
                
                # Проверяем на отрицательные значения для некоторых фичей
                if value is not None and value < 0:
                    if feature_name in ['frequency_90d', 'monetary_180d', 'aov_180d', 
                                      'orders_lifetime', 'revenue_lifetime', 'categories_unique']:
                        errors.append(f"Feature '{feature_name}' cannot be negative: {value}")
        
        return errors


# Глобальный экземпляр сервиса
ml_service = MLModelService()
