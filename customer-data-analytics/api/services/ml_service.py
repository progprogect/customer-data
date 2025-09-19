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
            # Purchase prediction model
            self.model = None
            self.scaler = None
            self.metadata = None
            self.model_version = None
            self.feature_names = None
            self.fill_values = None
            self.load_timestamp = None
            
            # Churn prediction model
            self.churn_model = None
            self.churn_model_version = None
            self.churn_load_timestamp = None
            self.churn_feature_importance = None
            
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
    
    def load_churn_model(self, model_path: Optional[str] = None) -> bool:
        """
        Загрузка churn prediction модели
        
        Args:
            model_path: Путь к файлу модели (если не указан, ищем в ml-engine/models)
            
        Returns:
            bool: Успешность загрузки
        """
        try:
            if model_path is None:
                model_path = self._find_churn_model()
            
            if not model_path or not os.path.exists(model_path):
                raise FileNotFoundError(f"Churn модель не найдена: {model_path}")
            
            logger.info(f"📦 Загрузка churn модели из: {model_path}")
            
            # Загрузка модели
            self.churn_model = joblib.load(model_path)
            
            # Устанавливаем версию и время загрузки
            self.churn_model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.churn_load_timestamp = datetime.now().isoformat()
            
            # Загружаем feature importance из отчета
            self._load_churn_feature_importance()
            
            logger.info("✅ Churn модель успешно загружена!")
            logger.info(f"   📌 Версия: {self.churn_model_version}")
            logger.info(f"   🕐 Время загрузки: {self.churn_load_timestamp}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки churn модели: {e}")
            self._reset_churn_model()
            return False
    
    def _find_churn_model(self) -> Optional[str]:
        """Поиск churn модели"""
        try:
            # Ищем в ml-engine/models директории
            base_path = Path(__file__).parent.parent.parent / "ml-engine" / "models"
            
            if not base_path.exists():
                logger.warning(f"Директория не найдена: {base_path}")
                return None
            
            # Ищем файл churn_xgboost_model.pkl
            model_file = base_path / "churn_xgboost_model.pkl"
            
            if not model_file.exists():
                logger.warning("Churn модель не найдена")
                return None
            
            logger.info(f"🔍 Найдена churn модель: {model_file}")
            return str(model_file)
            
        except Exception as e:
            logger.error(f"❌ Ошибка поиска churn модели: {e}")
            return None
    
    def _load_churn_feature_importance(self):
        """Загрузка feature importance из отчета"""
        try:
            base_path = Path(__file__).parent.parent.parent / "ml-engine" / "models"
            report_file = base_path / "churn_model_report.txt"
            
            if report_file.exists():
                with open(report_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Парсим feature importance из отчета
                self.churn_feature_importance = {}
                lines = content.split('\n')
                for line in lines:
                    if ': 0.' in line and any(feature in line for feature in [
                        'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
                        'orders_lifetime', 'revenue_lifetime', 'categories_unique'
                    ]):
                        parts = line.split(':')
                        if len(parts) == 2:
                            feature = parts[0].strip()
                            importance = float(parts[1].strip())
                            self.churn_feature_importance[feature] = importance
                
                logger.info(f"📊 Загружена feature importance: {len(self.churn_feature_importance)} признаков")
            else:
                # Используем дефолтные значения из обучения
                self.churn_feature_importance = {
                    'revenue_lifetime': 0.1806,
                    'aov_180d': 0.1648,
                    'monetary_180d': 0.1525,
                    'categories_unique': 0.1517,
                    'orders_lifetime': 0.1305,
                    'frequency_90d': 0.1168,
                    'recency_days': 0.1032
                }
                logger.info("📊 Использованы дефолтные значения feature importance")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки feature importance: {e}")
            self.churn_feature_importance = {}
    
    def _reset_churn_model(self):
        """Сброс churn модели"""
        self.churn_model = None
        self.churn_model_version = None
        self.churn_load_timestamp = None
        self.churn_feature_importance = None
    
    def is_churn_model_loaded(self) -> bool:
        """Проверка загружена ли churn модель"""
        return self.churn_model is not None
    
    def predict_churn(self, features: Dict[str, Any]) -> Tuple[float, bool, List[str]]:
        """
        Предсказание оттока клиента
        
        Args:
            features: Словарь с признаками клиента
            
        Returns:
            Tuple[float, bool, List[str]]: (вероятность, предсказание, причины)
        """
        if not self.is_churn_model_loaded():
            raise RuntimeError("Churn модель не загружена")
        
        try:
            # Подготовка данных
            feature_df = pd.DataFrame([{
                'recency_days': features.get('recency_days', 999),  # 999 если не покупал
                'frequency_90d': features.get('frequency_90d', 0),
                'monetary_180d': features.get('monetary_180d', 0),
                'aov_180d': features.get('aov_180d', 0),
                'orders_lifetime': features.get('orders_lifetime', 0),
                'revenue_lifetime': features.get('revenue_lifetime', 0),
                'categories_unique': features.get('categories_unique', 0)
            }])
            
            # Предсказание
            churn_probability = self.churn_model.predict_proba(feature_df)[0][1]
            will_churn = churn_probability >= 0.6  # threshold
            
            # Генерируем причины на основе feature importance и значений
            top_reasons = self._generate_churn_reasons(features, churn_probability)
            
            return churn_probability, will_churn, top_reasons
            
        except Exception as e:
            logger.error(f"❌ Ошибка предсказания оттока: {e}")
            raise
    
    def _generate_churn_reasons(self, features: Dict[str, Any], probability: float) -> List[str]:
        """Генерация причин риска оттока"""
        reasons = []
        
        # Пороги для определения проблемных значений
        thresholds = {
            'recency_days': 30,      # больше 30 дней
            'frequency_90d': 1,      # меньше 2 заказов
            'monetary_180d': 500,    # меньше 500 рублей
            'aov_180d': 200,         # меньше 200 рублей
            'orders_lifetime': 2,    # меньше 3 заказов
            'revenue_lifetime': 1000, # меньше 1000 рублей
            'categories_unique': 1   # меньше 2 категорий
        }
        
        # Проверяем каждый признак
        if features.get('recency_days', 0) > thresholds['recency_days']:
            reasons.append("давно не покупал")
        
        if features.get('frequency_90d', 0) < thresholds['frequency_90d']:
            reasons.append("низкая частота покупок за 90 дней")
        
        if features.get('monetary_180d', 0) < thresholds['monetary_180d']:
            reasons.append("низкие траты за 180 дней")
        
        if features.get('aov_180d', 0) < thresholds['aov_180d']:
            reasons.append("снижение среднего чека")
        
        if features.get('orders_lifetime', 0) < thresholds['orders_lifetime']:
            reasons.append("мало заказов за всё время")
        
        if features.get('revenue_lifetime', 0) < thresholds['revenue_lifetime']:
            reasons.append("низкая общая выручка")
        
        if features.get('categories_unique', 0) < thresholds['categories_unique']:
            reasons.append("узкий ассортимент покупок")
        
        # Если причин нет, добавляем общую
        if not reasons:
            if probability > 0.7:
                reasons.append("высокий общий риск оттока")
            elif probability > 0.5:
                reasons.append("средний риск оттока")
            else:
                reasons.append("стабильный клиент")
        
        # Ограничиваем количество причин
        return reasons[:3]
    
    def validate_churn_features(self, features: Dict[str, Any]) -> List[str]:
        """
        Валидация признаков для churn prediction
        
        Args:
            features: Словарь с признаками
            
        Returns:
            List[str]: Список ошибок валидации
        """
        errors = []
        
        churn_feature_names = [
            'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
            'orders_lifetime', 'revenue_lifetime', 'categories_unique'
        ]
        
        for feature_name in churn_feature_names:
            if feature_name in features:
                value = features[feature_name]
                
                # Проверяем что значение числовое или None
                if value is not None and not isinstance(value, (int, float)):
                    errors.append(f"Feature '{feature_name}' must be numeric, got {type(value).__name__}")
                
                # Проверяем на отрицательные значения
                if value is not None and value < 0:
                    errors.append(f"Feature '{feature_name}' cannot be negative: {value}")
        
        # Проверяем обязательные поля
        required_fields = ['frequency_90d', 'monetary_180d', 'orders_lifetime', 'revenue_lifetime', 'categories_unique']
        for field in required_fields:
            if field not in features or features[field] is None:
                errors.append(f"Required field '{field}' is missing or null")
        
        return errors


# Глобальный экземпляр сервиса
ml_service = MLModelService()
