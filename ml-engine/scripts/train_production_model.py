#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Production XGBoost Model Training
Финальное обучение и сериализация production-ready XGBoost модели

Features:
- Precision-optimized XGBoost
- Full preprocessing pipeline
- Model serialization
- Performance benchmarking
- Ready for API integration

Author: Customer Data Analytics Team
"""

import pandas as pd
import numpy as np
import joblib
import json
import warnings
warnings.filterwarnings('ignore')

# ML libraries
import xgboost as xgb
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score, 
    precision_recall_curve, average_precision_score,
    precision_score, recall_score, f1_score
)
from sklearn.model_selection import RandomizedSearchCV

# System libraries
import logging
import sys
import os
from datetime import datetime
import pickle

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('production_model_training.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class ProductionModelTrainer:
    """Production-ready XGBoost model trainer"""
    
    def __init__(self):
        """Инициализация тренера"""
        self.model = None
        self.scaler = None
        self.feature_names = [
            'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
            'orders_lifetime', 'revenue_lifetime', 'categories_unique'
        ]
        self.training_metadata = {}
        self.model_version = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        logger.info("🚀 Production Model Trainer инициализирован")
        logger.info(f"📦 Model version: {self.model_version}")
    
    def load_and_prepare_data(self) -> tuple:
        """Загрузка и подготовка данных"""
        logger.info("📂 Загрузка и подготовка данных...")
        
        try:
            # Загрузка всех сплитов
            train_df = pd.read_csv('train_set.csv')
            valid_df = pd.read_csv('valid_set.csv') 
            test_df = pd.read_csv('test_set.csv')
            
            logger.info(f"✅ Загружено: train={len(train_df)}, valid={len(valid_df)}, test={len(test_df)}")
            
            # Извлечение фичей и таргета
            X_train = train_df[self.feature_names].copy()
            y_train = train_df['target'].copy()
            
            X_valid = valid_df[self.feature_names].copy()
            y_valid = valid_df['target'].copy()
            
            X_test = test_df[self.feature_names].copy()
            y_test = test_df['target'].copy()
            
            # Заполнение NaN медианными значениями из train
            self.fill_values = {}
            for col in self.feature_names:
                median_val = X_train[col].median()
                self.fill_values[col] = median_val
                
                X_train[col].fillna(median_val, inplace=True)
                X_valid[col].fillna(median_val, inplace=True)
                X_test[col].fillna(median_val, inplace=True)
            
            logger.info("✅ NaN значения заполнены медианными значениями из train set")
            
            # Статистика классов
            train_pos_rate = y_train.mean()
            valid_pos_rate = y_valid.mean()
            test_pos_rate = y_test.mean()
            
            logger.info(f"🎯 Баланс классов:")
            logger.info(f"   Train: {train_pos_rate:.1%} positive")
            logger.info(f"   Valid: {valid_pos_rate:.1%} positive")
            logger.info(f"   Test:  {test_pos_rate:.1%} positive")
            
            # Сохранение метаданных
            self.training_metadata = {
                'model_version': self.model_version,
                'training_timestamp': datetime.now().isoformat(),
                'data_statistics': {
                    'train_size': int(len(train_df)),
                    'valid_size': int(len(valid_df)),
                    'test_size': int(len(test_df)),
                    'feature_count': len(self.feature_names),
                    'train_pos_rate': float(train_pos_rate),
                    'valid_pos_rate': float(valid_pos_rate),
                    'test_pos_rate': float(test_pos_rate)
                },
                'feature_names': self.feature_names,
                'fill_values': {k: float(v) for k, v in self.fill_values.items()}
            }
            
            return X_train, X_valid, X_test, y_train, y_valid, y_test
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            raise
    
    def prepare_features(self, X_train: pd.DataFrame, X_valid: pd.DataFrame, X_test: pd.DataFrame) -> tuple:
        """Подготовка фичей с scaling"""
        logger.info("🔧 Подготовка фичей...")
        
        # Обучение скейлера на train set
        self.scaler = StandardScaler()
        X_train_scaled = pd.DataFrame(
            self.scaler.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index
        )
        
        # Применение к valid и test
        X_valid_scaled = pd.DataFrame(
            self.scaler.transform(X_valid),
            columns=X_valid.columns,
            index=X_valid.index
        )
        
        X_test_scaled = pd.DataFrame(
            self.scaler.transform(X_test),
            columns=X_test.columns,
            index=X_test.index
        )
        
        logger.info("✅ Скейлинг применен ко всем сплитам")
        
        return X_train_scaled, X_valid_scaled, X_test_scaled
    
    def train_optimized_model(self, X_train: pd.DataFrame, y_train: pd.Series,
                            X_valid: pd.DataFrame, y_valid: pd.Series) -> dict:
        """Обучение оптимизированной модели"""
        logger.info("🎯 Обучение precision-optimized XGBoost модели...")
        
        # Расчет scale_pos_weight
        neg_count = (y_train == 0).sum()
        pos_count = (y_train == 1).sum()
        scale_pos_weight = neg_count / pos_count
        
        logger.info(f"⚖️ Scale pos weight: {scale_pos_weight:.2f}")
        
        # Основанные на предыдущих экспериментах лучшие параметры
        best_params = {
            'n_estimators': 200,
            'max_depth': 4,
            'learning_rate': 0.1,
            'min_child_weight': 3,
            'subsample': 0.9,
            'colsample_bytree': 0.9,
            'reg_alpha': 0.1,
            'reg_lambda': 1.5,
            'scale_pos_weight': scale_pos_weight,
            'objective': 'binary:logistic',
            'random_state': 42,
            'n_jobs': -1
        }
        
        # Обучение модели
        self.model = xgb.XGBClassifier(**best_params)
        
        logger.info("⚡ Запуск обучения модели...")
        self.model.fit(X_train, y_train)
        
        # Предсказания на validation set
        y_valid_pred = self.model.predict(X_valid)
        y_valid_proba = self.model.predict_proba(X_valid)[:, 1]
        
        # Метрики
        valid_precision = precision_score(y_valid, y_valid_pred)
        valid_recall = recall_score(y_valid, y_valid_pred)
        valid_f1 = f1_score(y_valid, y_valid_pred)
        valid_roc_auc = roc_auc_score(y_valid, y_valid_proba)
        
        training_results = {
            'model_params': best_params,
            'validation_metrics': {
                'precision': float(valid_precision),
                'recall': float(valid_recall),
                'f1_score': float(valid_f1),
                'roc_auc': float(valid_roc_auc)
            },
            'early_stopping_round': int(self.model.best_iteration) if hasattr(self.model, 'best_iteration') else None
        }
        
        logger.info("✅ Модель обучена!")
        logger.info(f"📊 Valid metrics: Precision={valid_precision:.3f}, Recall={valid_recall:.3f}, F1={valid_f1:.3f}")
        
        return training_results
    
    def evaluate_final_model(self, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
        """Финальная оценка модели на test set"""
        logger.info("🧪 Финальная оценка на test set...")
        
        # Предсказания
        y_test_pred = self.model.predict(X_test)
        y_test_proba = self.model.predict_proba(X_test)[:, 1]
        
        # Основные метрики
        test_precision = precision_score(y_test, y_test_pred)
        test_recall = recall_score(y_test, y_test_pred)
        test_f1 = f1_score(y_test, y_test_pred)
        test_roc_auc = roc_auc_score(y_test, y_test_proba)
        test_pr_auc = average_precision_score(y_test, y_test_proba)
        
        # Confusion matrix
        cm = confusion_matrix(y_test, y_test_pred)
        tn, fp, fn, tp = cm.ravel()
        
        # Бизнес-метрики
        false_positive_rate = fp / (fp + tn) if (fp + tn) > 0 else 0
        false_negative_rate = fn / (fn + tp) if (fn + tp) > 0 else 0
        
        test_results = {
            'confusion_matrix': {
                'TP': int(tp), 'FP': int(fp), 
                'TN': int(tn), 'FN': int(fn)
            },
            'metrics': {
                'precision': float(test_precision),
                'recall': float(test_recall),
                'f1_score': float(test_f1),
                'roc_auc': float(test_roc_auc),
                'pr_auc': float(test_pr_auc)
            },
            'business_metrics': {
                'false_positive_rate': float(false_positive_rate),
                'false_negative_rate': float(false_negative_rate),
                'precision_focused_score': float(test_precision * 0.7 + test_recall * 0.3)  # Weighted score
            }
        }
        
        # Feature importance
        if hasattr(self.model, 'feature_importances_'):
            feature_importance = {}
            for feature, importance in zip(self.feature_names, self.model.feature_importances_):
                feature_importance[feature] = float(importance)
            test_results['feature_importance'] = feature_importance
        
        logger.info("🎯 ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ:")
        logger.info(f"   Precision: {test_precision:.3f}")
        logger.info(f"   Recall: {test_recall:.3f}")
        logger.info(f"   F1-Score: {test_f1:.3f}")
        logger.info(f"   ROC-AUC: {test_roc_auc:.3f}")
        logger.info(f"   False Positive Rate: {false_positive_rate:.1%}")
        
        return test_results
    
    def save_production_model(self, training_results: dict, test_results: dict) -> dict:
        """Сохранение production-ready модели"""
        logger.info("💾 Сохранение production модели...")
        
        # Создание директории для модели
        model_dir = f"production_model_{self.model_version}"
        os.makedirs(model_dir, exist_ok=True)
        
        # Пути к файлам
        model_path = os.path.join(model_dir, "xgboost_model.pkl")
        scaler_path = os.path.join(model_dir, "scaler.pkl")
        metadata_path = os.path.join(model_dir, "model_metadata.json")
        
        # Сохранение модели
        joblib.dump(self.model, model_path)
        logger.info(f"✅ Модель сохранена: {model_path}")
        
        # Сохранение скейлера
        joblib.dump(self.scaler, scaler_path)
        logger.info(f"✅ Скейлер сохранен: {scaler_path}")
        
        # Полные метаданные
        complete_metadata = {
            **self.training_metadata,
            'training_results': training_results,
            'test_results': test_results,
            'model_files': {
                'model': model_path,
                'scaler': scaler_path,
                'metadata': metadata_path
            },
            'usage_instructions': {
                'load_model': f"joblib.load('{model_path}')",
                'load_scaler': f"joblib.load('{scaler_path}')",
                'prediction_pipeline': [
                    "1. Load model and scaler",
                    "2. Fill NaN values using fill_values",
                    "3. Apply scaler.transform()",
                    "4. Use model.predict() or model.predict_proba()"
                ]
            }
        }
        
        # Сохранение метаданных
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(complete_metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Метаданные сохранены: {metadata_path}")
        
        save_info = {
            'model_version': self.model_version,
            'model_directory': model_dir,
            'files': {
                'model': model_path,
                'scaler': scaler_path,
                'metadata': metadata_path
            }
        }
        
        return save_info
    
    def create_prediction_example(self, X_test: pd.DataFrame, save_info: dict) -> None:
        """Создание примера использования модели"""
        logger.info("📝 Создание примера использования...")
        
        example_code = f'''#!/usr/bin/env python3
"""
Production Model Usage Example
Пример использования обученной XGBoost модели

Model Version: {self.model_version}
"""

import joblib
import pandas as pd
import numpy as np

# Загрузка модели и скейлера
model = joblib.load('{save_info['files']['model']}')
scaler = joblib.load('{save_info['files']['scaler']}')

# Параметры для заполнения NaN
FILL_VALUES = {json.dumps(self.fill_values, indent=4)}

FEATURE_NAMES = {self.feature_names}

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
    
    return {{
        'probability': float(probability),
        'prediction': bool(prediction),
        'confidence': 'high' if probability > 0.8 or probability < 0.2 else 'medium'
    }}

# Пример использования
if __name__ == "__main__":
    # Тестовый пользователь
    test_user = {{
        'recency_days': 15.0,
        'frequency_90d': 3,
        'monetary_180d': 500.0,
        'aov_180d': 167.0,
        'orders_lifetime': 10,
        'revenue_lifetime': 2500.0,
        'categories_unique': 5
    }}
    
    result = predict_purchase_probability(test_user)
    print(f"Prediction: {{result}}")
'''
        
        example_path = os.path.join(save_info['model_directory'], "usage_example.py")
        with open(example_path, 'w', encoding='utf-8') as f:
            f.write(example_code)
        
        logger.info(f"✅ Пример использования создан: {example_path}")

def main():
    """Главная функция"""
    logger.info("=" * 70)
    logger.info("🚀 ЗАПУСК PRODUCTION ОБУЧЕНИЯ XGBOOST МОДЕЛИ")
    logger.info("=" * 70)
    
    try:
        # Инициализация тренера
        trainer = ProductionModelTrainer()
        
        # Загрузка и подготовка данных
        X_train, X_valid, X_test, y_train, y_valid, y_test = trainer.load_and_prepare_data()
        
        # Подготовка фичей
        X_train_scaled, X_valid_scaled, X_test_scaled = trainer.prepare_features(X_train, X_valid, X_test)
        
        # Обучение модели
        training_results = trainer.train_optimized_model(X_train_scaled, y_train, X_valid_scaled, y_valid)
        
        # Финальная оценка
        test_results = trainer.evaluate_final_model(X_test_scaled, y_test)
        
        # Сохранение модели
        save_info = trainer.save_production_model(training_results, test_results)
        
        # Создание примера использования
        trainer.create_prediction_example(X_test, save_info)
        
        # Финальный отчет
        logger.info("=" * 70)
        logger.info("🎉 PRODUCTION МОДЕЛЬ УСПЕШНО ОБУЧЕНА И СОХРАНЕНА!")
        logger.info("=" * 70)
        logger.info(f"📦 Model Version: {trainer.model_version}")
        logger.info(f"📁 Model Directory: {save_info['model_directory']}")
        logger.info(f"🎯 Test Precision: {test_results['metrics']['precision']:.3f}")
        logger.info(f"🎯 Test Recall: {test_results['metrics']['recall']:.3f}")
        logger.info(f"🎯 Test F1-Score: {test_results['metrics']['f1_score']:.3f}")
        logger.info(f"💼 False Positive Rate: {test_results['business_metrics']['false_positive_rate']:.1%}")
        
        # Топ-3 важных признака
        if 'feature_importance' in test_results:
            sorted_features = sorted(test_results['feature_importance'].items(), key=lambda x: x[1], reverse=True)
            logger.info("🏆 ТОП-3 важных признака:")
            for i, (feature, importance) in enumerate(sorted_features[:3], 1):
                logger.info(f"   {i}. {feature}: {importance:.3f}")
        
        logger.info("✅ ГОТОВО К PRODUCTION ИСПОЛЬЗОВАНИЮ!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
