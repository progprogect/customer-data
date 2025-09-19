#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XGBoost Model Training Pipeline
Обучение XGBoost модели для предсказания purchase_next_30d

Features:
- Precision-focused optimization
- Feature importance analysis
- SHAP explanations
- Production-ready model serialization

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
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score, 
    precision_recall_curve, average_precision_score,
    roc_curve, precision_score, recall_score, f1_score
)
from sklearn.model_selection import RandomizedSearchCV
from sklearn.preprocessing import StandardScaler
import shap

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# System libraries
import logging
import sys
import os
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('xgboost_training.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class XGBoostTrainer:
    """Класс для обучения и валидации XGBoost модели"""
    
    def __init__(self, precision_focused=True):
        """
        Инициализация тренера
        
        Args:
            precision_focused: Если True, оптимизируем под precision
        """
        self.precision_focused = precision_focused
        self.model = None
        self.scaler = None
        self.feature_names = None
        self.training_history = {}
        
        # Определяем feature columns (исключаем target и meta)
        self.feature_columns = [
            'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
            'orders_lifetime', 'revenue_lifetime', 'categories_unique'
        ]
        
        logger.info("🚀 XGBoost Trainer инициализирован")
        if precision_focused:
            logger.info("🎯 Режим: PRECISION-FOCUSED (минимизация false positives)")
    
    def load_data(self) -> tuple:
        """Загрузка тренировочных данных"""
        try:
            logger.info("📂 Загрузка данных...")
            
            # Загрузка сплитов
            train_df = pd.read_csv('train_set.csv')
            valid_df = pd.read_csv('valid_set.csv')
            test_df = pd.read_csv('test_set.csv')
            
            logger.info(f"✅ Train: {len(train_df):,} строк")
            logger.info(f"✅ Valid: {len(valid_df):,} строк")
            logger.info(f"✅ Test: {len(test_df):,} строк")
            
            # Проверка наличия всех фичей
            missing_features = set(self.feature_columns) - set(train_df.columns)
            if missing_features:
                raise ValueError(f"Отсутствуют фичи: {missing_features}")
            
            # Извлечение фичей и таргета
            X_train = train_df[self.feature_columns].copy()
            y_train = train_df['target'].copy()
            
            X_valid = valid_df[self.feature_columns].copy()
            y_valid = valid_df['target'].copy()
            
            X_test = test_df[self.feature_columns].copy()
            y_test = test_df['target'].copy()
            
            # Проверка на NaN
            train_nan = X_train.isnull().sum().sum()
            valid_nan = X_valid.isnull().sum().sum()
            test_nan = X_test.isnull().sum().sum()
            
            if train_nan + valid_nan + test_nan > 0:
                logger.warning(f"⚠️ Найдены NaN: train={train_nan}, valid={valid_nan}, test={test_nan}")
                # Заполняем NaN медианными значениями из train
                for col in self.feature_columns:
                    median_val = X_train[col].median()
                    X_train[col].fillna(median_val, inplace=True)
                    X_valid[col].fillna(median_val, inplace=True)
                    X_test[col].fillna(median_val, inplace=True)
                logger.info("✅ NaN заполнены медианными значениями")
            
            # Сохраняем имена фичей
            self.feature_names = self.feature_columns.copy()
            
            # Логируем статистику классов
            train_pos_rate = y_train.mean()
            valid_pos_rate = y_valid.mean()
            test_pos_rate = y_test.mean()
            
            logger.info(f"🎯 Class balance:")
            logger.info(f"   Train: {train_pos_rate:.1%} positive")
            logger.info(f"   Valid: {valid_pos_rate:.1%} positive")
            logger.info(f"   Test:  {test_pos_rate:.1%} positive")
            
            self.training_history['data_stats'] = {
                'train_size': len(train_df),
                'valid_size': len(valid_df),
                'test_size': len(test_df),
                'train_pos_rate': float(train_pos_rate),
                'valid_pos_rate': float(valid_pos_rate),
                'test_pos_rate': float(test_pos_rate),
                'features_count': len(self.feature_columns),
                'feature_names': self.feature_columns
            }
            
            return X_train, X_valid, X_test, y_train, y_valid, y_test
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных: {e}")
            raise
    
    def fit_scaler(self, X_train: pd.DataFrame) -> pd.DataFrame:
        """Обучение и применение скейлера"""
        logger.info("🔧 Применение StandardScaler...")
        
        self.scaler = StandardScaler()
        X_train_scaled = pd.DataFrame(
            self.scaler.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index
        )
        
        # Логируем статистику после скейлинга
        logger.info(f"✅ Скейлинг применен. Средние: {X_train_scaled.mean().round(3).to_dict()}")
        
        return X_train_scaled
    
    def transform_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """Применение обученного скейлера"""
        if self.scaler is None:
            raise ValueError("Скейлер не обучен. Вызовите fit_scaler() сначала.")
        
        X_scaled = pd.DataFrame(
            self.scaler.transform(X),
            columns=X.columns,
            index=X.index
        )
        return X_scaled
    
    def calculate_scale_pos_weight(self, y_train: pd.Series) -> float:
        """Расчет scale_pos_weight для балансировки классов"""
        neg_count = (y_train == 0).sum()
        pos_count = (y_train == 1).sum()
        scale_pos_weight = neg_count / pos_count
        
        logger.info(f"📊 Class distribution: {neg_count:,} negative, {pos_count:,} positive")
        logger.info(f"⚖️ Calculated scale_pos_weight: {scale_pos_weight:.2f}")
        
        return scale_pos_weight
    
    def get_hyperparameter_space(self, scale_pos_weight: float) -> dict:
        """Определение пространства гиперпараметров"""
        if self.precision_focused:
            # Параметры для максимизации precision
            param_space = {
                'n_estimators': [100, 200, 300, 500],
                'max_depth': [3, 4, 5, 6],
                'learning_rate': [0.01, 0.05, 0.1, 0.2],
                'min_child_weight': [1, 3, 5, 7],
                'subsample': [0.8, 0.9, 1.0],
                'colsample_bytree': [0.8, 0.9, 1.0],
                'reg_alpha': [0, 0.1, 0.5, 1.0],
                'reg_lambda': [1, 1.5, 2.0],
                'scale_pos_weight': [scale_pos_weight * 0.8, scale_pos_weight, scale_pos_weight * 1.2]
            }
        else:
            # Стандартные параметры
            param_space = {
                'n_estimators': [100, 200, 300],
                'max_depth': [4, 5, 6],
                'learning_rate': [0.05, 0.1, 0.2],
                'min_child_weight': [1, 3, 5],
                'subsample': [0.8, 0.9],
                'colsample_bytree': [0.8, 0.9],
                'scale_pos_weight': [scale_pos_weight]
            }
        
        logger.info(f"🎲 Hyperparameter space size: {np.prod([len(v) for v in param_space.values()]):,} combinations")
        return param_space
    
    def precision_scorer(self, estimator, X, y):
        """Кастомный scorer для precision"""
        y_pred = estimator.predict(X)
        return precision_score(y, y_pred, zero_division=0)
    
    def train_model(self, X_train: pd.DataFrame, y_train: pd.Series, 
                   X_valid: pd.DataFrame, y_valid: pd.Series) -> dict:
        """Обучение модели с hyperparameter tuning"""
        logger.info("🎯 Начало обучения XGBoost модели...")
        
        # Расчет scale_pos_weight
        scale_pos_weight = self.calculate_scale_pos_weight(y_train)
        
        # Определение пространства гиперпараметров
        param_space = self.get_hyperparameter_space(scale_pos_weight)
        
        # Базовая модель
        base_model = xgb.XGBClassifier(
            objective='binary:logistic',
            random_state=42,
            n_jobs=-1,
            verbosity=0
        )
        
        # Выбор scorer'а
        if self.precision_focused:
            scoring = self.precision_scorer
            logger.info("📊 Optimization metric: PRECISION")
        else:
            scoring = 'roc_auc'
            logger.info("📊 Optimization metric: ROC-AUC")
        
        # RandomizedSearchCV
        logger.info("🔍 Запуск RandomizedSearchCV...")
        random_search = RandomizedSearchCV(
            estimator=base_model,
            param_distributions=param_space,
            n_iter=50,  # Количество итераций
            scoring=scoring,
            cv=3,  # 3-fold CV
            random_state=42,
            n_jobs=-1,
            verbose=1
        )
        
        # Обучение
        random_search.fit(X_train, y_train)
        
        # Лучшая модель
        self.model = random_search.best_estimator_
        
        # Валидация на validation set
        y_valid_pred = self.model.predict(X_valid)
        y_valid_proba = self.model.predict_proba(X_valid)[:, 1]
        
        # Метрики на validation
        valid_precision = precision_score(y_valid, y_valid_pred)
        valid_recall = recall_score(y_valid, y_valid_pred)
        valid_f1 = f1_score(y_valid, y_valid_pred)
        valid_roc_auc = roc_auc_score(y_valid, y_valid_proba)
        
        # Результаты tuning'а
        tuning_results = {
            'best_params': random_search.best_params_,
            'best_cv_score': float(random_search.best_score_),
            'cv_std': float(random_search.cv_results_['std_test_score'][random_search.best_index_]),
            'validation_metrics': {
                'precision': float(valid_precision),
                'recall': float(valid_recall),
                'f1_score': float(valid_f1),
                'roc_auc': float(valid_roc_auc)
            }
        }
        
        logger.info("✅ Обучение завершено!")
        logger.info(f"🏆 Best CV score: {random_search.best_score_:.4f}")
        logger.info(f"📊 Valid metrics: Precision={valid_precision:.3f}, Recall={valid_recall:.3f}, F1={valid_f1:.3f}")
        
        self.training_history['tuning_results'] = tuning_results
        
        return tuning_results
    
    def evaluate_model(self, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
        """Финальная оценка модели на test set"""
        logger.info("🧪 Оценка модели на test set...")
        
        if self.model is None:
            raise ValueError("Модель не обучена. Вызовите train_model() сначала.")
        
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
        
        # Дополнительные метрики
        specificity = tn / (tn + fp) if (tn + fp) > 0 else 0
        npv = tn / (tn + fn) if (tn + fn) > 0 else 0  # Negative Predictive Value
        
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
                'pr_auc': float(test_pr_auc),
                'specificity': float(specificity),
                'npv': float(npv)
            },
            'business_impact': {
                'false_positive_rate': float(fp / (fp + tn)) if (fp + tn) > 0 else 0,
                'false_negative_rate': float(fn / (fn + tp)) if (fn + tp) > 0 else 0,
                'predicted_positive_rate': float((tp + fp) / len(y_test)),
                'actual_positive_rate': float(y_test.mean())
            }
        }
        
        # Логирование результатов
        logger.info("🎯 ФИНАЛЬНЫЕ РЕЗУЛЬТАТЫ НА TEST SET:")
        logger.info(f"   Precision: {test_precision:.3f}")
        logger.info(f"   Recall: {test_recall:.3f}")
        logger.info(f"   F1-Score: {test_f1:.3f}")
        logger.info(f"   ROC-AUC: {test_roc_auc:.3f}")
        logger.info(f"   PR-AUC: {test_pr_auc:.3f}")
        logger.info(f"📊 Confusion Matrix: TP={tp}, FP={fp}, TN={tn}, FN={fn}")
        logger.info(f"💼 False Positive Rate: {test_results['business_impact']['false_positive_rate']:.1%}")
        
        self.training_history['test_results'] = test_results
        
        return test_results

def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ОБУЧЕНИЯ XGBOOST МОДЕЛИ")
    logger.info("=" * 60)
    
    try:
        # Инициализация тренера
        trainer = XGBoostTrainer(precision_focused=True)
        
        # Загрузка данных
        X_train, X_valid, X_test, y_train, y_valid, y_test = trainer.load_data()
        
        # Скейлинг фичей
        X_train_scaled = trainer.fit_scaler(X_train)
        X_valid_scaled = trainer.transform_features(X_valid)
        X_test_scaled = trainer.transform_features(X_test)
        
        # Обучение модели
        tuning_results = trainer.train_model(X_train_scaled, y_train, X_valid_scaled, y_valid)
        
        # Оценка на test set
        test_results = trainer.evaluate_model(X_test_scaled, y_test)
        
        # Сохранение результатов
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохранение истории обучения
        with open(f'training_history_{timestamp}.json', 'w', encoding='utf-8') as f:
            json.dump(trainer.training_history, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📁 История обучения сохранена: training_history_{timestamp}.json")
        logger.info("✅ ОБУЧЕНИЕ ЗАВЕРШЕНО УСПЕШНО!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
