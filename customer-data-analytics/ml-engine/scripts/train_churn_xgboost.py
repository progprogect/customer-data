#!/usr/bin/env python3
"""
Train XGBoost Churn Prediction Model
Скрипт для обучения XGBoost модели предсказания оттока клиентов

Author: Customer Data Analytics Team
"""

import os
import sys
import psycopg2
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import logging
from datetime import datetime
from pathlib import Path
from sklearn.metrics import (
    roc_auc_score, precision_score, recall_score, f1_score,
    confusion_matrix, classification_report, roc_curve
)
import matplotlib.pyplot as plt
import seaborn as sns

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('churn_xgboost_training.log')
    ]
)
logger = logging.getLogger(__name__)


def load_training_data(conn: psycopg2.extensions.connection) -> tuple:
    """Загрузка тренировочных данных из базы"""
    try:
        logger.info("📊 Загрузка тренировочных данных...")
        
        # Загружаем train данные
        train_query = """
        SELECT 
            recency_days, frequency_90d, monetary_180d, aov_180d,
            orders_lifetime, revenue_lifetime, categories_unique, target
        FROM ml_training_dataset_churn 
        WHERE split_type = 'train'
        ORDER BY user_id, snapshot_date
        """
        
        train_data = pd.read_sql(train_query, conn)
        logger.info(f"✅ Train данных загружено: {len(train_data)} записей")
        
        # Загружаем valid/test данные
        valid_query = """
        SELECT 
            recency_days, frequency_90d, monetary_180d, aov_180d,
            orders_lifetime, revenue_lifetime, categories_unique, target
        FROM ml_training_dataset_churn 
        WHERE split_type = 'valid_test'
        ORDER BY user_id, snapshot_date
        """
        
        valid_data = pd.read_sql(valid_query, conn)
        logger.info(f"✅ Valid/Test данных загружено: {len(valid_data)} записей")
        
        return train_data, valid_data
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки данных: {e}")
        raise


def prepare_features_and_target(data: pd.DataFrame) -> tuple:
    """Подготовка признаков и таргета"""
    try:
        # Признаки (X)
        feature_columns = [
            'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
            'orders_lifetime', 'revenue_lifetime', 'categories_unique'
        ]
        
        X = data[feature_columns].copy()
        
        # Заполняем NULL значения
        X['recency_days'] = X['recency_days'].fillna(999)  # Если не покупал - ставим большое значение
        X['aov_180d'] = X['aov_180d'].fillna(0)  # Если не было заказов - 0
        
        # Таргет (y) - конвертируем boolean в int
        y = data['target'].astype(int)
        
        logger.info(f"📋 Признаки: {list(X.columns)}")
        logger.info(f"📊 Размер X: {X.shape}, размер y: {y.shape}")
        logger.info(f"🎯 Баланс классов: {y.value_counts().to_dict()}")
        
        return X, y
        
    except Exception as e:
        logger.error(f"❌ Ошибка подготовки данных: {e}")
        raise


def get_scale_pos_weight(y_train: pd.Series) -> float:
    """Расчет веса для положительного класса (churn)"""
    churn_count = y_train.sum()
    retention_count = len(y_train) - churn_count
    scale_pos_weight = retention_count / churn_count
    logger.info(f"⚖️ Scale pos weight: {scale_pos_weight:.2f} (retention: {retention_count}, churn: {churn_count})")
    return scale_pos_weight


def train_xgboost_model(X_train: pd.DataFrame, y_train: pd.Series, 
                       X_valid: pd.DataFrame, y_valid: pd.Series) -> xgb.XGBClassifier:
    """Обучение XGBoost модели"""
    try:
        logger.info("🚀 Начало обучения XGBoost модели...")
        
        # Расчет веса для дисбаланса классов
        scale_pos_weight = get_scale_pos_weight(y_train)
        
        # Параметры модели
        params = {
            'objective': 'binary:logistic',
            'eval_metric': 'auc',
            'n_estimators': 300,
            'max_depth': 6,
            'learning_rate': 0.1,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'scale_pos_weight': scale_pos_weight,
            'random_state': 42,
            'n_jobs': -1
        }
        
        logger.info(f"⚙️ Параметры модели: {params}")
        
        # Создание и обучение модели
        model = xgb.XGBClassifier(**params)
        
        # Обучение модели
        model.fit(X_train, y_train)
        
        logger.info("✅ Модель обучена!")
        
        return model
        
    except Exception as e:
        logger.error(f"❌ Ошибка обучения модели: {e}")
        raise


def evaluate_model(model: xgb.XGBClassifier, X_valid: pd.DataFrame, y_valid: pd.Series) -> dict:
    """Оценка качества модели"""
    try:
        logger.info("📊 Оценка качества модели...")
        
        # Предсказания
        y_pred_proba = model.predict_proba(X_valid)[:, 1]
        y_pred = model.predict(X_valid)
        
        # Метрики
        auc = roc_auc_score(y_valid, y_pred_proba)
        precision = precision_score(y_valid, y_pred)
        recall = recall_score(y_valid, y_pred)
        f1 = f1_score(y_valid, y_pred)
        
        # Confusion Matrix
        cm = confusion_matrix(y_valid, y_pred)
        
        metrics = {
            'auc': auc,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'confusion_matrix': cm,
            'y_pred_proba': y_pred_proba,
            'y_pred': y_pred
        }
        
        logger.info("📈 Результаты оценки:")
        logger.info(f"   AUC-ROC: {auc:.4f}")
        logger.info(f"   Precision: {precision:.4f}")
        logger.info(f"   Recall: {recall:.4f}")
        logger.info(f"   F1-Score: {f1:.4f}")
        logger.info(f"   Confusion Matrix:\n{cm}")
        
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Ошибка оценки модели: {e}")
        raise


def get_feature_importance(model: xgb.XGBClassifier, feature_names: list) -> pd.DataFrame:
    """Получение важности признаков"""
    try:
        importance_scores = model.feature_importances_
        
        importance_df = pd.DataFrame({
            'feature': feature_names,
            'importance': importance_scores
        }).sort_values('importance', ascending=False)
        
        logger.info("🔍 Важность признаков:")
        for _, row in importance_df.iterrows():
            logger.info(f"   {row['feature']}: {row['importance']:.4f}")
        
        return importance_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения важности признаков: {e}")
        raise


def save_model_and_report(model: xgb.XGBClassifier, metrics: dict, 
                         feature_importance: pd.DataFrame, 
                         output_dir: Path) -> None:
    """Сохранение модели и отчета"""
    try:
        logger.info("💾 Сохранение модели и отчета...")
        
        # Создаем директорию для результатов
        output_dir.mkdir(exist_ok=True)
        
        # Сохраняем модель
        model_path = output_dir / 'churn_xgboost_model.pkl'
        joblib.dump(model, model_path)
        logger.info(f"✅ Модель сохранена: {model_path}")
        
        # Создаем отчет
        report_path = output_dir / 'churn_model_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("CHURN PREDICTION MODEL REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Модель: XGBoost Classifier\n\n")
            
            f.write("МЕТРИКИ КАЧЕСТВА:\n")
            f.write(f"AUC-ROC: {metrics['auc']:.4f}\n")
            f.write(f"Precision: {metrics['precision']:.4f}\n")
            f.write(f"Recall: {metrics['recall']:.4f}\n")
            f.write(f"F1-Score: {metrics['f1']:.4f}\n\n")
            
            f.write("CONFUSION MATRIX:\n")
            f.write(f"{metrics['confusion_matrix']}\n\n")
            
            f.write("ВАЖНОСТЬ ПРИЗНАКОВ:\n")
            for _, row in feature_importance.iterrows():
                f.write(f"{row['feature']}: {row['importance']:.4f}\n")
        
        logger.info(f"✅ Отчет сохранен: {report_path}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения: {e}")
        raise


def main():
    """Главная функция"""
    logger.info("🚀 ОБУЧЕНИЕ XGBOOST МОДЕЛИ CHURN PREDICTION")
    logger.info("=" * 60)
    
    conn = None
    try:
        # Подключение к БД
        logger.info("🔌 Подключение к базе данных...")
        conn = psycopg2.connect(
            host="localhost",
            database="customer_data",
            user="mikitavalkunovich",
            port="5432"
        )
        logger.info("✅ Подключение установлено")
        
        # Загрузка данных
        train_data, valid_data = load_training_data(conn)
        
        # Подготовка признаков и таргета
        X_train, y_train = prepare_features_and_target(train_data)
        X_valid, y_valid = prepare_features_and_target(valid_data)
        
        # Обучение модели
        model = train_xgboost_model(X_train, y_train, X_valid, y_valid)
        
        # Оценка качества
        metrics = evaluate_model(model, X_valid, y_valid)
        
        # Важность признаков
        feature_importance = get_feature_importance(model, list(X_train.columns))
        
        # Сохранение результатов
        output_dir = Path(__file__).parent.parent / 'models'
        save_model_and_report(model, metrics, feature_importance, output_dir)
        
        # Финальная оценка
        logger.info("🎯 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:")
        logger.info("=" * 40)
        logger.info(f"📊 AUC-ROC: {metrics['auc']:.4f}")
        logger.info(f"🎯 Precision: {metrics['precision']:.4f}")
        logger.info(f"🔄 Recall: {metrics['recall']:.4f}")
        logger.info(f"⚖️ F1-Score: {metrics['f1']:.4f}")
        logger.info(f"🔍 Топ-3 признака:")
        for i, (_, row) in enumerate(feature_importance.head(3).iterrows()):
            logger.info(f"   {i+1}. {row['feature']}: {row['importance']:.4f}")
        
        # Проверяем качество
        if metrics['auc'] >= 0.7:
            logger.info("✅ Отличное качество модели (AUC >= 0.7)")
        elif metrics['auc'] >= 0.6:
            logger.info("✅ Хорошее качество модели (AUC >= 0.6)")
        else:
            logger.warning("⚠️ Модель требует улучшения (AUC < 0.6)")
        
        logger.info("🎉 ОБУЧЕНИЕ XGBOOST МОДЕЛИ ЗАВЕРШЕНО УСПЕШНО!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False
        
    finally:
        if conn:
            conn.close()
            logger.info("🔌 Соединение с БД закрыто")


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
