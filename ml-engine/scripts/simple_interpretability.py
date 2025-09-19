#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple Model Interpretability Analysis
Упрощенный анализ важности признаков для XGBoost модели

Author: Customer Data Analytics Team
"""

import pandas as pd
import numpy as np
import json
import warnings
warnings.filterwarnings('ignore')

# ML libraries
import xgboost as xgb
import shap

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# System libraries
import logging
from datetime import datetime

# Настройка для matplotlib
plt.switch_backend('Agg')

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_float(value):
    """Безопасное преобразование в float для JSON"""
    if isinstance(value, (np.float32, np.float64)):
        return float(value)
    elif isinstance(value, (np.int32, np.int64)):
        return int(value)
    else:
        return value

def main():
    """Главная функция"""
    logger.info("🔍 Простой анализ интерпретируемости XGBoost модели")
    
    try:
        # Загрузка данных
        train_df = pd.read_csv('train_set.csv')
        test_df = pd.read_csv('test_set.csv')
        
        feature_names = [
            'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
            'orders_lifetime', 'revenue_lifetime', 'categories_unique'
        ]
        
        # Подготовка данных
        X_train = train_df[feature_names].fillna(train_df[feature_names].median())
        y_train = train_df['target']
        
        X_test = test_df[feature_names].fillna(train_df[feature_names].median())
        y_test = test_df['target']
        
        logger.info(f"✅ Данные загружены: train={len(X_train)}, test={len(X_test)}")
        
        # Обучение модели
        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            random_state=42,
            scale_pos_weight=1.57
        )
        
        model.fit(X_train, y_train)
        
        # Оценка модели
        test_accuracy = model.score(X_test, y_test)
        logger.info(f"✅ Модель обучена. Test accuracy: {test_accuracy:.3f}")
        
        # === FEATURE IMPORTANCE ANALYSIS ===
        
        # XGBoost feature importance
        importance_scores = model.feature_importances_
        importance_dict = {}
        
        for feature, score in zip(feature_names, importance_scores):
            importance_dict[feature] = safe_float(score)
        
        # Сортировка по важности
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        
        logger.info("🏆 ТОП-5 важных признаков (XGBoost):")
        for idx, (feature, importance) in enumerate(sorted_features[:5], 1):
            percentage = importance / sum(importance_dict.values()) * 100
            logger.info(f"   {idx}. {feature}: {importance:.3f} ({percentage:.1f}%)")
        
        # Создание графика важности
        plt.figure(figsize=(10, 6))
        features = [f[0] for f in sorted_features]
        importances = [f[1] for f in sorted_features]
        
        sns.barplot(x=importances, y=features, palette='viridis')
        plt.title('Feature Importance (XGBoost)', fontsize=14, fontweight='bold')
        plt.xlabel('Важность', fontsize=12)
        plt.ylabel('Признаки', fontsize=12)
        plt.tight_layout()
        plt.savefig('feature_importance_simple.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info("📊 График важности сохранен: feature_importance_simple.png")
        
        # === SHAP ANALYSIS ===
        
        logger.info("🔬 Запуск SHAP анализа...")
        
        # Сэмплирование для SHAP
        X_sample = X_test.sample(n=min(300, len(X_test)), random_state=42)
        
        # SHAP explainer
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_sample)
        
        # SHAP feature importance (среднее абсолютное значение)
        shap_importance = {}
        for idx, feature in enumerate(feature_names):
            mean_abs_shap = np.mean(np.abs(shap_values[:, idx]))
            shap_importance[feature] = safe_float(mean_abs_shap)
        
        # Сортировка SHAP важности
        sorted_shap = sorted(shap_importance.items(), key=lambda x: x[1], reverse=True)
        
        logger.info("🏆 ТОП-5 признаков (SHAP):")
        for idx, (feature, shap_imp) in enumerate(sorted_shap[:5], 1):
            logger.info(f"   {idx}. {feature}: {shap_imp:.3f}")
        
        # SHAP summary plot
        plt.figure(figsize=(10, 6))
        shap.summary_plot(shap_values, X_sample, feature_names=feature_names, 
                         plot_type="bar", show=False)
        plt.tight_layout()
        plt.savefig('shap_importance_simple.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # SHAP detailed plot
        plt.figure(figsize=(10, 6))
        shap.summary_plot(shap_values, X_sample, feature_names=feature_names, show=False)
        plt.tight_layout()
        plt.savefig('shap_summary_simple.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info("📊 SHAP графики сохранены:")
        logger.info("   - shap_importance_simple.png")
        logger.info("   - shap_summary_simple.png")
        
        # === BUSINESS INSIGHTS ===
        
        # Анализ топ-3 признаков
        top_xgb = sorted_features[:3]
        top_shap = sorted_shap[:3]
        
        business_insights = {
            'xgb_top_feature': top_xgb[0][0],
            'shap_top_feature': top_shap[0][0],
            'consistency_check': top_xgb[0][0] == top_shap[0][0],
            'interpretation': {}
        }
        
        # Интерпретации признаков
        interpretations = {
            'recency_days': "Дни с последней покупки - ключевой индикатор готовности к покупке",
            'frequency_90d': "Частота покупок за 90 дней - показатель активности клиента", 
            'monetary_180d': "Потраченная сумма за 180 дней - индикатор покупательной способности",
            'aov_180d': "Средний чек за 180 дней - показатель ценности клиента",
            'orders_lifetime': "Общее количество заказов - индикатор лояльности",
            'revenue_lifetime': "Общая выручка от клиента - показатель lifetime value",
            'categories_unique': "Количество уникальных категорий - показатель разнообразия покупок"
        }
        
        # Бизнес-рекомендации
        if top_xgb[0][0] == 'orders_lifetime':
            business_conclusion = "Модель фокусируется на лояльности клиентов - развивайте программы удержания"
        elif top_xgb[0][0] == 'categories_unique':
            business_conclusion = "Модель ценит разнообразие покупок - развивайте кросс-селлинг"
        elif top_xgb[0][0] == 'recency_days':
            business_conclusion = "Модель ориентируется на недавность покупок - фокус на реактивацию"
        else:
            business_conclusion = "Модель использует комплексный подход к оценке клиентов"
        
        # === СОХРАНЕНИЕ РЕЗУЛЬТАТОВ ===
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'model_performance': {
                'test_accuracy': safe_float(test_accuracy),
                'total_samples': safe_float(len(X_test))
            },
            'xgboost_feature_importance': {
                'ranking': [(f, safe_float(s)) for f, s in sorted_features],
                'top_3': [(f, safe_float(s)) for f, s in sorted_features[:3]]
            },
            'shap_analysis': {
                'ranking': [(f, safe_float(s)) for f, s in sorted_shap],
                'top_3': [(f, safe_float(s)) for f, s in sorted_shap[:3]],
                'sample_size': len(X_sample),
                'base_value': safe_float(explainer.expected_value)
            },
            'business_insights': {
                'xgb_top_feature': business_insights['xgb_top_feature'],
                'shap_top_feature': business_insights['shap_top_feature'],
                'methods_consistent': business_insights['consistency_check'],
                'business_conclusion': business_conclusion,
                'feature_interpretations': interpretations
            },
            'visualizations': [
                'feature_importance_simple.png',
                'shap_importance_simple.png',
                'shap_summary_simple.png'
            ]
        }
        
        # Сохранение JSON отчета
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f'interpretability_report_simple_{timestamp}.json'
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # === ФИНАЛЬНЫЙ ОТЧЕТ ===
        
        logger.info("=" * 60)
        logger.info("📊 РЕЗУЛЬТАТЫ АНАЛИЗА ИНТЕРПРЕТИРУЕМОСТИ")
        logger.info("=" * 60)
        
        logger.info("🏆 ТОП-3 признака (XGBoost):")
        for idx, (feature, importance) in enumerate(top_xgb, 1):
            logger.info(f"   {idx}. {feature}: {importance:.3f}")
        
        logger.info("🏆 ТОП-3 признака (SHAP):")
        for idx, (feature, shap_imp) in enumerate(top_shap, 1):
            logger.info(f"   {idx}. {feature}: {shap_imp:.3f}")
        
        logger.info(f"🤝 Методы согласованы: {'ДА' if business_insights['consistency_check'] else 'НЕТ'}")
        logger.info(f"💼 Бизнес-вывод: {business_conclusion}")
        
        logger.info("📁 Созданные файлы:")
        for viz in report['visualizations']:
            logger.info(f"   - {viz}")
        logger.info(f"   - {report_filename}")
        
        logger.info("✅ АНАЛИЗ ИНТЕРПРЕТИРУЕМОСТИ ЗАВЕРШЕН УСПЕШНО!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
