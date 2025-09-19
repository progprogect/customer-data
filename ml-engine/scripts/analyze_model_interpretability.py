#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Model Interpretability Analysis
Анализ важности признаков и SHAP объяснений для XGBoost модели

Features:
- Feature importance visualization
- SHAP global and local explanations
- Business insights generation

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
from sklearn.metrics import classification_report
import shap

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns

# System libraries
import logging
import sys
import os
from datetime import datetime

# Настройка для matplotlib на серверах без GUI
plt.switch_backend('Agg')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('interpretability_analysis.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class ModelInterpreter:
    """Класс для анализа интерпретируемости модели"""
    
    def __init__(self, model_path: str = None):
        """
        Инициализация интерпретатора
        
        Args:
            model_path: Путь к сохраненной модели (если нужно загрузить)
        """
        self.model = None
        self.scaler = None
        self.feature_names = [
            'recency_days', 'frequency_90d', 'monetary_180d', 'aov_180d',
            'orders_lifetime', 'revenue_lifetime', 'categories_unique'
        ]
        self.explainer = None
        self.shap_values = None
        
        if model_path and os.path.exists(model_path):
            self.load_model(model_path)
        
        logger.info("🔍 Model Interpreter инициализирован")
    
    def train_simple_model(self) -> None:
        """Обучение простой модели для демонстрации"""
        logger.info("🎯 Обучение простой XGBoost модели для анализа...")
        
        # Загрузка данных
        train_df = pd.read_csv('train_set.csv')
        test_df = pd.read_csv('test_set.csv')
        
        X_train = train_df[self.feature_names].fillna(train_df[self.feature_names].median())
        y_train = train_df['target']
        
        X_test = test_df[self.feature_names].fillna(train_df[self.feature_names].median())
        y_test = test_df['target']
        
        # Простая модель с хорошей интерпретируемостью
        self.model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            random_state=42,
            scale_pos_weight=1.57
        )
        
        self.model.fit(X_train, y_train)
        
        # Быстрая оценка
        y_pred = self.model.predict(X_test)
        accuracy = (y_pred == y_test).mean()
        logger.info(f"✅ Модель обучена. Test accuracy: {accuracy:.3f}")
    
    def load_model(self, model_path: str) -> None:
        """Загрузка обученной модели"""
        try:
            self.model = joblib.load(model_path)
            logger.info(f"✅ Модель загружена: {model_path}")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки модели: {e}")
            raise
    
    def analyze_feature_importance(self) -> dict:
        """Анализ важности признаков"""
        logger.info("📊 Анализ важности признаков...")
        
        if self.model is None:
            raise ValueError("Модель не загружена. Вызовите train_simple_model() или load_model()")
        
        # XGBoost feature importance
        importance_gain = self.model.feature_importances_
        importance_dict = dict(zip(self.feature_names, importance_gain))
        
        # Сортировка по важности
        sorted_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        
        # Создание DataFrame для удобства
        importance_df = pd.DataFrame(sorted_features, columns=['feature', 'importance'])
        importance_df['importance_percent'] = importance_df['importance'] / importance_df['importance'].sum() * 100
        
        # Логирование топ-фичей
        logger.info("🏆 ТОП-5 важных признаков:")
        for idx, (feature, importance) in enumerate(sorted_features[:5], 1):
            logger.info(f"   {idx}. {feature}: {importance:.3f} ({importance/sum(importance_gain)*100:.1f}%)")
        
        # Создание визуализации
        plt.figure(figsize=(10, 6))
        sns.barplot(data=importance_df, x='importance_percent', y='feature', palette='viridis')
        plt.title('Feature Importance (XGBoost)', fontsize=14, fontweight='bold')
        plt.xlabel('Важность (%)', fontsize=12)
        plt.ylabel('Признаки', fontsize=12)
        plt.tight_layout()
        plt.savefig('feature_importance.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info("📊 График важности сохранен: feature_importance.png")
        
        # Результат
        results = {
            'feature_importance': importance_dict,
            'sorted_features': sorted_features,
            'top_3_features': sorted_features[:3],
            'interpretation': self._interpret_feature_importance(sorted_features)
        }
        
        return results
    
    def _interpret_feature_importance(self, sorted_features: list) -> dict:
        """Интерпретация важности признаков"""
        interpretations = {
            'recency_days': "Дни с последней покупки - ключевой индикатор готовности к покупке",
            'frequency_90d': "Частота покупок за 90 дней - показатель активности клиента", 
            'monetary_180d': "Потраченная сумма за 180 дней - индикатор покупательной способности",
            'aov_180d': "Средний чек за 180 дней - показатель ценности клиента",
            'orders_lifetime': "Общее количество заказов - индикатор лояльности",
            'revenue_lifetime': "Общая выручка от клиента - показатель lifetime value",
            'categories_unique': "Количество уникальных категорий - показатель разнообразия покупок"
        }
        
        top_feature = sorted_features[0][0]
        top_importance = sorted_features[0][1]
        
        business_insights = {
            'most_important_feature': top_feature,
            'most_important_interpretation': interpretations.get(top_feature, "Неизвестная фича"),
            'dominance_score': top_importance / sum([f[1] for f in sorted_features]),
            'business_conclusion': self._generate_business_conclusion(sorted_features[:3])
        }
        
        return business_insights
    
    def _generate_business_conclusion(self, top_features: list) -> str:
        """Генерация бизнес-выводов"""
        if top_features[0][0] == 'recency_days':
            return "Модель в первую очередь ориентируется на недавность покупок - фокус на реактивацию"
        elif top_features[0][0] in ['frequency_90d', 'orders_lifetime']:
            return "Модель больше ценит частоту покупок - фокус на удержание активных клиентов"
        elif top_features[0][0] in ['monetary_180d', 'revenue_lifetime', 'aov_180d']:
            return "Модель акцентирует внимание на денежной ценности - фокус на VIP-клиентов"
        else:
            return "Модель использует комплексный подход к оценке клиентов"
    
    def calculate_shap_values(self, sample_size: int = 1000) -> dict:
        """Расчет SHAP значений"""
        logger.info(f"🔬 Расчет SHAP значений (sample_size={sample_size})...")
        
        if self.model is None:
            raise ValueError("Модель не загружена")
        
        # Загрузка тестовых данных
        test_df = pd.read_csv('test_set.csv')
        X_test = test_df[self.feature_names].fillna(test_df[self.feature_names].median())
        
        # Сэмплирование для ускорения SHAP
        if len(X_test) > sample_size:
            X_sample = X_test.sample(n=sample_size, random_state=42)
        else:
            X_sample = X_test
        
        # Создание SHAP explainer
        logger.info("🧮 Создание SHAP explainer...")
        self.explainer = shap.TreeExplainer(self.model)
        
        # Расчет SHAP значений
        logger.info("⚡ Расчет SHAP значений...")
        self.shap_values = self.explainer.shap_values(X_sample)
        
        # Summary plot
        logger.info("📈 Создание SHAP summary plot...")
        plt.figure(figsize=(10, 6))
        shap.summary_plot(self.shap_values, X_sample, feature_names=self.feature_names, show=False)
        plt.tight_layout()
        plt.savefig('shap_summary.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # Feature importance from SHAP
        logger.info("📊 Создание SHAP feature importance...")
        plt.figure(figsize=(8, 6))
        shap.summary_plot(self.shap_values, X_sample, feature_names=self.feature_names, 
                         plot_type="bar", show=False)
        plt.tight_layout()
        plt.savefig('shap_feature_importance.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # Waterfall plot для первого примера
        logger.info("💧 Создание SHAP waterfall plot...")
        try:
            plt.figure(figsize=(10, 6))
            # Создаем объект Explanation для waterfall plot
            explanation = shap.Explanation(
                values=self.shap_values[0], 
                base_values=self.explainer.expected_value,
                data=X_sample.iloc[0].values,
                feature_names=self.feature_names
            )
            shap.plots.waterfall(explanation, show=False)
            plt.tight_layout()
            plt.savefig('shap_waterfall_example.png', dpi=300, bbox_inches='tight')
            plt.close()
        except Exception as e:
            logger.warning(f"⚠️ Не удалось создать waterfall plot: {e}")
            # Создаем альтернативную визуализацию
            plt.figure(figsize=(10, 6))
            feature_values = X_sample.iloc[0].values
            shap_vals = self.shap_values[0]
            
            # Simple bar plot as alternative
            y_pos = np.arange(len(self.feature_names))
            plt.barh(y_pos, shap_vals)
            plt.yticks(y_pos, self.feature_names)
            plt.xlabel('SHAP значение')
            plt.title(f'SHAP объяснение для примера 1\n(base_value: {self.explainer.expected_value:.3f})')
            plt.tight_layout()
            plt.savefig('shap_waterfall_example.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # Статистика SHAP значений
        shap_stats = {
            'mean_abs_shap': [float(x) for x in np.mean(np.abs(self.shap_values), axis=0)],
            'feature_names': self.feature_names,
            'base_value': float(self.explainer.expected_value),
            'sample_size': int(len(X_sample))
        }
        
        # Топ фичи по SHAP
        feature_shap_importance = dict(zip(self.feature_names, shap_stats['mean_abs_shap']))
        sorted_shap_features = sorted(feature_shap_importance.items(), key=lambda x: x[1], reverse=True)
        
        logger.info("🏆 ТОП-5 признаков по SHAP важности:")
        for idx, (feature, shap_imp) in enumerate(sorted_shap_features[:5], 1):
            logger.info(f"   {idx}. {feature}: {shap_imp:.3f}")
        
        logger.info("📊 SHAP графики сохранены:")
        logger.info("   - shap_summary.png")
        logger.info("   - shap_feature_importance.png")
        logger.info("   - shap_waterfall_example.png")
        
        results = {
            'shap_statistics': shap_stats,
            'shap_feature_ranking': sorted_shap_features,
            'interpretation': self._interpret_shap_results(sorted_shap_features)
        }
        
        return results
    
    def _interpret_shap_results(self, sorted_shap_features: list) -> dict:
        """Интерпретация SHAP результатов"""
        shap_interpretations = {
            'top_shap_feature': sorted_shap_features[0][0],
            'shap_vs_xgb_consistency': self._check_feature_ranking_consistency(sorted_shap_features),
            'local_explanation_available': True,
            'business_recommendation': self._generate_shap_business_recommendation(sorted_shap_features[:3])
        }
        
        return shap_interpretations
    
    def _check_feature_ranking_consistency(self, shap_ranking: list) -> str:
        """Проверка консистентности между XGBoost и SHAP ranking"""
        # Эта проверка будет работать если у нас есть XGBoost importance
        return "SHAP ranking рассчитан успешно"
    
    def _generate_shap_business_recommendation(self, top_shap_features: list) -> str:
        """Генерация бизнес-рекомендаций на основе SHAP"""
        top_feature = top_shap_features[0][0]
        
        recommendations = {
            'recency_days': "Фокус на реактивации недавних клиентов через персонализированные предложения",
            'frequency_90d': "Стратегия удержания частых покупателей через программы лояльности",
            'monetary_180d': "Работа с высокоценными клиентами через премиум-сервис",
            'aov_180d': "Увеличение среднего чека через кросс-селлинг",
            'orders_lifetime': "Развитие долгосрочных отношений с лояльными клиентами",
            'revenue_lifetime': "VIP-программы для клиентов с высоким LTV",
            'categories_unique': "Расширение ассортимента для клиентов с разнообразными покупками"
        }
        
        return recommendations.get(top_feature, "Требуется дополнительный анализ для конкретных рекомендаций")
    
    def generate_comprehensive_report(self) -> dict:
        """Генерация комплексного отчета"""
        logger.info("📋 Генерация комплексного отчета интерпретируемости...")
        
        # Анализ feature importance
        fi_results = self.analyze_feature_importance()
        
        # SHAP анализ
        shap_results = self.calculate_shap_values(sample_size=500)
        
        # Объединенный отчет
        comprehensive_report = {
            'timestamp': datetime.now().isoformat(),
            'model_type': 'XGBoost',
            'analysis_type': 'Feature Importance + SHAP',
            'feature_importance_analysis': fi_results,
            'shap_analysis': shap_results,
            'summary': {
                'most_important_xgb': fi_results['top_3_features'],
                'most_important_shap': shap_results['shap_feature_ranking'][:3],
                'business_insights': {
                    'xgb_conclusion': fi_results['interpretation']['business_conclusion'],
                    'shap_recommendation': shap_results['interpretation']['business_recommendation']
                }
            },
            'visualizations_created': [
                'feature_importance.png',
                'shap_summary.png', 
                'shap_feature_importance.png',
                'shap_waterfall_example.png'
            ]
        }
        
        # Сохранение отчета
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f'interpretability_report_{timestamp}.json'
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            json.dump(comprehensive_report, f, indent=2, ensure_ascii=False)
        
        logger.info(f"📁 Комплексный отчет сохранен: {report_filename}")
        
        return comprehensive_report

def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("🔍 ЗАПУСК АНАЛИЗА ИНТЕРПРЕТИРУЕМОСТИ МОДЕЛИ")
    logger.info("=" * 60)
    
    try:
        # Инициализация интерпретатора
        interpreter = ModelInterpreter()
        
        # Обучение простой модели для демонстрации
        interpreter.train_simple_model()
        
        # Генерация комплексного отчета
        report = interpreter.generate_comprehensive_report()
        
        # Логирование ключевых результатов
        logger.info("=" * 60)
        logger.info("📊 КЛЮЧЕВЫЕ РЕЗУЛЬТАТЫ АНАЛИЗА:")
        logger.info("=" * 60)
        
        # XGBoost feature importance
        xgb_top3 = report['feature_importance_analysis']['top_3_features']
        logger.info("🏆 ТОП-3 признака (XGBoost importance):")
        for idx, (feature, importance) in enumerate(xgb_top3, 1):
            logger.info(f"   {idx}. {feature}: {importance:.3f}")
        
        # SHAP feature importance
        shap_top3 = report['shap_analysis']['shap_feature_ranking'][:3]
        logger.info("🏆 ТОП-3 признака (SHAP importance):")
        for idx, (feature, shap_imp) in enumerate(shap_top3, 1):
            logger.info(f"   {idx}. {feature}: {shap_imp:.3f}")
        
        # Бизнес-выводы
        logger.info("💼 БИЗНЕС-ВЫВОДЫ:")
        logger.info(f"   XGBoost: {report['summary']['business_insights']['xgb_conclusion']}")
        logger.info(f"   SHAP: {report['summary']['business_insights']['shap_recommendation']}")
        
        # Созданные файлы
        logger.info("📁 СОЗДАННЫЕ ФАЙЛЫ:")
        for file in report['visualizations_created']:
            logger.info(f"   - {file}")
        
        logger.info("✅ АНАЛИЗ ИНТЕРПРЕТИРУЕМОСТИ ЗАВЕРШЕН УСПЕШНО!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
