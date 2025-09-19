#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Select optimal number of clusters (k) for K-means clustering
Author: Customer Data Analytics Team
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import logging
from typing import Tuple, List, Dict
import sys
import os

# Добавляем путь к модулям
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from prepare_data_for_clustering import main as prepare_data

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_optimal_k(X_scaled: np.ndarray, k_range: range = range(2, 11)) -> Tuple[List[float], List[float], Dict]:
    """
    Поиск оптимального количества кластеров k
    
    Args:
        X_scaled: Масштабированные данные
        k_range: Диапазон значений k для тестирования
        
    Returns:
        Tuple[List[float], List[float], Dict]: (inertias, silhouettes, results_dict)
    """
    inertias = []
    silhouettes = []
    results = {}
    
    logger.info(f"Тестирование K-means для k = {k_range.start}...{k_range.stop-1}")
    
    for k in k_range:
        logger.info(f"Обработка k = {k}")
        
        # Обучение K-means
        kmeans = KMeans(
            n_clusters=k, 
            random_state=42, 
            n_init=10, 
            max_iter=300
        )
        
        # Предсказание кластеров
        labels = kmeans.fit_predict(X_scaled)
        
        # Расчет метрик
        inertia = kmeans.inertia_
        silhouette = silhouette_score(X_scaled, labels)
        
        inertias.append(inertia)
        silhouettes.append(silhouette)
        
        # Сохранение результатов
        results[k] = {
            'inertia': inertia,
            'silhouette': silhouette,
            'labels': labels,
            'kmeans': kmeans
        }
        
        logger.info(f"k = {k}: inertia = {inertia:.2f}, silhouette = {silhouette:.4f}")
    
    return inertias, silhouettes, results

def plot_elbow_and_silhouette(k_range: range, inertias: List[float], silhouettes: List[float], 
                             save_path: str = None) -> None:
    """
    Построение графиков метода локтя и silhouette score
    
    Args:
        k_range: Диапазон значений k
        inertias: Список значений inertia
        silhouettes: Список значений silhouette score
        save_path: Путь для сохранения графиков
    """
    plt.figure(figsize=(15, 6))
    
    # График метода локтя
    plt.subplot(1, 2, 1)
    plt.plot(k_range, inertias, 'bo-', linewidth=2, markersize=8)
    plt.xlabel('k (число кластеров)', fontsize=12)
    plt.ylabel('Inertia (сумма квадратов расстояний)', fontsize=12)
    plt.title('Метод локтя (Elbow Method)', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    
    # Добавление значений на точки
    for i, (k, inertia) in enumerate(zip(k_range, inertias)):
        plt.annotate(f'{inertia:.0f}', (k, inertia), textcoords="offset points", 
                    xytext=(0,10), ha='center', fontsize=10)
    
    # График silhouette score
    plt.subplot(1, 2, 2)
    plt.plot(k_range, silhouettes, 'ro-', linewidth=2, markersize=8)
    plt.xlabel('k (число кластеров)', fontsize=12)
    plt.ylabel('Silhouette Score', fontsize=12)
    plt.title('Метод силуэта (Silhouette Method)', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    
    # Добавление значений на точки
    for i, (k, silhouette) in enumerate(zip(k_range, silhouettes)):
        plt.annotate(f'{silhouette:.3f}', (k, silhouette), textcoords="offset points", 
                    xytext=(0,10), ha='center', fontsize=10)
    
    plt.tight_layout()
    
    # Сохранение графика
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        logger.info(f"Графики сохранены в {save_path}")
    
    plt.show()

def analyze_optimal_k(k_range: range, inertias: List[float], silhouettes: List[float]) -> int:
    """
    Анализ результатов и выбор оптимального k
    
    Args:
        k_range: Диапазон значений k
        inertias: Список значений inertia
        silhouettes: Список значений silhouette score
        
    Returns:
        int: Оптимальное значение k
    """
    print("\n" + "="*70)
    print("АНАЛИЗ ОПТИМАЛЬНОГО КОЛИЧЕСТВА КЛАСТЕРОВ")
    print("="*70)
    
    # Поиск локтя (максимальное изменение наклона)
    inertia_diffs = np.diff(inertias)
    inertia_diff2 = np.diff(inertia_diffs)
    elbow_k = k_range[np.argmax(inertia_diff2) + 1]
    
    # Поиск максимального silhouette score
    best_silhouette_k = k_range[np.argmax(silhouettes)]
    best_silhouette_score = max(silhouettes)
    
    print(f"Метод локтя (Elbow): k = {elbow_k}")
    print(f"Максимальный silhouette score: k = {best_silhouette_k} (score = {best_silhouette_score:.4f})")
    
    # Рекомендация
    print("\nРекомендации:")
    print("-" * 50)
    
    if abs(elbow_k - best_silhouette_k) <= 1:
        optimal_k = best_silhouette_k
        print(f"✅ Оба метода согласны: рекомендуем k = {optimal_k}")
    else:
        # Выбираем k с лучшим silhouette score, но в разумных пределах
        optimal_k = best_silhouette_k
        print(f"⚠️  Методы расходятся: выбираем k = {optimal_k} (лучший silhouette)")
        print(f"   Альтернатива: k = {elbow_k} (метод локтя)")
    
    # Дополнительный анализ
    print(f"\nДетальный анализ для k = {optimal_k}:")
    print("-" * 50)
    
    k_idx = list(k_range).index(optimal_k)
    print(f"Inertia: {inertias[k_idx]:.2f}")
    print(f"Silhouette score: {silhouettes[k_idx]:.4f}")
    
    # Интерпретация silhouette score
    silhouette_score = silhouettes[k_idx]
    if silhouette_score > 0.7:
        interpretation = "Отличная кластеризация"
    elif silhouette_score > 0.5:
        interpretation = "Хорошая кластеризация"
    elif silhouette_score > 0.3:
        interpretation = "Умеренная кластеризация"
    else:
        interpretation = "Слабая кластеризация"
    
    print(f"Качество кластеризации: {interpretation}")
    
    return optimal_k

def print_k_analysis_table(k_range: range, inertias: List[float], silhouettes: List[float]) -> None:
    """
    Вывод таблицы с анализом всех значений k
    
    Args:
        k_range: Диапазон значений k
        inertias: Список значений inertia
        silhouettes: Список значений silhouette score
    """
    print("\n" + "="*70)
    print("ТАБЛИЦА АНАЛИЗА ВСЕХ ЗНАЧЕНИЙ k")
    print("="*70)
    
    df = pd.DataFrame({
        'k': list(k_range),
        'Inertia': inertias,
        'Silhouette': silhouettes,
        'Inertia_diff': [0] + list(np.diff(inertias)),
        'Silhouette_rank': [len(silhouettes) - i for i in range(len(silhouettes))]
    })
    
    # Сортировка по silhouette score
    df_sorted = df.sort_values('Silhouette', ascending=False)
    
    print(df_sorted.to_string(index=False, float_format='%.4f'))
    
    print(f"\nТоп-3 по silhouette score:")
    print("-" * 30)
    for i, row in df_sorted.head(3).iterrows():
        print(f"k = {int(row['k']):2d}: silhouette = {row['Silhouette']:.4f}, inertia = {row['Inertia']:.2f}")

def main():
    """
    Основная функция для подбора оптимального k
    """
    logger.info("Начало подбора оптимального количества кластеров")
    
    # Подготовка данных
    X_scaled, user_ids, scaler = prepare_data()
    
    # Диапазон значений k для тестирования
    k_range = range(2, 11)
    
    # Поиск оптимального k
    inertias, silhouettes, results = find_optimal_k(X_scaled, k_range)
    
    # Построение графиков
    plot_elbow_and_silhouette(
        k_range, inertias, silhouettes, 
        save_path='ml-engine/results/k_selection_plots.png'
    )
    
    # Анализ результатов
    optimal_k = analyze_optimal_k(k_range, inertias, silhouettes)
    
    # Вывод таблицы анализа
    print_k_analysis_table(k_range, inertias, silhouettes)
    
    # Сохранение результатов
    results_summary = {
        'optimal_k': optimal_k,
        'k_range': list(k_range),
        'inertias': inertias,
        'silhouettes': silhouettes,
        'best_silhouette_k': k_range[np.argmax(silhouettes)],
        'best_silhouette_score': max(silhouettes)
    }
    
    # Сохранение в файл
    import json
    with open('ml-engine/results/k_selection_results.json', 'w') as f:
        json.dump(results_summary, f, indent=2)
    
    logger.info(f"Оптимальное количество кластеров: k = {optimal_k}")
    logger.info("Результаты сохранены в ml-engine/results/")
    
    return optimal_k, results[optimal_k]

if __name__ == "__main__":
    optimal_k, best_result = main()
