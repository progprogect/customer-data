"""
User Segmentation Model
Модель сегментации пользователей по поведенческим паттернам
"""

from typing import Dict, List, Optional
import pandas as pd
from sklearn.cluster import KMeans, DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
from .base_model import BaseMLModel


class UserSegmentationModel(BaseMLModel):
    """Модель сегментации пользователей"""
    
    def __init__(self, algorithm: str = "kmeans", n_clusters: int = 5):
        super().__init__("user_segmentation")
        self.algorithm = algorithm
        self.n_clusters = n_clusters
        self.scaler = StandardScaler()
        
    def train(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Dict[str, float]:
        """Обучение модели сегментации"""
        # Нормализация данных
        X_scaled = self.scaler.fit_transform(X)
        
        # Выбор алгоритма
        if self.algorithm == "kmeans":
            self.model = KMeans(n_clusters=self.n_clusters, random_state=42)
        elif self.algorithm == "dbscan":
            self.model = DBSCAN(eps=0.5, min_samples=5)
        else:
            raise ValueError(f"Неподдерживаемый алгоритм: {self.algorithm}")
        
        # Обучение
        clusters = self.model.fit_predict(X_scaled)
        
        # Метрики
        if self.algorithm == "kmeans":
            silhouette_avg = silhouette_score(X_scaled, clusters)
            self.metrics = {
                "silhouette_score": silhouette_avg,
                "n_clusters": self.n_clusters,
                "inertia": self.model.inertia_
            }
        else:
            self.metrics = {
                "n_clusters": len(set(clusters)) - (1 if -1 in clusters else 0),
                "n_noise": list(clusters).count(-1)
            }
        
        self.is_trained = True
        return self.metrics
    
    def predict(self, X: pd.DataFrame) -> List[int]:
        """Предсказание сегментов для новых пользователей"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        X_scaled = self.scaler.transform(X)
        return self.model.predict(X_scaled).tolist()
    
    def get_cluster_centers(self) -> pd.DataFrame:
        """Получение центров кластеров"""
        if not self.is_trained or self.algorithm != "kmeans":
            raise ValueError("Центры кластеров доступны только для KMeans")
        
        return pd.DataFrame(
            self.scaler.inverse_transform(self.model.cluster_centers_),
            columns=[f"feature_{i}" for i in range(self.model.cluster_centers_.shape[1])]
        )

