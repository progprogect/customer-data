"""
Recommendation Model
Модель рекомендательной системы (Collaborative + Content-based)
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from .base_model import BaseMLModel


class RecommendationModel(BaseMLModel):
    """Модель рекомендательной системы"""
    
    def __init__(self, method: str = "hybrid"):
        super().__init__("recommendation")
        self.method = method
        self.user_item_matrix = None
        self.item_features = None
        self.tfidf_vectorizer = TfidfVectorizer(max_features=1000)
        
    def train(self, X: pd.DataFrame, y: Optional[pd.Series] = None) -> Dict[str, float]:
        """Обучение модели рекомендаций"""
        # Подготовка данных для collaborative filtering
        if 'user_id' in X.columns and 'product_id' in X.columns and 'rating' in X.columns:
            self.user_item_matrix = X.pivot_table(
                index='user_id', 
                columns='product_id', 
                values='rating', 
                fill_value=0
            )
        
        # Подготовка данных для content-based filtering
        if 'product_id' in X.columns and 'description' in X.columns:
            product_descriptions = X.groupby('product_id')['description'].first()
            self.item_features = self.tfidf_vectorizer.fit_transform(
                product_descriptions.fillna('')
            )
        
        self.is_trained = True
        self.metrics = {
            "method": self.method,
            "n_users": len(self.user_item_matrix) if self.user_item_matrix is not None else 0,
            "n_items": len(self.user_item_matrix.columns) if self.user_item_matrix is not None else 0
        }
        
        return self.metrics
    
    def predict(self, X: pd.DataFrame) -> List[Dict]:
        """Предсказание рекомендаций"""
        if not self.is_trained:
            raise ValueError("Модель не обучена")
        
        recommendations = []
        
        for _, row in X.iterrows():
            user_id = row.get('user_id')
            n_recommendations = row.get('n_recommendations', 5)
            
            if self.method == "collaborative" and self.user_item_matrix is not None:
                recs = self._collaborative_filtering(user_id, n_recommendations)
            elif self.method == "content" and self.item_features is not None:
                recs = self._content_based_filtering(user_id, n_recommendations)
            elif self.method == "hybrid":
                recs = self._hybrid_recommendations(user_id, n_recommendations)
            else:
                recs = []
            
            recommendations.append({
                "user_id": user_id,
                "recommendations": recs
            })
        
        return recommendations
    
    def _collaborative_filtering(self, user_id: int, n_recommendations: int) -> List[int]:
        """Collaborative filtering рекомендации"""
        if user_id not in self.user_item_matrix.index:
            return []
        
        user_ratings = self.user_item_matrix.loc[user_id]
        unrated_items = user_ratings[user_ratings == 0].index
        
        if len(unrated_items) == 0:
            return []
        
        # Простая рекомендация на основе популярности
        item_popularity = self.user_item_matrix.sum().sort_values(ascending=False)
        return item_popularity[item_popularity.index.isin(unrated_items)].head(n_recommendations).index.tolist()
    
    def _content_based_filtering(self, user_id: int, n_recommendations: int) -> List[int]:
        """Content-based рекомендации"""
        # Упрощенная реализация
        return list(range(1, n_recommendations + 1))
    
    def _hybrid_recommendations(self, user_id: int, n_recommendations: int) -> List[int]:
        """Гибридные рекомендации"""
        collab_recs = self._collaborative_filtering(user_id, n_recommendations // 2)
        content_recs = self._content_based_filtering(user_id, n_recommendations // 2)
        
        return collab_recs + content_recs

