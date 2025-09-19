#!/usr/bin/env python3
"""
Content-Based Item Similarity Computation
Вычисление content-based похожести товаров

Author: Customer Data Analytics Team
"""

import os
import sys
import numpy as np
import pandas as pd
import psycopg2
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from scipy.sparse import csr_matrix, hstack
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import TruncatedSVD
import warnings
warnings.filterwarnings('ignore')

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('content_similarity.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Параметры алгоритма
SIMILARITY_CONFIG = {
    'weights': {
        'tfidf_tags': 0.4,
        'brand': 0.2,
        'category': 0.1,
        'style': 0.1,  # будет добавлен к категориальным
        'numeric': 0.3
    },
    'tfidf': {
        'max_features': 500,  # ограничиваем словарь
        'min_df': 2,         # минимум 2 товара должны иметь тег
        'max_df': 0.8,       # исключаем слишком частые
        'ngram_range': (1, 1)
    },
    'numeric_features': ['price_current', 'popularity_30d', 'rating'],
    'top_k': 50,             # топ-50 похожих для каждого товара
    'min_sim_score': 0.01    # минимальный порог сходства
}

class ContentSimilarityComputer:
    def __init__(self, db_connection_string: str):
        self.db_conn_str = db_connection_string
        self.conn = None
        self.products_df = None
        self.feature_vectors = None
        self.similarity_matrix = None
        
    def connect_db(self):
        """Подключение к БД"""
        try:
            self.conn = psycopg2.connect(self.db_conn_str)
            logger.info("✅ Connected to database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def load_product_features(self) -> pd.DataFrame:
        """Загрузка признаков товаров из БД"""
        query = """
        SELECT 
            product_id,
            brand,
            category,
            color,
            size,
            material,
            gender,
            style,
            price_current,
            rating,
            popularity_30d,
            tags_normalized,
            tags_count,
            title,
            description_short,
            is_active
        FROM ml_item_content_features
        WHERE is_active = true
        ORDER BY product_id
        """
        
        logger.info("📊 Loading product features from database...")
        self.products_df = pd.read_sql(query, self.conn)
        
        # Обработка отсутствующих значений
        self.products_df['brand'] = self.products_df['brand'].fillna('unknown')
        self.products_df['category'] = self.products_df['category'].fillna('unknown')
        self.products_df['style'] = self.products_df['style'].fillna('unknown')
        self.products_df['rating'] = self.products_df['rating'].fillna(2.5)
        self.products_df['popularity_30d'] = self.products_df['popularity_30d'].fillna(0)
        
        # Обработка тегов
        self.products_df['tags_text'] = self.products_df['tags_normalized'].apply(
            lambda x: ' '.join(x) if x and isinstance(x, list) else ''
        )
        
        logger.info(f"📈 Loaded {len(self.products_df)} active products")
        return self.products_df
    
    def create_tfidf_features(self) -> csr_matrix:
        """Создание TF-IDF векторов для тегов"""
        logger.info("🔤 Creating TF-IDF vectors for tags...")
        
        vectorizer = TfidfVectorizer(
            max_features=SIMILARITY_CONFIG['tfidf']['max_features'],
            min_df=SIMILARITY_CONFIG['tfidf']['min_df'],
            max_df=SIMILARITY_CONFIG['tfidf']['max_df'],
            ngram_range=SIMILARITY_CONFIG['tfidf']['ngram_range'],
            token_pattern=r'\b[a-zA-Z]+\b',  # только буквенные токены
            lowercase=True,
            stop_words=None
        )
        
        # Создаем TF-IDF матрицу
        tfidf_matrix = vectorizer.fit_transform(self.products_df['tags_text'])
        
        # Логируем статистику
        feature_names = vectorizer.get_feature_names_out()
        logger.info(f"📊 TF-IDF vocabulary size: {len(feature_names)}")
        logger.info(f"📊 TF-IDF matrix shape: {tfidf_matrix.shape}")
        logger.info(f"📊 TF-IDF sparsity: {(1 - tfidf_matrix.nnz / tfidf_matrix.size) * 100:.1f}%")
        
        # Топ-20 самых важных тегов
        feature_scores = np.array(tfidf_matrix.sum(axis=0)).flatten()
        top_features_idx = np.argsort(feature_scores)[-20:]
        top_features = [feature_names[i] for i in top_features_idx[::-1]]
        logger.info(f"🏷️ Top tags: {', '.join(top_features[:10])}")
        
        return tfidf_matrix, vectorizer
    
    def create_categorical_features(self) -> csr_matrix:
        """Создание one-hot векторов для категориальных признаков"""
        logger.info("📂 Creating categorical features...")
        
        categorical_matrices = []
        
        # Brand features
        brand_encoder = LabelEncoder()
        brand_encoded = brand_encoder.fit_transform(self.products_df['brand'])
        brand_matrix = self._create_onehot_matrix(brand_encoded, len(brand_encoder.classes_))
        categorical_matrices.append(brand_matrix)
        logger.info(f"🏷️ Brand features: {len(brand_encoder.classes_)} unique brands")
        
        # Category features  
        category_encoder = LabelEncoder()
        category_encoded = category_encoder.fit_transform(self.products_df['category'])
        category_matrix = self._create_onehot_matrix(category_encoded, len(category_encoder.classes_))
        categorical_matrices.append(category_matrix)
        logger.info(f"📂 Category features: {len(category_encoder.classes_)} unique categories")
        
        # Style features
        style_encoder = LabelEncoder()
        style_encoded = style_encoder.fit_transform(self.products_df['style'])
        style_matrix = self._create_onehot_matrix(style_encoded, len(style_encoder.classes_))
        categorical_matrices.append(style_matrix)
        logger.info(f"🎨 Style features: {len(style_encoder.classes_)} unique styles")
        
        # Объединяем все категориальные признаки
        categorical_features = hstack(categorical_matrices)
        logger.info(f"📊 Total categorical features shape: {categorical_features.shape}")
        
        return categorical_features
    
    def _create_onehot_matrix(self, encoded_values: np.ndarray, n_classes: int) -> csr_matrix:
        """Создание one-hot матрицы"""
        from scipy.sparse import csr_matrix
        rows = np.arange(len(encoded_values))
        cols = encoded_values
        data = np.ones(len(encoded_values))
        return csr_matrix((data, (rows, cols)), shape=(len(encoded_values), n_classes))
    
    def create_numeric_features(self) -> np.ndarray:
        """Создание нормализованных числовых признаков"""
        logger.info("🔢 Creating numeric features...")
        
        numeric_cols = SIMILARITY_CONFIG['numeric_features']
        numeric_data = self.products_df[numeric_cols].copy()
        
        # Логарифмическое преобразование для цены и популярности
        numeric_data['price_current'] = np.log1p(numeric_data['price_current'])
        numeric_data['popularity_30d'] = np.log1p(numeric_data['popularity_30d'])
        
        # Стандартизация
        scaler = StandardScaler()
        numeric_features = scaler.fit_transform(numeric_data)
        
        logger.info(f"📊 Numeric features shape: {numeric_features.shape}")
        logger.info(f"📊 Feature means: {numeric_features.mean(axis=0)}")
        logger.info(f"📊 Feature stds: {numeric_features.std(axis=0)}")
        
        return numeric_features
    
    def combine_features(self) -> Tuple[csr_matrix, Dict]:
        """Объединение всех типов признаков с весами"""
        logger.info("🔗 Combining all feature types...")
        
        # Создаем каждый тип признаков
        tfidf_matrix, tfidf_vectorizer = self.create_tfidf_features()
        categorical_matrix = self.create_categorical_features()
        numeric_matrix = self.create_numeric_features()
        
        # Применяем веса
        weights = SIMILARITY_CONFIG['weights']
        
        # Взвешиваем TF-IDF
        tfidf_weighted = tfidf_matrix * weights['tfidf_tags']
        
        # Взвешиваем категориальные (равномерно распределяем веса)
        cat_weight_per_feature = (weights['brand'] + weights['category'] + weights['style']) / categorical_matrix.shape[1]
        categorical_weighted = categorical_matrix * cat_weight_per_feature
        
        # Взвешиваем числовые
        numeric_weight_per_feature = weights['numeric'] / numeric_matrix.shape[1]
        numeric_weighted = numeric_matrix * numeric_weight_per_feature
        
        # Объединяем все в одну матрицу
        from scipy.sparse import csr_matrix
        numeric_sparse = csr_matrix(numeric_weighted)
        
        combined_features = hstack([tfidf_weighted, categorical_weighted, numeric_sparse])
        
        # Сохраняем метаданные
        feature_info = {
            'tfidf_features': tfidf_matrix.shape[1],
            'categorical_features': categorical_matrix.shape[1], 
            'numeric_features': numeric_matrix.shape[1],
            'total_features': combined_features.shape[1],
            'vectorizer': tfidf_vectorizer
        }
        
        logger.info(f"🎯 Combined features shape: {combined_features.shape}")
        logger.info(f"📊 Feature breakdown: TF-IDF={feature_info['tfidf_features']}, "
                   f"Categorical={feature_info['categorical_features']}, "
                   f"Numeric={feature_info['numeric_features']}")
        
        self.feature_vectors = combined_features
        return combined_features, feature_info
    
    def compute_similarity_matrix(self, features: csr_matrix) -> np.ndarray:
        """Вычисление cosine similarity матрицы"""
        logger.info("🔄 Computing cosine similarity matrix...")
        
        n_items = features.shape[0]
        logger.info(f"📊 Computing similarity for {n_items} items...")
        
        # Для больших матриц используем батчи
        batch_size = 100
        similarity_matrix = np.zeros((n_items, n_items), dtype=np.float32)
        
        for i in range(0, n_items, batch_size):
            end_i = min(i + batch_size, n_items)
            batch_features = features[i:end_i]
            
            # Вычисляем сходство только с товарами после текущего батча (симметричная матрица)
            for j in range(i, n_items, batch_size):
                end_j = min(j + batch_size, n_items)
                other_features = features[j:end_j]
                
                # Cosine similarity
                sim_batch = cosine_similarity(batch_features, other_features)
                similarity_matrix[i:end_i, j:end_j] = sim_batch
                
                # Заполняем симметричную часть
                if i != j:
                    similarity_matrix[j:end_j, i:end_i] = sim_batch.T
            
            if (i // batch_size + 1) % 10 == 0:
                logger.info(f"⏳ Processed {i + batch_size}/{n_items} items...")
        
        logger.info("✅ Similarity matrix computation completed")
        self.similarity_matrix = similarity_matrix
        return similarity_matrix
    
    def extract_top_similar(self, similarity_matrix: np.ndarray) -> List[Dict]:
        """Извлечение топ-K похожих товаров для каждого"""
        logger.info(f"🔝 Extracting top-{SIMILARITY_CONFIG['top_k']} similar items...")
        
        similarities = []
        n_items = similarity_matrix.shape[0]
        
        for i in range(n_items):
            product_id = self.products_df.iloc[i]['product_id']
            
            # Получаем сходства для текущего товара
            sim_scores = similarity_matrix[i]
            
            # Исключаем себя и сортируем по убыванию
            sim_scores[i] = -1  # исключаем самого себя
            top_indices = np.argsort(sim_scores)[::-1]
            
            # Берем топ-K с фильтрацией по минимальному порогу
            count = 0
            for idx in top_indices:
                if count >= SIMILARITY_CONFIG['top_k']:
                    break
                
                sim_score = sim_scores[idx]
                if sim_score < SIMILARITY_CONFIG['min_sim_score']:
                    continue
                
                similar_product_id = self.products_df.iloc[idx]['product_id']
                
                similarities.append({
                    'product_id': int(product_id),
                    'similar_product_id': int(similar_product_id),
                    'sim_score': float(sim_score),
                    'features_used': 'tfidf_tags,brand,category,style,numeric'
                })
                count += 1
        
        logger.info(f"📊 Generated {len(similarities)} similarity pairs")
        return similarities
    
    def save_similarities_to_db(self, similarities: List[Dict]):
        """Сохранение результатов в БД"""
        logger.info("💾 Saving similarities to database...")
        
        # Очищаем старые данные
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM ml_item_sim_content")
            logger.info("🗑️ Cleared old similarity data")
        
        # Подготавливаем данные для batch insert
        insert_data = []
        for sim in similarities:
            insert_data.append((
                sim['product_id'],
                sim['similar_product_id'], 
                sim['sim_score'],
                sim['features_used'],
                json.dumps({})  # пока пустой breakdown
            ))
        
        # Batch insert
        insert_query = """
        INSERT INTO ml_item_sim_content 
        (product_id, similar_product_id, sim_score, features_used, sim_breakdown)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        with self.conn.cursor() as cur:
            cur.executemany(insert_query, insert_data)
            self.conn.commit()
        
        logger.info(f"✅ Saved {len(similarities)} similarity records to database")
    
    def run_content_similarity_pipeline(self):
        """Запуск полного пайплайна вычисления content-based сходства"""
        logger.info("🚀 Starting content-based similarity computation pipeline...")
        start_time = datetime.now()
        
        try:
            # 1. Подключение к БД и загрузка данных
            self.connect_db()
            self.load_product_features()
            
            # 2. Создание объединенных признаков
            features, feature_info = self.combine_features()
            
            # 3. Вычисление матрицы сходства
            similarity_matrix = self.compute_similarity_matrix(features)
            
            # 4. Извлечение топ-K
            similarities = self.extract_top_similar(similarity_matrix)
            
            # 5. Сохранение в БД
            self.save_similarities_to_db(similarities)
            
            # Финальная статистика
            end_time = datetime.now()
            duration = end_time - start_time
            
            logger.info("🎉 Content similarity pipeline completed successfully!")
            logger.info(f"⏱️ Total duration: {duration}")
            logger.info(f"📊 Products processed: {len(self.products_df)}")
            logger.info(f"📊 Similarity pairs generated: {len(similarities)}")
            logger.info(f"📊 Average similarities per product: {len(similarities) / len(self.products_df):.1f}")
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            raise
        finally:
            if self.conn:
                self.conn.close()


def main():
    """Главная функция"""
    # Параметры подключения к БД
    db_connection = "postgresql://mikitavalkunovich@localhost:5432/customer_data"
    
    # Создаем и запускаем компьютер сходства
    computer = ContentSimilarityComputer(db_connection)
    computer.run_content_similarity_pipeline()


if __name__ == "__main__":
    main()
