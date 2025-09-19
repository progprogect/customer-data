"""
ML Models Module
Модели машинного обучения для различных задач
"""

from .base_model import BaseMLModel
from .segmentation import UserSegmentationModel
from .recommendation import RecommendationModel
from .churn_prediction import ChurnPredictionModel
from .price_elasticity import PriceElasticityModel

__all__ = [
    "BaseMLModel",
    "UserSegmentationModel", 
    "RecommendationModel",
    "ChurnPredictionModel",
    "PriceElasticityModel"
]
