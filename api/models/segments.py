#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pydantic models for segments API
Author: Customer Data Analytics Team
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date

class SegmentDistribution(BaseModel):
    """Модель распределения сегмента"""
    cluster_id: int = Field(..., description="ID кластера")
    users_count: int = Field(..., description="Количество пользователей")
    share: float = Field(..., description="Доля от общего числа пользователей")

class SegmentsDistributionResponse(BaseModel):
    """Ответ API для распределения сегментов"""
    date: str = Field(..., description="Дата снапшота")
    timezone: str = Field(..., description="Часовой пояс")
    available: bool = Field(..., description="Доступность данных")
    total_users: int = Field(..., description="Общее количество пользователей")
    segments: List[SegmentDistribution] = Field(..., description="Список сегментов")

class SegmentPoint(BaseModel):
    """Точка данных для динамики сегмента"""
    date: str = Field(..., description="Дата")
    count: int = Field(..., description="Количество пользователей")

class SegmentSeries(BaseModel):
    """Серия данных для кластера"""
    cluster_id: int = Field(..., description="ID кластера")
    points: List[SegmentPoint] = Field(..., description="Точки данных")

class SegmentsDynamicsResponse(BaseModel):
    """Ответ API для динамики сегментов"""
    from_date: str = Field(..., alias="from", description="Начальная дата")
    to_date: str = Field(..., alias="to", description="Конечная дата")
    timezone: str = Field(..., description="Часовой пояс")
    series: List[SegmentSeries] = Field(..., description="Серии данных по кластерам")

class MigrationMatrix(BaseModel):
    """Элемент матрицы переходов"""
    from_cluster: int = Field(..., alias="from", description="Исходный кластер")
    to_cluster: int = Field(..., alias="to", description="Целевой кластер")
    count: int = Field(..., description="Количество переходов")

class SegmentsMigrationResponse(BaseModel):
    """Ответ API для матрицы переходов"""
    date: str = Field(..., description="Дата анализа")
    prev_date: str = Field(..., description="Предыдущая дата")
    timezone: str = Field(..., description="Часовой пояс")
    available: bool = Field(..., description="Доступность данных")
    matrix: List[MigrationMatrix] = Field(..., description="Матрица переходов")
    note: Optional[str] = Field(None, description="Дополнительная информация")

class ClusterMeta(BaseModel):
    """Метаданные кластера"""
    id: int = Field(..., description="ID кластера")
    name: str = Field(..., description="Название кластера")
    description: str = Field(..., description="Описание кластера")

class SegmentsMetaResponse(BaseModel):
    """Ответ API для метаданных сегментов"""
    clusters: List[ClusterMeta] = Field(..., description="Список кластеров")

