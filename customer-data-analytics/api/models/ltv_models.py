"""
LTV (Lifetime Value) Models
Pydantic модели для LTV аналитики
"""

from typing import List, Optional
from datetime import date, datetime
from pydantic import BaseModel, Field


class UserLTV(BaseModel):
    """Модель LTV данных пользователя"""
    
    user_id: int = Field(..., description="ID пользователя")
    signup_date: date = Field(..., description="Дата регистрации")
    
    # LTV по горизонтам (в USD)
    revenue_3m: float = Field(0.0, description="Выручка за первые 3 месяца")
    revenue_6m: float = Field(0.0, description="Выручка за первые 6 месяцев")
    revenue_12m: float = Field(0.0, description="Выручка за первые 12 месяцев")
    lifetime_revenue: float = Field(0.0, description="Выручка за весь период")
    
    # Количество заказов
    orders_3m: int = Field(0, description="Заказы за 3 месяца")
    orders_6m: int = Field(0, description="Заказы за 6 месяцев")
    orders_12m: int = Field(0, description="Заказы за 12 месяцев")
    orders_lifetime: int = Field(0, description="Заказы за весь период")
    
    # Средний чек
    avg_order_value_3m: Optional[float] = Field(None, description="AOV за 3 месяца")
    avg_order_value_6m: Optional[float] = Field(None, description="AOV за 6 месяцев")
    avg_order_value_12m: Optional[float] = Field(None, description="AOV за 12 месяцев")
    avg_order_value_lifetime: Optional[float] = Field(None, description="AOV за весь период")
    
    # Метаданные
    last_order_date: Optional[date] = Field(None, description="Дата последнего заказа")
    first_order_date: Optional[date] = Field(None, description="Дата первого заказа")
    days_since_last_order: Optional[int] = Field(None, description="Дней с последнего заказа")
    
    # Системные поля
    created_at: datetime = Field(..., description="Дата создания записи")
    updated_at: datetime = Field(..., description="Дата обновления записи")


class LTVSummary(BaseModel):
    """Модель агрегированной статистики LTV"""
    
    metric_name: str = Field(..., description="Название метрики")
    value_3m: float = Field(0.0, description="Значение за 3 месяца")
    value_6m: float = Field(0.0, description="Значение за 6 месяцев")
    value_12m: float = Field(0.0, description="Значение за 12 месяцев")
    value_lifetime: float = Field(0.0, description="Значение за весь период")


class LTVSummaryResponse(BaseModel):
    """Ответ с агрегированной статистикой LTV"""
    
    summary: List[LTVSummary] = Field(..., description="Список метрик LTV")
    total_users: int = Field(..., description="Общее количество пользователей")
    calculated_at: datetime = Field(..., description="Время расчета")


class UserLTVResponse(BaseModel):
    """Ответ с LTV данными пользователей"""
    
    users: List[UserLTV] = Field(..., description="Список пользователей с LTV данными")
    total_count: int = Field(..., description="Общее количество пользователей")
    page: int = Field(1, description="Номер страницы")
    page_size: int = Field(50, description="Размер страницы")


class LTVCalculationRequest(BaseModel):
    """Запрос на пересчет LTV"""
    
    user_ids: Optional[List[int]] = Field(None, description="ID пользователей для пересчета (если None - все пользователи)")
    force_recalculate: bool = Field(False, description="Принудительный пересчет")


class LTVCalculationResponse(BaseModel):
    """Ответ на запрос пересчета LTV"""
    
    success: bool = Field(..., description="Успешность операции")
    message: str = Field(..., description="Сообщение о результате")
    users_processed: int = Field(..., description="Количество обработанных пользователей")
    calculation_time: float = Field(..., description="Время расчета в секундах")


class LTVFilters(BaseModel):
    """Фильтры для LTV данных"""
    
    min_revenue_6m: Optional[float] = Field(None, description="Минимальная выручка за 6 месяцев")
    max_revenue_6m: Optional[float] = Field(None, description="Максимальная выручка за 6 месяцев")
    min_revenue_12m: Optional[float] = Field(None, description="Минимальная выручка за 12 месяцев")
    max_revenue_12m: Optional[float] = Field(None, description="Максимальная выручка за 12 месяцев")
    min_orders_lifetime: Optional[int] = Field(None, description="Минимальное количество заказов")
    max_orders_lifetime: Optional[int] = Field(None, description="Максимальное количество заказов")
    signup_date_from: Optional[date] = Field(None, description="Дата регистрации с")
    signup_date_to: Optional[date] = Field(None, description="Дата регистрации по")
    sort_by: str = Field("lifetime_revenue", description="Поле для сортировки")
    sort_order: str = Field("desc", description="Порядок сортировки (asc/desc)")


class LTVDistribution(BaseModel):
    """Модель распределения LTV"""
    
    range_min: float = Field(..., description="Минимальное значение диапазона")
    range_max: float = Field(..., description="Максимальное значение диапазона")
    count: int = Field(..., description="Количество пользователей в диапазоне")
    percentage: float = Field(..., description="Процент от общего количества")


class LTVDistributionResponse(BaseModel):
    """Ответ с распределением LTV"""
    
    distribution: List[LTVDistribution] = Field(..., description="Распределение LTV по диапазонам")
    total_users: int = Field(..., description="Общее количество пользователей")
    metric_type: str = Field(..., description="Тип метрики (revenue_6m, revenue_12m, etc.)")
