"""
Helper Functions
Общие вспомогательные функции
"""

import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd


def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """Настройка логирования"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file) if log_file else logging.NullHandler()
        ]
    )


def safe_json_loads(json_str: str, default: Any = None) -> Any:
    """Безопасная загрузка JSON"""
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return default


def safe_json_dumps(obj: Any, default: str = "{}") -> str:
    """Безопасная сериализация в JSON"""
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return default


def format_currency(amount: float, currency: str = "USD") -> str:
    """Форматирование валюты"""
    if currency == "USD":
        return f"${amount:,.2f}"
    elif currency == "EUR":
        return f"€{amount:,.2f}"
    elif currency == "RUB":
        return f"{amount:,.2f} ₽"
    else:
        return f"{amount:,.2f} {currency}"


def format_percentage(value: float, decimals: int = 1) -> str:
    """Форматирование процентов"""
    return f"{value * 100:.{decimals}f}%"


def calculate_date_range(days: int) -> tuple[datetime, datetime]:
    """Расчет диапазона дат"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Очистка DataFrame от NaN и пустых значений"""
    # Заполняем NaN значения
    df = df.fillna("")
    
    # Удаляем дубликаты
    df = df.drop_duplicates()
    
    # Очищаем строки от лишних пробелов
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
    
    return df


def validate_email(email: str) -> bool:
    """Валидация email"""
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None


def validate_phone(phone: str) -> bool:
    """Валидация телефона"""
    import re
    # Простая валидация для международных номеров
    pattern = r'^\+?[1-9]\d{1,14}$'
    return re.match(pattern, phone.replace(' ', '').replace('-', '')) is not None


def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Разделение списка на чанки"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Объединение словарей"""
    result = {}
    for d in dicts:
        result.update(d)
    return result


def get_nested_value(data: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """Получение значения по вложенному ключу"""
    keys = key_path.split('.')
    value = data
    
    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default
    
    return value


def set_nested_value(data: Dict[str, Any], key_path: str, value: Any) -> None:
    """Установка значения по вложенному ключу"""
    keys = key_path.split('.')
    current = data
    
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    current[keys[-1]] = value

