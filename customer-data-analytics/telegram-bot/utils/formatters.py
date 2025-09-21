"""
Formatters
Утилиты для форматирования данных для Telegram
"""

import html
from typing import List, Dict, Any, Optional
from datetime import datetime, date
import locale

# Устанавливаем локаль для форматирования чисел
try:
    locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except:
        pass  # Используем дефолтное форматирование


def escape_html(text: str) -> str:
    """Экранирование HTML символов"""
    if text is None:
        return ""
    return html.escape(str(text))


def format_number(value: float, decimals: int = 0) -> str:
    """Форматирование чисел с разделителями"""
    if value is None:
        return "0"
    
    try:
        if decimals == 0:
            return f"{int(value):,}".replace(",", " ")
        else:
            return f"{value:,.{decimals}f}".replace(",", " ")
    except:
        return str(value)


def format_currency(value: float, currency: str = "$") -> str:
    """Форматирование валюты"""
    if value is None:
        return f"{currency}0"
    
    try:
        formatted = format_number(value, 2)
        return f"{currency}{formatted}"
    except:
        return f"{currency}{value}"


def format_percentage(value: float, decimals: int = 1) -> str:
    """Форматирование процентов"""
    if value is None:
        return "0%"
    
    try:
        return f"{value * 100:.{decimals}f}%"
    except:
        return f"{value}%"


def format_date(date_obj: Any) -> str:
    """Форматирование даты"""
    if date_obj is None:
        return "неизвестно"
    
    try:
        if isinstance(date_obj, str):
            # Пытаемся распарсить дату
            if "T" in date_obj:
                date_obj = datetime.fromisoformat(date_obj.replace("Z", "+00:00"))
            else:
                date_obj = datetime.strptime(date_obj, "%Y-%m-%d")
        
        return date_obj.strftime("%d.%m.%Y")
    except:
        return str(date_obj)


def format_datetime(datetime_obj: Any) -> str:
    """Форматирование даты и времени"""
    if datetime_obj is None:
        return "неизвестно"
    
    try:
        if isinstance(datetime_obj, str):
            datetime_obj = datetime.fromisoformat(datetime_obj.replace("Z", "+00:00"))
        
        return datetime_obj.strftime("%d.%m.%Y %H:%M")
    except:
        return str(datetime_obj)


def format_days_ago(days: int) -> str:
    """Форматирование количества дней назад"""
    if days is None or days < 0:
        return "неизвестно"
    
    if days == 0:
        return "сегодня"
    elif days == 1:
        return "вчера"
    elif days < 5:
        return f"{days} дня назад"
    else:
        return f"{days} дней назад"


def format_table_row(cells: List[str], widths: List[int] = None) -> str:
    """Форматирование строки таблицы"""
    if not cells:
        return ""
    
    if widths is None:
        widths = [len(str(cell)) for cell in cells]
    
    # Обрезаем длинные значения
    formatted_cells = []
    for i, cell in enumerate(cells):
        if len(str(cell)) > widths[i]:
            formatted_cells.append(str(cell)[:widths[i]-3] + "...")
        else:
            formatted_cells.append(str(cell).ljust(widths[i]))
    
    return " | ".join(formatted_cells)


def format_user_summary(user_data: Dict[str, Any]) -> str:
    """Форматирование сводки пользователя"""
    user_id = user_data.get("user_id", "N/A")
    segment = user_data.get("segment", "неизвестно")
    ltv_12m = user_data.get("ltv_12m", 0)
    last_order_days = user_data.get("last_order_days", None)
    prob_purchase = user_data.get("prob_purchase_30d", None)
    prob_churn = user_data.get("prob_churn_60d", None)
    
    text = f"""
👤 <b>Пользователь #{user_id}</b>
🎯 Сегмент: <b>{escape_html(segment)}</b>
💰 LTV(12м): <b>{format_currency(ltv_12m)}</b>

📅 Последняя покупка: <b>{format_days_ago(last_order_days)}</b>"""

    # Добавляем ML предсказания только если они есть
    if prob_purchase is not None:
        text += f"\n🛒 Вероятность покупки (30д): <b>{format_percentage(prob_purchase)}</b>"
    
    if prob_churn is not None:
        text += f"\n⚠️ Риск оттока (60д): <b>{format_percentage(prob_churn)}</b>"
    
    # Добавляем рекомендации если есть
    recommendations = user_data.get("recommendations", [])
    if recommendations:
        # Обрабатываем как список объектов или как список строк
        rec_texts = []
        for rec in recommendations[:3]:
            if isinstance(rec, dict):
                title = rec.get("title", rec.get("item_name", ""))
                # Проверяем, что это не моковые данные
                if title and not title.startswith("Premium Product for User") and not title.startswith("Accessory for User"):
                    rec_texts.append(title)
            elif isinstance(rec, str) and not rec.startswith("Premium Product for User") and not rec.startswith("Accessory for User"):
                rec_texts.append(rec)
        
        if rec_texts:
            text += f"\n🎯 Рекомендации: {escape_html(', '.join(rec_texts))}"
        else:
            text += "\n🎯 Рекомендации: нет данных"
    else:
        text += "\n🎯 Рекомендации: нет данных"
    
    # Добавляем аномалии если есть
    anomalies = user_data.get("anomalies", [])
    if anomalies:
        text += f"\n🚨 Аномалии (90д): найдено {len(anomalies)}"
    else:
        text += "\n🚨 Аномалии (90д): нет"
    
    return text


def format_segments_table(segments_data: List[Dict[str, Any]]) -> str:
    """Форматирование таблицы сегментов"""
    if not segments_data:
        return "❌ Данные сегментов недоступны"
    
    text = "📊 <b>Распределение сегментов</b>\n\n"
    
    # Мапинг ID сегментов на названия
    segment_names = {
        0: "Новые/Неактивные",
        1: "Обычные", 
        2: "VIP",
        3: "Премиум"
    }
    
    for i, segment in enumerate(segments_data, 1):
        cluster_id = segment.get("cluster_id", segment.get("cluster", "N/A"))
        cluster_name = segment_names.get(cluster_id, f"Кластер {cluster_id}")
        users = segment.get("users_count", segment.get("users", segment.get("user_count", segment.get("count", 0))))
        percentage = segment.get("share", segment.get("percentage", segment.get("ratio", 0)))
        
        text += f"{i}. <b>{escape_html(cluster_name)}</b>\n"
        text += f"   👥 Пользователей: {format_number(users)}\n"
        text += f"   📊 Доля: {format_percentage(percentage, 1)}\n\n"
    
    return text


def format_probability_table(users_data: List[Dict[str, Any]], threshold: float) -> str:
    """Форматирование таблицы вероятности покупки"""
    if not users_data:
        return f"❌ Пользователи с вероятностью покупки ≥ {threshold:.1%} не найдены"
    
    text = ""
    
    for i, user in enumerate(users_data, 1):
        user_id = user.get("user_id", "N/A")
        probability = user.get("prob_next_30d", 0)
        
        # Определяем сегмент на основе вероятности (упрощенная логика)
        if probability >= 0.8:
            segment = "VIP"
        elif probability >= 0.6:
            segment = "Высокий"
        elif probability >= 0.4:
            segment = "Средний"
        else:
            segment = "Низкий"
        
        # Получаем дополнительные данные из features если доступны
        features = user.get("features", {})
        frequency_90d = features.get("frequency_90d", 0)
        aov = features.get("aov_180d", 0)
        
        text += f"{i}. <b>#{user_id}</b>\n"
        text += f"   🎯 Вероятность: {format_percentage(probability)}\n"
        text += f"   👤 Сегмент: {escape_html(str(segment))}\n"
        text += f"   📊 Частота (90д): {frequency_90d}\n"
        text += f"   💰 AOV: {format_currency(aov)}\n\n"
    
    return text


def format_churn_table(users_data: List[Dict[str, Any]], threshold: float) -> str:
    """Форматирование таблицы риска оттока"""
    if not users_data:
        return f"❌ Пользователи с риском оттока ≥ {threshold:.1%} не найдены"
    
    text = f"⚠️ <b>Риск оттока (60д)</b>\n"
    text += f"Порог: <b>{format_percentage(threshold)}</b>\n\n"
    
    for i, user in enumerate(users_data, 1):
        user_id = user.get("user_id", "N/A")
        churn_prob = user.get("churn_probability", 0)
        reasons = user.get("reasons", [])
        reasons_text = ", ".join(reasons[:2]) if reasons else "нет данных"
        
        text += f"{i}. <b>#{user_id}</b>\n"
        text += f"   ⚠️ Риск: {format_percentage(churn_prob)}\n"
        text += f"   📝 Причины: {escape_html(reasons_text)}\n\n"
    
    return text


def format_anomalies_table(anomalies_data: List[Dict[str, Any]]) -> str:
    """Форматирование таблицы аномалий"""
    if not anomalies_data:
        return "✅ Аномалии не обнаружены"
    
    text = "🚨 <b>Аномалии поведения</b>\n\n"
    
    for i, anomaly in enumerate(anomalies_data, 1):
        user_id = anomaly.get("user_id", "N/A")
        week = anomaly.get("week", anomaly.get("week_date", anomaly.get("week_start", "N/A")))
        score = anomaly.get("anomaly_score", anomaly.get("score", 0))
        triggers = anomaly.get("triggers", anomaly.get("trigger_types", []))
        
        # Обрабатываем триггеры
        if isinstance(triggers, list):
            # Преобразуем технические названия в читаемые
            readable_triggers = []
            for trigger in triggers[:2]:
                if "z_monetary" in str(trigger):
                    readable_triggers.append("💰 траты")
                elif "ratio_monetary" in str(trigger):
                    readable_triggers.append("📈 рост трат")
                elif "z_frequency" in str(trigger):
                    readable_triggers.append("🔄 частота")
                elif "z_recency" in str(trigger):
                    readable_triggers.append("⏰ давность")
                else:
                    readable_triggers.append(str(trigger)[:10])
            triggers_text = ", ".join(readable_triggers) if readable_triggers else "нет данных"
        else:
            triggers_text = str(triggers)[:20] if triggers else "нет данных"
        
        text += f"{i}. <b>#{user_id}</b>\n"
        text += f"   📅 Неделя: {escape_html(str(week))}\n"
        text += f"   📊 Скор: {score:.1f}\n"
        text += f"   ⚠️ Триггеры: {escape_html(triggers_text)}\n\n"
    
    return text


def format_recommendations_list(recommendations: List[Dict[str, Any]]) -> str:
    """Форматирование списка рекомендаций"""
    if not recommendations:
        return "❌ Рекомендации недоступны"
    
    text = "🎯 <b>Персональные рекомендации</b>\n\n"
    
    for i, rec in enumerate(recommendations[:5], 1):
        # Обрабатываем разные форматы данных рекомендаций
        if isinstance(rec, dict):
            title = rec.get("title", rec.get("product_title", rec.get("name", "Неизвестный товар")))
            score = rec.get("score", rec.get("rating", rec.get("confidence", 0)))
            source = rec.get("source", rec.get("method", "Hybrid"))
            price = rec.get("price", rec.get("product_price", 0))
            product_id = rec.get("product_id", rec.get("id", ""))
        else:
            # Если rec не словарь, пробуем преобразовать
            title = str(rec) if rec else "Неизвестный товар"
            score = 0
            source = "Hybrid"
            price = 0
            product_id = ""
        
        # Форматируем строку
        text += f"{i}. <b>{escape_html(title)}</b>"
        if product_id:
            text += f" (ID: {product_id})"
        text += "\n"
        
        # Добавляем дополнительную информацию если доступна
        if isinstance(rec, dict):
            category = rec.get('category', '')
            brand = rec.get('brand', '')
            rating = rec.get('rating', 0)
            
            extra_info = []
            if category:
                extra_info.append(f"📂 {escape_html(category)}")
            if brand:
                extra_info.append(f"🏷️ {escape_html(brand)}")
            if rating > 0:
                extra_info.append(f"⭐ {rating:.1f}")
            
            if extra_info:
                text += f"   {', '.join(extra_info)}\n"
        
        text += f"   📊 {format_percentage(score)} | 🔗 {escape_html(source)} | {format_currency(price)}\n\n"
    
    return text


def format_price_elasticity_scenarios(category: str, scenarios: List[Dict[str, Any]]) -> str:
    """Форматирование сценариев ценовой эластичности"""
    if not scenarios:
        return f"❌ Данные эластичности для категории '{category}' недоступны"
    
    text = f"💰 <b>Эластичность цен: {escape_html(category)}</b>\n\n"
    
    for i, scenario in enumerate(scenarios, 1):
        price_change = scenario.get("price_change", 0)
        sales_change = scenario.get("sales_change", 0)
        
        text += f"{i}. <b>Цена {price_change:+.0f}%</b> → <b>Продажи {sales_change:+.0f}%</b>\n"
    
    return text


def format_emoji_bar(value: float, max_value: float = 100, length: int = 10) -> str:
    """Форматирование эмодзи-бара для визуализации"""
    if max_value == 0:
        return "░" * length
    
    percentage = min(value / max_value, 1.0)
    filled = int(percentage * length)
    
    bar = "█" * filled + "░" * (length - filled)
    return f"{bar} {format_percentage(percentage)}"


def truncate_text(text: str, max_length: int = 4000) -> str:
    """Обрезание текста до максимальной длины с сохранением форматирования"""
    if len(text) <= max_length:
        return text
    
    # Пытаемся обрезать по строкам
    lines = text.split('\n')
    result = ""
    
    for line in lines:
        if len(result + line + '\n') > max_length - 50:  # Оставляем место для "..." 
            result += "\n... (сообщение обрезано)"
            break
        result += line + '\n'
    
    return result


def format_current_time() -> str:
    """Форматирование текущего времени"""
    return datetime.now().strftime("%d.%m.%Y %H:%M")
