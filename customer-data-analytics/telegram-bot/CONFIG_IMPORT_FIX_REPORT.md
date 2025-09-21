# ✅ Исправлена ошибка импорта config

## 🐛 **Проблема:**
```
Error showing probability table: name 'config' is not defined
```

## 🔧 **Решение:**
1. **Добавлен импорт** `from utils.config import get_config` в `callback_handler.py`
2. **Исправлено использование** `config.PAGE_SIZE` → `config = get_config()` + `config.PAGE_SIZE`

## 📝 **Изменения:**

### **Файл:** `handlers/callback_handler.py`
```python
# Добавлен импорт
from utils.config import get_config

# Исправлена функция
async def show_probability_table(query, user_id: int, page: int = 1, threshold: float = 0.7):
    try:
        config = get_config()  # ← Добавлено
        users_data = await api_client.get_top_purchase_probability_users(user_id, threshold, config.PAGE_SIZE)
```

## ✅ **Результат:**
- ✅ Функция работает без ошибок
- ✅ Сообщение отправляется корректно
- ✅ Telegram бот запущен и готов к использованию

---

**Теперь функция "Вероятность покупки" полностью работает с реальными данными!** 🎯
