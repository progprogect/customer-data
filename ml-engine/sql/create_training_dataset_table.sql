-- =========================
-- ML Training Dataset Table
-- =========================
-- Создание таблицы для хранения готового обучающего датасета

-- Таблица для обучающего датасета (фичи + таргеты)
CREATE TABLE IF NOT EXISTS ml_training_dataset (
  -- Идентификаторы
  user_id            BIGINT NOT NULL,
  snapshot_date      DATE   NOT NULL,
  
  -- Фичи (RFM признаки)
  recency_days       INT,                      -- дни с последней покупки (NULL если не покупал)
  frequency_90d      INT    NOT NULL DEFAULT 0, -- кол-во заказов за 90д
  monetary_180d      NUMERIC(12,2) NOT NULL DEFAULT 0, -- сумма заказов за 180д
  aov_180d           NUMERIC(12,2),            -- средний чек за 180д (NULL если нет заказов)
  orders_lifetime    INT    NOT NULL DEFAULT 0, -- общее кол-во заказов
  revenue_lifetime   NUMERIC(12,2) NOT NULL DEFAULT 0, -- общая выручка
  categories_unique  INT    NOT NULL DEFAULT 0, -- уник. категорий за 180д
  
  -- Таргет
  purchase_next_30d  BOOLEAN NOT NULL,         -- целевая переменная
  
  -- Метаинформация
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  PRIMARY KEY (user_id, snapshot_date)
);

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_training_dataset_target ON ml_training_dataset(purchase_next_30d);
CREATE INDEX IF NOT EXISTS idx_training_dataset_date ON ml_training_dataset(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_training_dataset_recency ON ml_training_dataset(recency_days);
CREATE INDEX IF NOT EXISTS idx_training_dataset_frequency ON ml_training_dataset(frequency_90d);
CREATE INDEX IF NOT EXISTS idx_training_dataset_monetary ON ml_training_dataset(monetary_180d);

-- Комментарии
COMMENT ON TABLE ml_training_dataset IS 'Готовый обучающий датасет для XGBoost модели';
COMMENT ON COLUMN ml_training_dataset.user_id IS 'ID пользователя';
COMMENT ON COLUMN ml_training_dataset.snapshot_date IS 'Дата снапшота (понедельник)';
COMMENT ON COLUMN ml_training_dataset.recency_days IS 'Дни с последней покупки (NULL если не покупал)';
COMMENT ON COLUMN ml_training_dataset.frequency_90d IS 'Количество заказов за последние 90 дней';
COMMENT ON COLUMN ml_training_dataset.monetary_180d IS 'Сумма заказов за последние 180 дней';
COMMENT ON COLUMN ml_training_dataset.aov_180d IS 'Средний чек за последние 180 дней';
COMMENT ON COLUMN ml_training_dataset.orders_lifetime IS 'Общее количество заказов за всё время';
COMMENT ON COLUMN ml_training_dataset.revenue_lifetime IS 'Общая сумма заказов за всё время';
COMMENT ON COLUMN ml_training_dataset.categories_unique IS 'Количество уникальных категорий за последние 180 дней';
COMMENT ON COLUMN ml_training_dataset.purchase_next_30d IS 'Таргет: TRUE если есть покупка в следующие 30 дней';
COMMENT ON COLUMN ml_training_dataset.created_at IS 'Дата создания записи';
