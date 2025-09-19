-- =========================
-- Create Churn Training Dataset Table
-- =========================
-- Создание таблицы для хранения тренировочного датасета churn prediction

-- Создание таблицы для тренировочного датасета
CREATE TABLE IF NOT EXISTS ml_training_dataset_churn (
  -- Идентификаторы
  user_id                     BIGINT NOT NULL,
  snapshot_date               DATE   NOT NULL,
  
  -- RFM признаки
  recency_days               INT,                    -- дни с последней покупки (NULL если не покупал)
  frequency_90d              INT    NOT NULL,        -- кол-во заказов за 90 дней
  monetary_180d              NUMERIC(12,2) NOT NULL, -- сумма заказов за 180 дней
  aov_180d                   NUMERIC(12,2),          -- средний чек за 180 дней (NULL если не было заказов)
  orders_lifetime            INT    NOT NULL,        -- общее кол-во заказов
  revenue_lifetime           NUMERIC(12,2) NOT NULL, -- общая выручка
  categories_unique          INT    NOT NULL,        -- уник. категорий за 180 дней
  
  -- Таргет
  target                     BOOLEAN NOT NULL,       -- is_churn_next_60d
  
  -- Split для train/valid/test
  split_type                 TEXT NOT NULL,          -- 'train' или 'valid_test'
  
  -- Метаданные
  last_order_before_date     DATE,                   -- дата последнего заказа до снапшота
  first_order_after_date     DATE,                   -- дата первого заказа в окне (D, D+60], если не ушёл
  
  -- Системные поля
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  -- Первичный ключ
  PRIMARY KEY (user_id, snapshot_date)
);

-- Создание индексов для оптимизации
CREATE INDEX IF NOT EXISTS idx_churn_training_split ON ml_training_dataset_churn(split_type);
CREATE INDEX IF NOT EXISTS idx_churn_training_target ON ml_training_dataset_churn(target);
CREATE INDEX IF NOT EXISTS idx_churn_training_date ON ml_training_dataset_churn(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_churn_training_user_date ON ml_training_dataset_churn(user_id, snapshot_date);

-- Комментарии к таблице и полям
COMMENT ON TABLE ml_training_dataset_churn IS 'Тренировочный датасет для churn prediction с time-based split';
COMMENT ON COLUMN ml_training_dataset_churn.user_id IS 'ID пользователя';
COMMENT ON COLUMN ml_training_dataset_churn.snapshot_date IS 'Дата снапшота';
COMMENT ON COLUMN ml_training_dataset_churn.recency_days IS 'Дни с последней покупки (NULL если не покупал)';
COMMENT ON COLUMN ml_training_dataset_churn.frequency_90d IS 'Количество заказов за последние 90 дней';
COMMENT ON COLUMN ml_training_dataset_churn.monetary_180d IS 'Сумма заказов за последние 180 дней';
COMMENT ON COLUMN ml_training_dataset_churn.aov_180d IS 'Средний чек за последние 180 дней (NULL если не было заказов)';
COMMENT ON COLUMN ml_training_dataset_churn.orders_lifetime IS 'Общее количество заказов за всё время';
COMMENT ON COLUMN ml_training_dataset_churn.revenue_lifetime IS 'Общая сумма заказов за всё время';
COMMENT ON COLUMN ml_training_dataset_churn.categories_unique IS 'Количество уникальных категорий за последние 180 дней';
COMMENT ON COLUMN ml_training_dataset_churn.target IS 'Таргет: TRUE если churn (нет заказов в следующие 60 дней)';
COMMENT ON COLUMN ml_training_dataset_churn.split_type IS 'Тип split: train или valid_test';
COMMENT ON COLUMN ml_training_dataset_churn.last_order_before_date IS 'Дата последнего заказа до снапшота (метаданные)';
COMMENT ON COLUMN ml_training_dataset_churn.first_order_after_date IS 'Дата первого заказа в окне (D, D+60], NULL если churn';
COMMENT ON COLUMN ml_training_dataset_churn.created_at IS 'Время создания записи';
COMMENT ON COLUMN ml_training_dataset_churn.updated_at IS 'Время последнего обновления записи';

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_churn_training_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического обновления updated_at
DROP TRIGGER IF EXISTS trigger_update_churn_training_updated_at ON ml_training_dataset_churn;
CREATE TRIGGER trigger_update_churn_training_updated_at
    BEFORE UPDATE ON ml_training_dataset_churn
    FOR EACH ROW
    EXECUTE FUNCTION update_churn_training_updated_at();

-- Проверка создания таблицы
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ml_training_dataset_churn') THEN
        RAISE NOTICE '✅ Таблица ml_training_dataset_churn успешно создана';
        RAISE NOTICE '📊 Структура таблицы:';
        RAISE NOTICE '   - user_id: BIGINT (PK part 1)';
        RAISE NOTICE '   - snapshot_date: DATE (PK part 2)';
        RAISE NOTICE '   - RFM признаки: recency_days, frequency_90d, monetary_180d, aov_180d';
        RAISE NOTICE '   - Lifetime признаки: orders_lifetime, revenue_lifetime, categories_unique';
        RAISE NOTICE '   - target: BOOLEAN (churn таргет)';
        RAISE NOTICE '   - split_type: TEXT (train/valid_test)';
        RAISE NOTICE '   - Метаданные: last_order_before_date, first_order_after_date';
    ELSE
        RAISE EXCEPTION '❌ Ошибка создания таблицы ml_training_dataset_churn';
    END IF;
END $$;
