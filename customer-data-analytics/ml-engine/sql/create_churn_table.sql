-- =========================
-- Create Churn Labels Table
-- =========================
-- Создание таблицы для хранения churn лейблов с горизонтом 60 дней

-- Создание таблицы для churn лейблов
CREATE TABLE IF NOT EXISTS ml_labels_churn_60d (
  -- Основные поля
  user_id                     BIGINT NOT NULL,
  snapshot_date               DATE   NOT NULL,
  is_churn_next_60d          BOOLEAN NOT NULL,
  
  -- Метаданные для анализа и отладки
  last_order_before_date     DATE,               -- дата последнего заказа до снапшота
  first_order_after_date     DATE,               -- дата первого заказа в окне (D, D+60], если не ушёл
  
  -- Системные поля
  created_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  -- Первичный ключ
  PRIMARY KEY (user_id, snapshot_date)
);

-- Создание индексов для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_churn_labels_date ON ml_labels_churn_60d(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_churn_labels_churn ON ml_labels_churn_60d(is_churn_next_60d);
CREATE INDEX IF NOT EXISTS idx_churn_labels_user_date ON ml_labels_churn_60d(user_id, snapshot_date);

-- Комментарии к таблице и полям
COMMENT ON TABLE ml_labels_churn_60d IS 'Churn лейблы с горизонтом 60 дней для еженедельных снапшотов';
COMMENT ON COLUMN ml_labels_churn_60d.user_id IS 'ID пользователя';
COMMENT ON COLUMN ml_labels_churn_60d.snapshot_date IS 'Дата снапшота (понедельник)';
COMMENT ON COLUMN ml_labels_churn_60d.is_churn_next_60d IS 'Флаг оттока: TRUE если нет заказов в следующие 60 дней';
COMMENT ON COLUMN ml_labels_churn_60d.last_order_before_date IS 'Дата последнего заказа до снапшота (для анализа)';
COMMENT ON COLUMN ml_labels_churn_60d.first_order_after_date IS 'Дата первого заказа в окне (D, D+60], NULL если churn';
COMMENT ON COLUMN ml_labels_churn_60d.created_at IS 'Время создания записи';
COMMENT ON COLUMN ml_labels_churn_60d.updated_at IS 'Время последнего обновления записи';

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_churn_labels_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Триггер для автоматического обновления updated_at
DROP TRIGGER IF EXISTS trigger_update_churn_labels_updated_at ON ml_labels_churn_60d;
CREATE TRIGGER trigger_update_churn_labels_updated_at
    BEFORE UPDATE ON ml_labels_churn_60d
    FOR EACH ROW
    EXECUTE FUNCTION update_churn_labels_updated_at();

-- Проверка создания таблицы
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'ml_labels_churn_60d') THEN
        RAISE NOTICE '✅ Таблица ml_labels_churn_60d успешно создана';
        RAISE NOTICE '📊 Структура таблицы:';
        RAISE NOTICE '   - user_id: BIGINT (PK part 1)';
        RAISE NOTICE '   - snapshot_date: DATE (PK part 2)';
        RAISE NOTICE '   - is_churn_next_60d: BOOLEAN (основной таргет)';
        RAISE NOTICE '   - last_order_before_date: DATE (метаданные)';
        RAISE NOTICE '   - first_order_after_date: DATE (метаданные)';
        RAISE NOTICE '   - created_at/updated_at: TIMESTAMPTZ (системные)';
    ELSE
        RAISE EXCEPTION '❌ Ошибка создания таблицы ml_labels_churn_60d';
    END IF;
END $$;
