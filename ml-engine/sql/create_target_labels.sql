-- =========================
-- ML Target Labels Table
-- =========================
-- Создание таблицы для хранения таргетов purchase_next_30d

-- Таблица для лейблов покупок в следующие 30 дней
CREATE TABLE IF NOT EXISTS ml_labels_purchase_30d (
  user_id            BIGINT NOT NULL,
  snapshot_date      DATE   NOT NULL,
  purchase_next_30d  BOOLEAN NOT NULL,
  first_order_id     BIGINT,         -- ID первого заказа в окне [snapshot_date+1, snapshot_date+30]
  first_order_date   DATE,           -- Дата первого заказа в окне
  PRIMARY KEY (user_id, snapshot_date)
);

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_labels_snapshot_date ON ml_labels_purchase_30d(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_labels_target ON ml_labels_purchase_30d(purchase_next_30d);
CREATE INDEX IF NOT EXISTS idx_labels_user_date ON ml_labels_purchase_30d(user_id, snapshot_date);

-- Комментарии
COMMENT ON TABLE ml_labels_purchase_30d IS 'Таргеты для предсказания покупок в следующие 30 дней';
COMMENT ON COLUMN ml_labels_purchase_30d.user_id IS 'ID пользователя';
COMMENT ON COLUMN ml_labels_purchase_30d.snapshot_date IS 'Дата снапшота (понедельник)';
COMMENT ON COLUMN ml_labels_purchase_30d.purchase_next_30d IS 'TRUE если есть заказ в окне (snapshot_date+1, snapshot_date+30]';
COMMENT ON COLUMN ml_labels_purchase_30d.first_order_id IS 'ID первого заказа в целевом окне (для анализа)';
COMMENT ON COLUMN ml_labels_purchase_30d.first_order_date IS 'Дата первого заказа в целевом окне';
