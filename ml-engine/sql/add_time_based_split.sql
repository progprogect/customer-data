-- =========================
-- Add Time-based Split Column
-- =========================
-- Добавление колонки split с временным разрезом train/valid/test

-- Добавляем колонку split (если её нет)
ALTER TABLE ml_training_dataset 
ADD COLUMN IF NOT EXISTS split TEXT;

-- Обновляем значения split на основе границ дат
UPDATE ml_training_dataset 
SET split = CASE 
  WHEN snapshot_date >= '2025-03-17'::date AND snapshot_date <= '2025-07-07'::date THEN 'train'
  WHEN snapshot_date >= '2025-07-14'::date AND snapshot_date <= '2025-07-28'::date THEN 'valid'  
  WHEN snapshot_date >= '2025-08-04'::date AND snapshot_date <= '2025-08-18'::date THEN 'test'
  ELSE 'unknown'  -- Не должно происходить
END;

-- Создаем индекс для быстрого доступа
CREATE INDEX IF NOT EXISTS idx_training_dataset_split ON ml_training_dataset(split);

-- Проверяем результат разбивки
SELECT 
  split,
  COUNT(*) as rows_count,
  COUNT(DISTINCT snapshot_date) as unique_dates,
  MIN(snapshot_date) as start_date,
  MAX(snapshot_date) as end_date,
  COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END) as positive_class,
  ROUND(COUNT(CASE WHEN purchase_next_30d = TRUE THEN 1 END)::NUMERIC / COUNT(*) * 100, 2) as positive_percent
FROM ml_training_dataset 
GROUP BY split 
ORDER BY 
  CASE split 
    WHEN 'train' THEN 1
    WHEN 'valid' THEN 2  
    WHEN 'test' THEN 3
    ELSE 4
  END;
