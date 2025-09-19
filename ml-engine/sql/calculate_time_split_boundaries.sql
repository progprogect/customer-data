-- =========================
-- Calculate Time-based Split Boundaries
-- =========================
-- Расчет границ временного разреза на train/valid/test (70%/15%/15%)

WITH date_sequence AS (
  -- Получаем отсортированный список уникальных дат
  SELECT 
    snapshot_date,
    ROW_NUMBER() OVER (ORDER BY snapshot_date) - 1 AS date_index  -- 0-based index
  FROM (
    SELECT DISTINCT snapshot_date 
    FROM ml_training_dataset 
    ORDER BY snapshot_date
  ) t
),

split_boundaries AS (
  SELECT 
    COUNT(*) AS total_dates,
    -- Расчет границ (70% / 15% / 15%)
    FLOOR(COUNT(*) * 0.70)::int AS train_end_index,
    FLOOR(COUNT(*) * 0.85)::int AS valid_end_index,
    COUNT(*) - 1 AS test_end_index  -- last index
  FROM date_sequence
),

date_boundaries AS (
  SELECT 
    sb.total_dates,
    sb.train_end_index,
    sb.valid_end_index,
    sb.test_end_index,
    
    -- Границы дат для train
    (SELECT snapshot_date FROM date_sequence WHERE date_index = 0) AS train_start_date,
    (SELECT snapshot_date FROM date_sequence WHERE date_index = sb.train_end_index) AS train_end_date,
    
    -- Границы дат для valid
    (SELECT snapshot_date FROM date_sequence WHERE date_index = sb.train_end_index + 1) AS valid_start_date,
    (SELECT snapshot_date FROM date_sequence WHERE date_index = sb.valid_end_index) AS valid_end_date,
    
    -- Границы дат для test
    (SELECT snapshot_date FROM date_sequence WHERE date_index = sb.valid_end_index + 1) AS test_start_date,
    (SELECT snapshot_date FROM date_sequence WHERE date_index = sb.test_end_index) AS test_end_date
    
  FROM split_boundaries sb
)

-- Выводим границы и статистику
SELECT 
  'SPLIT BOUNDARIES' as section,
  total_dates,
  train_end_index + 1 as train_dates,
  valid_end_index - train_end_index as valid_dates,
  test_end_index - valid_end_index as test_dates,
  
  train_start_date,
  train_end_date,
  valid_start_date,
  valid_end_date,
  test_start_date,
  test_end_date,
  
  -- Проценты
  ROUND((train_end_index + 1)::numeric / total_dates * 100, 1) as train_percent,
  ROUND((valid_end_index - train_end_index)::numeric / total_dates * 100, 1) as valid_percent,
  ROUND((test_end_index - valid_end_index)::numeric / total_dates * 100, 1) as test_percent
  
FROM date_boundaries;
