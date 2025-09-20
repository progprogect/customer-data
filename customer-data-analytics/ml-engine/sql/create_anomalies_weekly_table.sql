-- =========================
-- ML User Anomalies Weekly Table
-- =========================
-- Создание таблицы для хранения аномалий поведения пользователей

-- Таблица для хранения аномалий поведения пользователей
CREATE TABLE IF NOT EXISTS ml_user_anomalies_weekly (
  user_id              BIGINT NOT NULL,
  week_start           DATE NOT NULL,
  anomaly_score        NUMERIC(10,4) NOT NULL,
  is_anomaly           BOOLEAN NOT NULL,
  triggers             TEXT[] NOT NULL,
  insufficient_history BOOLEAN NOT NULL DEFAULT FALSE,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  
  PRIMARY KEY (user_id, week_start)
);

-- Комментарии к полям
COMMENT ON TABLE ml_user_anomalies_weekly IS 'Таблица аномалий поведения пользователей по неделям';
COMMENT ON COLUMN ml_user_anomalies_weekly.user_id IS 'ID пользователя';
COMMENT ON COLUMN ml_user_anomalies_weekly.week_start IS 'Начало недели (понедельник)';
COMMENT ON COLUMN ml_user_anomalies_weekly.anomaly_score IS 'Максимальный z-score или ln(ratio)';
COMMENT ON COLUMN ml_user_anomalies_weekly.is_anomaly IS 'Флаг аномалии (true/false)';
COMMENT ON COLUMN ml_user_anomalies_weekly.triggers IS 'Список сработавших триггеров';
COMMENT ON COLUMN ml_user_anomalies_weekly.insufficient_history IS 'Недостаточно истории (< 4 недель)';

-- Индексы для оптимизации
CREATE INDEX IF NOT EXISTS idx_anomalies_user_week ON ml_user_anomalies_weekly(user_id, week_start);
CREATE INDEX IF NOT EXISTS idx_anomalies_week ON ml_user_anomalies_weekly(week_start);
CREATE INDEX IF NOT EXISTS idx_anomalies_score ON ml_user_anomalies_weekly(anomaly_score);
CREATE INDEX IF NOT EXISTS idx_anomalies_flag ON ml_user_anomalies_weekly(is_anomaly);
CREATE INDEX IF NOT EXISTS idx_anomalies_insufficient ON ml_user_anomalies_weekly(insufficient_history);

-- Функция для детекции аномалий
CREATE OR REPLACE FUNCTION detect_anomalies_weekly()
RETURNS void AS $$
BEGIN
  -- Очищаем таблицу перед заполнением
  TRUNCATE ml_user_anomalies_weekly;
  
  -- Вставляем данные с детекцией аномалий
  INSERT INTO ml_user_anomalies_weekly (
    user_id,
    week_start,
    anomaly_score,
    is_anomaly,
    triggers,
    insufficient_history
  )
  WITH user_weeks AS (
    -- Получаем все недели для каждого пользователя
    SELECT 
      user_id,
      week_start_date,
      orders_count,
      monetary_sum,
      categories_count,
      aov_weekly,
      -- Подсчитываем количество недель истории для каждого пользователя
      COUNT(*) OVER (PARTITION BY user_id ORDER BY week_start_date ROWS UNBOUNDED PRECEDING) as weeks_count
    FROM ml_user_behavior_weekly
    ORDER BY user_id, week_start_date
  ),
  user_metrics AS (
    -- Рассчитываем метрики динамики и скользящие средние
    SELECT 
      user_id,
      week_start_date,
      orders_count,
      monetary_sum,
      categories_count,
      weeks_count,
      
      -- Метрики динамики
      orders_count - LAG(orders_count, 1) OVER (PARTITION BY user_id ORDER BY week_start_date) as delta_orders,
      monetary_sum - LAG(monetary_sum, 1) OVER (PARTITION BY user_id ORDER BY week_start_date) as delta_monetary,
      
      -- Ratio метрики (защита от деления на ноль)
      CASE 
        WHEN LAG(orders_count, 1) OVER (PARTITION BY user_id ORDER BY week_start_date) > 0 
        THEN LEAST(orders_count::NUMERIC / LAG(orders_count, 1) OVER (PARTITION BY user_id ORDER BY week_start_date), 10.0)
        ELSE NULL 
      END as ratio_orders,
      
      CASE 
        WHEN LAG(monetary_sum, 1) OVER (PARTITION BY user_id ORDER BY week_start_date) > 0 
        THEN LEAST(monetary_sum / LAG(monetary_sum, 1) OVER (PARTITION BY user_id ORDER BY week_start_date), 10.0)
        ELSE NULL 
      END as ratio_monetary,
      
      -- Скользящие средние (4 недели без текущей)
      AVG(orders_count) OVER (
        PARTITION BY user_id 
        ORDER BY week_start_date 
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
      ) as mean_orders_4w,
      
      STDDEV(orders_count) OVER (
        PARTITION BY user_id 
        ORDER BY week_start_date 
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
      ) as std_orders_4w,
      
      AVG(monetary_sum) OVER (
        PARTITION BY user_id 
        ORDER BY week_start_date 
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
      ) as mean_monetary_4w,
      
      STDDEV(monetary_sum) OVER (
        PARTITION BY user_id 
        ORDER BY week_start_date 
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
      ) as std_monetary_4w,
      
      -- Предыдущие значения для проверки провалов
      LAG(orders_count, 1) OVER (PARTITION BY user_id ORDER BY week_start_date) as lag_orders_count
      
    FROM user_weeks
  ),
  anomaly_detection AS (
    -- Рассчитываем z-scores и определяем аномалии
    SELECT 
      user_id,
      week_start_date,
      orders_count,
      monetary_sum,
      weeks_count,
      delta_orders,
      delta_monetary,
      ratio_orders,
      ratio_monetary,
      mean_orders_4w,
      std_orders_4w,
      mean_monetary_4w,
      std_monetary_4w,
      lag_orders_count,
      
      -- Z-scores (защита от деления на ноль)
      CASE 
        WHEN std_orders_4w > 0.01 
        THEN (orders_count - mean_orders_4w) / std_orders_4w
        ELSE 0 
      END as z_orders,
      
      CASE 
        WHEN std_monetary_4w > 0.01 
        THEN (monetary_sum - mean_monetary_4w) / std_monetary_4w
        ELSE 0 
      END as z_monetary,
      
      -- Логарифмы ratio (для anomaly_score)
      CASE 
        WHEN ratio_orders IS NOT NULL AND ratio_orders > 0 
        THEN LN(ratio_orders)
        ELSE 0 
      END as ln_ratio_orders,
      
      CASE 
        WHEN ratio_monetary IS NOT NULL AND ratio_monetary > 0 
        THEN LN(ratio_monetary)
        ELSE 0 
      END as ln_ratio_monetary
      
    FROM user_metrics
  ),
  anomaly_rules AS (
    -- Применяем правила детекции аномалий
    SELECT 
      user_id,
      week_start_date,
      weeks_count,
      orders_count,
      monetary_sum,
      z_orders,
      z_monetary,
      ratio_orders,
      ratio_monetary,
      lag_orders_count,
      
      -- Anomaly score (максимальное отклонение)
      GREATEST(
        ABS(z_orders),
        ABS(z_monetary),
        ABS(ln_ratio_orders),
        ABS(ln_ratio_monetary)
      ) as anomaly_score,
      
      -- Проверка правил аномалий
      (z_orders >= 3 OR ratio_orders >= 3.0) as spike_orders,
      (z_orders <= -3 OR (lag_orders_count > 0 AND orders_count = 0)) as drop_orders,
      (z_monetary >= 3 OR ratio_monetary >= 3.0) as spike_monetary,
      (z_monetary <= -3) as drop_monetary,
      
      -- Общий флаг аномалии
      CASE 
        WHEN (z_orders >= 3 OR ratio_orders >= 3.0 OR 
              z_orders <= -3 OR (lag_orders_count > 0 AND orders_count = 0) OR
              z_monetary >= 3 OR ratio_monetary >= 3.0 OR
              z_monetary <= -3) THEN true
        ELSE false
      END as is_anomaly
      
    FROM anomaly_detection
  )
  SELECT 
    user_id,
    week_start_date,
    anomaly_score,
    is_anomaly,
    
    -- Формируем массив триггеров
    ARRAY_REMOVE(ARRAY[
      CASE WHEN spike_orders THEN 'z_orders>=3' ELSE NULL END,
      CASE WHEN spike_orders AND ratio_orders >= 3.0 THEN 'ratio_orders>=3' ELSE NULL END,
      CASE WHEN drop_orders THEN 'z_orders<=-3' ELSE NULL END,
      CASE WHEN drop_orders AND lag_orders_count > 0 AND orders_count = 0 THEN 'orders_drop_to_zero' ELSE NULL END,
      CASE WHEN spike_monetary THEN 'z_monetary>=3' ELSE NULL END,
      CASE WHEN spike_monetary AND ratio_monetary >= 3.0 THEN 'ratio_monetary>=3' ELSE NULL END,
      CASE WHEN drop_monetary THEN 'z_monetary<=-3' ELSE NULL END
    ], NULL) as triggers,
    
    -- Флаг недостаточной истории
    (weeks_count < 4) as insufficient_history
    
  FROM anomaly_rules
  ORDER BY user_id, week_start_date;
  
  -- Логируем результат
  RAISE NOTICE 'Обработано % записей, найдено % аномалий', 
    (SELECT COUNT(*) FROM ml_user_anomalies_weekly),
    (SELECT COUNT(*) FROM ml_user_anomalies_weekly WHERE is_anomaly = true);
END;
$$ LANGUAGE plpgsql;

-- Функция для обновления аномалий (добавляет только новые данные)
CREATE OR REPLACE FUNCTION update_anomalies_weekly()
RETURNS void AS $$
DECLARE
  last_week DATE;
BEGIN
  -- Находим последнюю неделю в таблице аномалий
  SELECT COALESCE(MAX(week_start), '1900-01-01'::DATE) 
  INTO last_week 
  FROM ml_user_anomalies_weekly;
  
  -- Удаляем аномалии для недель, которые будут пересчитаны
  DELETE FROM ml_user_anomalies_weekly 
  WHERE week_start > last_week;
  
  -- Пересчитываем аномалии для новых недель
  PERFORM detect_anomalies_weekly();
  
  -- Логируем результат
  RAISE NOTICE 'Обновлены аномалии, последняя неделя: %', 
    (SELECT MAX(week_start) FROM ml_user_anomalies_weekly);
END;
$$ LANGUAGE plpgsql;
