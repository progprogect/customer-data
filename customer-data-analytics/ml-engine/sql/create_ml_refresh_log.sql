-- =========================
-- ML Refresh Log Table
-- Таблица для логирования обновлений витрин
-- =========================

CREATE TABLE IF NOT EXISTS ml_refresh_log (
    log_id          BIGSERIAL PRIMARY KEY,
    refresh_type    TEXT NOT NULL,          -- 'full_refresh', 'incremental', 'content_only'
    status          TEXT NOT NULL,          -- 'started', 'completed', 'failed'
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    duration_ms     BIGINT,                 -- длительность в миллисекундах
    details         JSONB,                  -- дополнительная информация
    error_message   TEXT,                   -- сообщение об ошибке если status = 'failed'
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_ml_refresh_log_type_date 
    ON ml_refresh_log(refresh_type, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_ml_refresh_log_status 
    ON ml_refresh_log(status);

-- Автоматическое вычисление duration_ms при обновлении
CREATE OR REPLACE FUNCTION calculate_refresh_duration()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.completed_at IS NOT NULL AND OLD.completed_at IS NULL THEN
        NEW.duration_ms = EXTRACT(EPOCH FROM (NEW.completed_at - NEW.started_at)) * 1000;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_duration
    BEFORE UPDATE ON ml_refresh_log
    FOR EACH ROW
    EXECUTE FUNCTION calculate_refresh_duration();

-- Комментарии
COMMENT ON TABLE ml_refresh_log IS 
    'Лог обновлений витрин рекомендательной системы';

COMMENT ON COLUMN ml_refresh_log.refresh_type IS 
    'Тип обновления: full_refresh, incremental, content_only';

COMMENT ON COLUMN ml_refresh_log.duration_ms IS 
    'Длительность обновления в миллисекундах (автоматически вычисляется)';
