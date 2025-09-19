import { useEffect, useState } from 'react'
import { fetchDistribution, fetchMeta, fetchDynamics, fetchMigration, type ClusterMeta, type DistributionResponse, type DynamicsResponse, type MigrationResponse } from '../services/segments'
import { useFilters } from '../store/filters'
import DistributionCard from '../components/DistributionCard'
import DynamicsCard from '../components/DynamicsCard'
import MigrationCard from '../components/MigrationCard'

function SegmentationPage() {
  const { state, dispatch } = useFilters()
  const [meta, setMeta] = useState<ClusterMeta[] | null>(null)
  const [distribution, setDistribution] = useState<DistributionResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [dynamics, setDynamics] = useState<DynamicsResponse | null>(null)
  const [migration, setMigration] = useState<MigrationResponse | null>(null)

  useEffect(() => {
    let mounted = true
    setLoading(true)
    setError(null)
    const isFuture = state.date ? state.date > new Date().toISOString().slice(0,10) : false
    if (isFuture) {
      setError(null)
      setDistribution(null)
      return
    }
    Promise.all([
      fetchMeta(),
      fetchDistribution(state.date ? { date: state.date } : undefined),
    ])
      .then(([m, d]) => {
        if (!mounted) return
        setMeta(m)
        if (!state.date && d.last_available_date) {
          dispatch({ type: 'setDate', date: d.last_available_date })
        }
        setDistribution(d)
      })
      .catch((e: unknown) => {
        if (!mounted) return
        setError(e instanceof Error ? e.message : 'Ошибка загрузки')
      })
      .finally(() => mounted && setLoading(false))
    return () => {
      mounted = false
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.date])

  useEffect(() => {
    if (!state.date) return
    const isFuture = state.date > new Date().toISOString().slice(0,10)
    if (isFuture) {
      setDynamics(null)
      setMigration({ date: state.date, timezone: 'Europe/Warsaw', available: false, matrix: [] })
      return
    }
    let mounted = true
    setError(null)
    const { period, granularity } = state
    const to = state.date
    const fromDate = new Date(to)
    fromDate.setDate(fromDate.getDate() - (period - 1))
    const from = fromDate.toISOString().slice(0, 10)
    Promise.all([
      fetchDynamics({ from_date: from, to_date: to, granularity }),
      fetchMigration({ date: to }),
    ])
      .then(([dy, mg]) => {
        if (!mounted) return
        setDynamics(dy)
        setMigration(mg)
      })
      .catch((e: unknown) => {
        if (!mounted) return
        setError(e instanceof Error ? e.message : 'Ошибка загрузки')
      })
    return () => {
      mounted = false
    }
  }, [state.date, state.period, state.granularity])

  return (
    <div>
      <h2>Сегментация</h2>
      <div className="filters">
        <label>
          Дата
          <input
            type="date"
            value={state.date ?? ''}
            onChange={(e) => dispatch({ type: 'setDate', date: e.target.value || null })}
          />
        </label>
        <label>
          Период
          <select
            value={state.period}
            onChange={(e) => dispatch({ type: 'setPeriod', period: Number(e.target.value) as 7 | 30 | 90 })}
          >
            <option value="7">7</option>
            <option value="30">30</option>
            <option value="90">90</option>
          </select>
        </label>
        <label>
          Гранулярность
          <select
            value={state.granularity}
            onChange={(e) => dispatch({ type: 'setGranularity', granularity: e.target.value as 'day' | 'week' | 'month' })}
          >
            <option value="day">День</option>
            <option value="week">Неделя</option>
            <option value="month">Месяц</option>
          </select>
        </label>
        
        {/* Чекбоксы кластеров */}
        {meta && meta.length > 0 && (
          <div className="cluster-filters">
            <span className="filter-label">Показать кластеры:</span>
            {meta.map((cluster) => (
              <label key={cluster.id} className="cluster-checkbox">
                <input
                  type="checkbox"
                  checked={!state.hiddenClusters.includes(cluster.id)}
                  onChange={() => dispatch({ type: 'toggleCluster', clusterId: cluster.id })}
                />
                <span className="cluster-name">{cluster.name}</span>
              </label>
            ))}
          </div>
        )}
      </div>
      <div className="grid">
        {loading ? (
          <section className="card" style={{ gridColumn: 'span 12' }}>
            <div className="loading-spinner"></div>
            <div className="muted" style={{ textAlign: 'center', marginTop: '16px' }}>
              Загрузка данных сегментации...
            </div>
          </section>
        ) : error ? (
          <section className="card" style={{ gridColumn: 'span 12' }}>
            <div className="error-message">
              <strong>Ошибка загрузки данных</strong>
              <p>{error}</p>
              <button 
                className="retry-button" 
                onClick={() => dispatch({ type: 'setDate', date: state.date })}
              >
                Повторить загрузку
              </button>
            </div>
          </section>
        ) : (
          <>
            {distribution && distribution.available && distribution.segments.length > 0 ? (
              <DistributionCard 
                data={distribution} 
                title="Распределение сегментов (сегодня)" 
                meta={meta ?? undefined}
                hiddenClusters={state.hiddenClusters}
              />
            ) : (
              <section className="card" style={{ gridColumn: 'span 6' }}>
                <h3>Распределение сегментов (сегодня)</h3>
                <div>Нет данных за выбранную дату</div>
              </section>
            )}
            {dynamics && dynamics.available && dynamics.series.length > 0 ? (
              <DynamicsCard 
                data={dynamics} 
                title="Динамика по времени" 
                meta={meta ?? undefined}
                hiddenClusters={state.hiddenClusters}
              />
            ) : (
              <section className="card" style={{ gridColumn: 'span 6' }}>
                <h3>Динамика по времени</h3>
                <div>Нет данных за выбранный период</div>
              </section>
            )}
            {migration ? (
              <MigrationCard 
                data={migration} 
                title="Переходы между сегментами (вчера→сегодня)" 
                meta={meta ?? undefined}
                hiddenClusters={state.hiddenClusters}
              />
            ) : (
              <section className="card" style={{ gridColumn: 'span 12' }}>
                <h3>Переходы между сегментами (вчера→сегодня)</h3>
                <div>Загрузка…</div>
              </section>
            )}
          </>
        )}
      </div>
    </div>
  )
}

export default SegmentationPage


