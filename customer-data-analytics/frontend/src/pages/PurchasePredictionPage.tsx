import { useState, useEffect } from 'react'
import {
  getModelInfo,
  getPurchaseProbabilities,
  getRealUsersWithFeatures,
  getDatabaseStatistics,
  createTestUsers,
  groupByProbabilityLevel,
  getUserExplanation,
  formatProbability,
  getProbabilityColor,
  getFeatureInterpretation,
  type ModelInfo,
  type PredictionResponse,
  type PredictionResult,
  type UserFeatures
} from '../services/ml'

interface StatCard {
  title: string
  value: string | number
  subtitle?: string
  color?: string
  icon: string
}

function StatCardComponent({ title, value, subtitle, color = '#3b82f6', icon }: StatCard) {
  return (
    <div className="stat-card">
      <div className="stat-icon" style={{ backgroundColor: color }}>
        {icon}
      </div>
      <div className="stat-content">
        <h3 className="stat-title">{title}</h3>
        <div className="stat-value">{value}</div>
        {subtitle && <div className="stat-subtitle">{subtitle}</div>}
      </div>
    </div>
  )
}

interface ProbabilityDistribution {
  high: number
  medium: number
  low: number
}

function DistributionChart({ distribution }: { distribution: ProbabilityDistribution }) {
  const total = distribution.high + distribution.medium + distribution.low
  const highPercent = (distribution.high / total) * 100
  const mediumPercent = (distribution.medium / total) * 100
  const lowPercent = (distribution.low / total) * 100
  
  // Создаем динамический conic-gradient на основе реальных данных
  const dynamicGradient = `conic-gradient(
    #10b981 0% ${highPercent}%, 
    #f59e0b ${highPercent}% ${highPercent + mediumPercent}%, 
    #ef4444 ${highPercent + mediumPercent}% 100%
  )`

  return (
    <div className="distribution-chart">
      <h3>Распределение по вероятности покупки</h3>
      <div className="chart-container">
        <div 
          className="pie-chart" 
          style={{ background: dynamicGradient }}
        >
        </div>
        <div className="chart-legend">
          <div className="legend-item">
            <span className="legend-color" style={{ backgroundColor: '#10b981' }}></span>
            <span>Высокая (≥60%): {distribution.high} чел. ({highPercent.toFixed(1)}%)</span>
          </div>
          <div className="legend-item">
            <span className="legend-color" style={{ backgroundColor: '#f59e0b' }}></span>
            <span>Средняя (30-60%): {distribution.medium} чел. ({mediumPercent.toFixed(1)}%)</span>
          </div>
          <div className="legend-item">
            <span className="legend-color" style={{ backgroundColor: '#ef4444' }}></span>
            <span>Низкая (&lt;30%): {distribution.low} чел. ({lowPercent.toFixed(1)}%)</span>
          </div>
        </div>
      </div>
    </div>
  )
}

interface TopUser {
  user: PredictionResult
  features: UserFeatures
  explanation: string
}

interface RealUserData {
  user_id: number
  snapshot_date: string
  features: UserFeatures
  explanation: string
}

function TopUsersTable({ topUsers }: { topUsers: TopUser[] }) {
  return (
    <div className="top-users-section">
      <h3>ТОП-5 пользователей с высокой вероятностью покупки</h3>
      <div className="table-container">
        <table className="top-users-table">
          <thead>
            <tr>
              <th>Пользователь</th>
              <th>Вероятность</th>
              <th>Ключевые факторы</th>
            </tr>
          </thead>
          <tbody>
            {topUsers.map(({ user, explanation }) => (
              <tr key={user.user_id}>
                <td>
                  <div className="user-info">
                    <div className="user-avatar">
                      {String(user.user_id).padStart(3, '0')}
                    </div>
                    <span>User {user.user_id}</span>
                  </div>
                </td>
                <td>
                  <span
                    className="probability-badge"
                    style={{ backgroundColor: getProbabilityColor(user.prob_next_30d) }}
                  >
                    {formatProbability(user.prob_next_30d)}
                  </span>
                </td>
                <td className="explanation">{explanation}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function ModelInterpretation({ modelInfo }: { modelInfo: ModelInfo }) {
  const topFeatures = modelInfo.feature_names.slice(0, 3) // Берем топ-3

  return (
    <div className="model-interpretation">
      <h3>Что учитывает модель при предсказании</h3>
      <div className="interpretation-grid">
        {topFeatures.map(featureName => {
          const interpretation = getFeatureInterpretation(featureName)
          return (
            <div key={featureName} className="interpretation-card">
              <div className="interpretation-icon">{interpretation.icon}</div>
              <div className="interpretation-content">
                <h4>{interpretation.label}</h4>
                <p>{interpretation.description}</p>
              </div>
            </div>
          )
        })}
      </div>
      <div className="model-performance">
        <h4>Качество модели</h4>
        <div className="performance-metrics">
          <span>Точность: {Math.round((modelInfo.model_performance.precision || 0) * 100)}%</span>
          <span>Версия: {modelInfo.model_version}</span>
        </div>
      </div>
    </div>
  )
}

interface DatabaseStats {
  snapshot_date: string
  total_users_in_db: number
  users_with_orders: number
  users_with_features: number
  sample_size_used: number
}

function PurchasePredictionPage() {
  const [modelInfo, setModelInfo] = useState<ModelInfo | null>(null)
  const [predictions, setPredictions] = useState<PredictionResponse | null>(null)
  const [realUsers, setRealUsers] = useState<RealUserData[]>([])
  const [dbStats, setDbStats] = useState<DatabaseStats | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  // Статистика с экстраполяцией на полную базу

  // Загрузка данных
  useEffect(() => {
    loadData()
  }, [])

  const loadData = async () => {
    try {
      setLoading(true)
      setError(null)

      // Загружаем информацию о модели
      const modelData = await getModelInfo()
      setModelInfo(modelData)

      // Загружаем общую статистику базы данных
      const dbStatsData = await getDatabaseStatistics()
      setDbStats(dbStatsData)

      try {
        // Загружаем реальных пользователей из базы данных (случайная выборка 500)
        const realUsersData = await getRealUsersWithFeatures(500)
        setRealUsers(realUsersData.users)
        
        // Получаем предсказания для реальных пользователей
        const predictionRows = realUsersData.users.map(u => ({
          user_id: u.user_id,
          snapshot_date: u.snapshot_date,
          features: u.features
        }))
        const predictionsData = await getPurchaseProbabilities(predictionRows)
        setPredictions(predictionsData)
        
      } catch (realDataError) {
        // Если не удалось загрузить реальные данные, показываем ошибку
        throw new Error(`Не удалось загрузить данные из базы: ${realDataError}`)
      }

    } catch (err) {
      setError(err instanceof Error ? err.message : 'Произошла ошибка')
    } finally {
      setLoading(false)
    }
  }

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-spinner"></div>
        <p>Загрузка данных модели машинного обучения...</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="error-container">
        <div className="error-message">
          <h3>Ошибка загрузки данных</h3>
          <p>{error}</p>
          <button onClick={loadData} className="retry-button">
            Попробовать еще раз
          </button>
        </div>
      </div>
    )
  }

  if (!modelInfo || !predictions) {
    return <div>Нет данных для отображения</div>
  }

  // Обработка данных для визуализации
  const successfulResults = predictions.results.filter(r => r.prob_next_30d !== null)
  const distributionLevels = groupByProbabilityLevel(successfulResults)
  
  const distribution: ProbabilityDistribution = {
    high: distributionLevels.high.count,
    medium: distributionLevels.medium.count,
    low: distributionLevels.low.count
  }

  const averageProbability = successfulResults.reduce((sum, r) => sum + (r.prob_next_30d || 0), 0) / successfulResults.length

  // Топ пользователи с учетом реальных данных
  const topUsers: TopUser[] = distributionLevels.high.users
    .sort((a, b) => (b.prob_next_30d || 0) - (a.prob_next_30d || 0))
    .slice(0, 5)
    .map(user => {
      // Ищем реального пользователя из базы данных
      const realUser = realUsers.find(ru => ru.user_id === user.user_id)
      
      if (realUser) {
        // Используем реальные данные и объяснения
        const features = realUser.features
        const explanation = realUser.explanation
        
        return {
          user,
          features,
          explanation
        }
      }
      
      // Если пользователь не найден в реальных данных, пропускаем
      return null
    })
    .filter(Boolean) as TopUser[] // Убираем null значения

  // Экстраполяция на полную базу данных
  const sampleRate = successfulResults.length / (dbStats?.users_with_orders || 1)
  const extrapolatedHighProbability = Math.round((distribution.high / successfulResults.length) * (dbStats?.users_with_orders || 0))
  
  // Статистические карточки с экстраполяцией
  const statCards: StatCard[] = [
    {
      title: 'Пользователей в базе',
      value: dbStats?.users_with_orders || predictions.count,
      subtitle: `Анализ на основе выборки ${predictions.count} пользователей`,
      icon: '👨‍💼',
      color: '#3b82f6'
    },
    {
      title: 'Высокий шанс покупки',
      value: `~${extrapolatedHighProbability}`,
      subtitle: `Вероятность ≥60% (${Math.round((distribution.high / successfulResults.length) * 100)}% от выборки)`,
      icon: '🎯',
      color: '#10b981'
    },
    {
      title: 'Средняя вероятность',
      value: `${Math.round(averageProbability * 100)}%`,
      subtitle: 'По случайной выборке',
      icon: '📊',
      color: '#f59e0b'
    },
    {
      title: 'Версия модели',
      value: modelInfo.model_version.split('_')[0],
      subtitle: `${modelInfo.feature_count} признаков`,
      icon: '🤖',
      color: '#8b5cf6'
    }
  ]

  return (
    <div className="purchase-prediction-page">
      <div className="page-header">
        <h1>Прогноз покупок</h1>
        <p className="page-subtitle">
          Анализ вероятности покупки клиентов в ближайшие 30 дней на основе машинного обучения
        </p>
        {dbStats && (
          <div className="disclaimer">
            📊 <strong>Масштаб анализа:</strong> {dbStats.users_with_orders.toLocaleString()} пользователей в базе данных, 
            анализ проведен на репрезентативной выборке {dbStats.sample_size_used} пользователей 
            (дата: {dbStats.snapshot_date})
          </div>
        )}
      </div>

      {/* Статистические карточки */}
      <div className="stats-section">
        <div className="stats-grid">
          {statCards.map((card, index) => (
            <StatCardComponent key={index} {...card} />
          ))}
        </div>
      </div>

      {/* Кнопка обновления */}
      <div className="filters-section">
        <div className="filters-container">
          <div className="filter-group">
            <button onClick={loadData} className="refresh-button">
              🔄 Обновить данные
            </button>
          </div>
        </div>
      </div>

      {/* Основной контент */}
      <div className="main-content">
        {/* Визуализации */}
        <div className="visualizations-section">
          <DistributionChart distribution={distribution} />
        </div>

        {/* Топ пользователи */}
        <div className="top-users-section">
          <TopUsersTable topUsers={topUsers} />
        </div>

        {/* Интерпретация модели */}
        <div className="interpretation-section">
          <ModelInterpretation modelInfo={modelInfo} />
        </div>
      </div>
    </div>
  )
}

export default PurchasePredictionPage
