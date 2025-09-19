import React, { useState, useEffect } from 'react'
import './ChurnPredictionPage.css'

interface ChurnStatistics {
  total_records: number
  churn_count: number
  retention_count: number
  churn_rate_percent: number
  unique_users: number
  date_range: {
    earliest: string | null
    latest: string | null
  }
}

interface UserFeatures {
  user_id: number
  snapshot_date: string
  recency_days: number | null
  frequency_90d: number
  monetary_180d: number
  aov_180d: number | null
  orders_lifetime: number
  revenue_lifetime: number
  categories_unique: number
}

interface ChurnPrediction {
  user_id: number
  snapshot_date: string
  prob_churn_next_60d: number
  will_churn: boolean
  top_reasons: string[]
}

interface UserWithPrediction {
  user_info: {
    user_id: number
    email: string
    created_at: string
    last_login_at: string | null
  } | null
  features: UserFeatures
  prediction: ChurnPrediction
}

const ChurnPredictionPage: React.FC = () => {
  const [statistics, setStatistics] = useState<ChurnStatistics | null>(null)
  const [highRiskUsers, setHighRiskUsers] = useState<UserWithPrediction[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [threshold, setThreshold] = useState(0.6)
  const [limit, setLimit] = useState(20)

  const API_BASE = 'http://localhost:8000/api/v1/churn'
  const API_KEY = 'dev-token-12345'

  useEffect(() => {
    loadData()
  }, []) // Загружаем данные только при монтировании компонента

  const loadData = async (customThreshold?: number, customLimit?: number) => {
    setLoading(true)
    setError(null)

    const currentThreshold = customThreshold !== undefined ? customThreshold : threshold
    const currentLimit = customLimit !== undefined ? customLimit : limit

    try {
      // Загружаем статистику и пользователей с высоким риском параллельно
      const [statsResponse, usersResponse] = await Promise.all([
        fetch(`${API_BASE}/statistics`, {
          headers: {
            'Authorization': `Bearer ${API_KEY}`,
            'Content-Type': 'application/json'
          }
        }),
        fetch(`${API_BASE}/high-risk-users`, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${API_KEY}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            threshold: currentThreshold,
            limit: currentLimit,
            snapshot_date: '2025-07-14'
          })
        })
      ])

      if (!statsResponse.ok || !usersResponse.ok) {
        const errorText = !statsResponse.ok ? 
          `Statistics API error: ${statsResponse.status}` : 
          `Users API error: ${usersResponse.status}`
        throw new Error(errorText)
      }

      const [statsData, usersData] = await Promise.all([
        statsResponse.json(),
        usersResponse.json()
      ])

      setStatistics(statsData)
      setHighRiskUsers(usersData)
    } catch (err) {
      // Безопасная обработка ошибок
      let errorMessage = 'Произошла неизвестная ошибка'
      
      if (err instanceof Error) {
        errorMessage = err.message
      } else if (typeof err === 'string') {
        errorMessage = err
      } else {
        // Для объектов с возможными циклическими ссылками
        try {
          errorMessage = JSON.stringify(err)
        } catch {
          errorMessage = 'Ошибка загрузки данных (невозможно сериализовать)'
        }
      }
      
      setError(errorMessage)
    } finally {
      setLoading(false)
    }
  }

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('ru-RU')
  }

  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('ru-RU', {
      style: 'currency',
      currency: 'RUB'
    }).format(amount)
  }

  const getRiskLevel = (probability: number) => {
    if (probability >= 0.8) return { level: 'Критический', class: 'critical' }
    if (probability >= 0.6) return { level: 'Высокий', class: 'high' }
    if (probability >= 0.4) return { level: 'Средний', class: 'medium' }
    return { level: 'Низкий', class: 'low' }
  }

  if (loading) {
    return (
      <div className="churn-prediction-page">
        <div className="loading">Загрузка данных...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="churn-prediction-page">
        <div className="error">Ошибка: {error}</div>
      </div>
    )
  }

  return (
    <div className="churn-prediction-page">
      <div className="page-header">
        <h1>💔 Отток клиентов</h1>
        <p>Анализ и предсказание оттока клиентов на основе ML модели</p>
      </div>

      {/* KPI карточки */}
      {statistics && (
        <div className="kpi-cards">
          <div className="kpi-card">
            <div className="kpi-title">Общий отток</div>
            <div className="kpi-value">{statistics.churn_rate_percent}%</div>
            <div className="kpi-subtitle">
              {statistics.churn_count} из {statistics.total_records} клиентов
            </div>
          </div>
          
          <div className="kpi-card">
            <div className="kpi-title">Уникальных клиентов</div>
            <div className="kpi-value">{statistics.unique_users.toLocaleString()}</div>
            <div className="kpi-subtitle">в анализе</div>
          </div>
          
          <div className="kpi-card">
            <div className="kpi-title">Период анализа</div>
            <div className="kpi-value">
              {statistics.date_range.earliest && statistics.date_range.latest
                ? `${formatDate(statistics.date_range.earliest)} - ${formatDate(statistics.date_range.latest)}`
                : 'Не указан'
              }
            </div>
            <div className="kpi-subtitle">даты снапшотов</div>
          </div>
          
          <div className="kpi-card">
            <div className="kpi-title">Клиенты в риске</div>
            <div className="kpi-value">{highRiskUsers.length}</div>
            <div className="kpi-subtitle">порог {Math.round(threshold * 100)}%</div>
          </div>
        </div>
      )}

      {/* Фильтры */}
      <div className="filters">
        <div className="filter-group">
          <label htmlFor="threshold">Порог риска (%):</label>
          <input
            id="threshold"
            type="number"
            min="30"
            max="90"
            step="5"
            value={Math.round(threshold * 100)}
            onChange={(e) => {
              const value = parseInt(e.target.value)
              if (!isNaN(value)) {
                setThreshold(value / 100)
              }
            }}
            className="threshold-input"
          />
        </div>
        
        <div className="filter-group">
          <label htmlFor="limit">Количество клиентов:</label>
          <input
            id="limit"
            type="number"
            min="1"
            max="500"
            step="1"
            value={limit}
            onChange={(e) => {
              const value = parseInt(e.target.value)
              if (!isNaN(value) && value > 0) {
                setLimit(value)
              }
            }}
            className="limit-input"
          />
        </div>
        
        <button onClick={() => loadData()} className="refresh-btn">
          🔄 Обновить
        </button>
      </div>

      {/* Таблица пользователей с высоким риском */}
      <div className="high-risk-users">
        <h2>👥 Клиенты с высоким риском оттока</h2>
        
        {highRiskUsers.length === 0 ? (
          <div className="no-data">
            Нет клиентов с риском оттока выше {Math.round(threshold * 100)}%
          </div>
        ) : (
          <div className="users-table">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Email</th>
                  <th>Вероятность оттока</th>
                  <th>Уровень риска</th>
                  <th>Последняя покупка</th>
                  <th>Заказы за 90 дней</th>
                  <th>Траты за 180 дней</th>
                  <th>Причины риска</th>
                </tr>
              </thead>
              <tbody>
                {highRiskUsers.map((user) => {
                  const riskLevel = getRiskLevel(user.prediction.prob_churn_next_60d)
                  return (
                    <tr key={user.features.user_id}>
                      <td>{user.features.user_id}</td>
                      <td>
                        {user.user_info?.email || 'Неизвестно'}
                      </td>
                      <td>
                        <div className="probability">
                          {Math.round(user.prediction.prob_churn_next_60d * 100)}%
                        </div>
                      </td>
                      <td>
                        <span className={`risk-level ${riskLevel.class}`}>
                          {riskLevel.level}
                        </span>
                      </td>
                      <td>
                        {user.features.recency_days 
                          ? `${user.features.recency_days} дней назад`
                          : 'Не покупал'
                        }
                      </td>
                      <td>{user.features.frequency_90d}</td>
                      <td>{formatCurrency(user.features.monetary_180d)}</td>
                      <td>
                        <div className="reasons">
                          {user.prediction.top_reasons.map((reason, index) => (
                            <span key={index} className="reason-tag">
                              {reason}
                            </span>
                          ))}
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Дополнительная информация */}
      <div className="info-section">
        <h3>📊 О модели</h3>
        <div className="model-info">
          <p>
            <strong>XGBoost модель</strong> для предсказания оттока клиентов с горизонтом 60 дней.
            Модель обучена на данных за 6 месяцев и показывает AUC-ROC = 0.63.
          </p>
          <p>
            <strong>Основные признаки:</strong> частота покупок, сумма трат, 
            дни с последней покупки, количество заказов, разнообразие категорий.
          </p>
          <p>
            <strong>Рекомендации:</strong> клиенты с вероятностью оттока выше 60% 
            требуют особого внимания и удерживающих кампаний.
          </p>
        </div>
      </div>
    </div>
  )
}

export default ChurnPredictionPage
