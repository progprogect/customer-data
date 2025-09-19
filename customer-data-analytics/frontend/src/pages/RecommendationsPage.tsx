import { useState, useEffect } from 'react'
import './RecommendationsPage.css'
import { 
  getHybridRecommendations, 
  getUserPurchases, 
  getTopRecommendedProducts,
  getAlgorithmMetrics,
  type RecommendationItem,
  type UserPurchase,
  type TopProduct,
  type AlgorithmMetrics
} from '../services/recommendations'

interface UserCardData {
  user_id: number
  recent_purchases: UserPurchase[]
  hybrid_recommendations: RecommendationItem[]
  loading: boolean
  error?: string
}

function RecommendationsPage() {
  const [userCards, setUserCards] = useState<UserCardData[]>([])
  const [algorithmMetrics, setAlgorithmMetrics] = useState<AlgorithmMetrics[]>([])
  const [topProducts, setTopProducts] = useState<TopProduct[]>([])
  const [loading, setLoading] = useState(true)

  // Фиксированный список пользователей для демо
  const DEMO_USER_IDS = [323, 588, 665, 2106, 2311]

  const ALGORITHM_COLORS = {
    'CF': '#10b981', // green
    'Content': '#3b82f6', // blue  
    'Popularity': '#6b7280', // gray
    'Hybrid': '#8b5cf6' // purple
  }

  useEffect(() => {
    loadRecommendationsData()
  }, [])

  const loadRecommendationsData = async () => {
    setLoading(true)
    try {
      // Загружаем данные для всех demo пользователей параллельно
      const userPromises = DEMO_USER_IDS.map(async (userId) => {
        const userData: UserCardData = {
          user_id: userId,
          recent_purchases: [],
          hybrid_recommendations: [],
          loading: true
        }

        try {
          // Получаем рекомендации и покупки параллельно
          const [hybridRecs, userPurchases] = await Promise.all([
            getHybridRecommendations(userId, 5),
            getUserPurchases(userId, 3)
          ])

          userData.hybrid_recommendations = hybridRecs
          userData.recent_purchases = userPurchases
          userData.loading = false
        } catch (error) {
          console.error(`Error loading data for user ${userId}:`, error instanceof Error ? error.message : String(error))
          userData.error = `Ошибка загрузки данных для пользователя ${userId}`
          userData.loading = false
        }

        return userData
      })

      // Загружаем метрики алгоритмов и топ товары параллельно с пользователями
      const [usersData, topProductsData] = await Promise.all([
        Promise.all(userPromises),
        getTopRecommendedProducts(8)
      ])

      setUserCards(usersData)
      setTopProducts(topProductsData)

      // Получаем метрики алгоритмов с цветами
      const metrics = getAlgorithmMetrics().map(metric => ({
        ...metric,
        color: ALGORITHM_COLORS[metric.algorithm.includes('CF') ? 'CF' : 
                                 metric.algorithm.includes('Content') ? 'Content' :
                                 metric.algorithm.includes('Hybrid') ? 'Hybrid' : 'Popularity']
      }))
      setAlgorithmMetrics(metrics)

    } catch (error) {
      console.error('Error loading recommendations data:', error instanceof Error ? error.message : String(error))
    } finally {
      setLoading(false)
    }
  }

  const getRecommendationExplanation = (item: RecommendationItem): string[] => {
    const explanations: string[] = []
    
    if (item.recommendation_reason.includes('cf')) {
      explanations.push('👥 Пользователи с похожими интересами покупали этот товар')
    }
    if (item.recommendation_reason.includes('content')) {
      explanations.push('🔗 Похож на товары, которые вы покупали ранее')
    }
    if (item.recommendation_reason.includes('pop')) {
      explanations.push('🔥 Популярный товар среди всех покупателей')
    }
    
    if (explanations.length === 0) {
      explanations.push('✨ Рекомендован гибридным алгоритмом')
    }
    
    return explanations.slice(0, 2) // Максимум 2 объяснения
  }

  const getSourceBadges = (item: RecommendationItem): { text: string, color: string }[] => {
    const badges: { text: string, color: string }[] = []
    
    if (item.recommendation_reason.includes('cf')) {
      badges.push({ text: 'CF', color: ALGORITHM_COLORS.CF })
    }
    if (item.recommendation_reason.includes('content')) {
      badges.push({ text: 'Content', color: ALGORITHM_COLORS.Content })
    }
    if (item.recommendation_reason.includes('pop')) {
      badges.push({ text: 'Popular', color: ALGORITHM_COLORS.Popularity })
    }
    
    if (badges.length === 0) {
      badges.push({ text: 'Hybrid', color: ALGORITHM_COLORS.Hybrid })
    }
    
    return badges
  }

  if (loading) {
    return (
      <div className="recommendations-page">
        <div className="page-header">
          <h1>🎯 Рекомендации</h1>
          <p>Загрузка данных...</p>
        </div>
        <div className="loading-skeleton">
          {[1, 2, 3].map(i => (
            <div key={i} className="skeleton-card"></div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="recommendations-page">
      <div className="page-header">
        <h1>🎯 Рекомендации</h1>
        <p>Примеры персональных рекомендаций для пользователей</p>
      </div>

      <div className="recommendations-grid">
        {/* Секция 1: Карточки пользователей */}
        <section className="user-cards-section">
          <h2>📊 Примеры пользователей</h2>
          <div className="user-cards-grid">
            {userCards.map(user => (
              <div key={user.user_id} className="user-card">
                <div className="user-header">
                  <h3>Пользователь #{user.user_id}</h3>
                  {user.loading && <span className="loading-badge">Загрузка...</span>}
                  {user.error && <span className="error-badge">Ошибка</span>}
                </div>

                {/* Последние покупки */}
                <div className="user-purchases">
                  <h4>🛒 Последние покупки</h4>
                  <div className="purchases-list">
                    {user.recent_purchases.length > 0 ? 
                      user.recent_purchases.slice(0, 3).map((purchase, idx) => (
                        <div key={idx} className="purchase-item">
                          <span className="purchase-title">{purchase.title}</span>
                          <span className="purchase-price">{(purchase.price / 100).toLocaleString('ru-RU')}₽</span>
                        </div>
                      )) : (
                        <div className="empty-state">
                          <span>История покупок недоступна</span>
                        </div>
                      )
                    }
                  </div>
                </div>

                {/* Рекомендации */}
                <div className="user-recommendations">
                  <h4>✨ Рекомендации Hybrid</h4>
                  <div className="recommendations-list">
                    {user.hybrid_recommendations.length > 0 ?
                      user.hybrid_recommendations.slice(0, 3).map((rec, idx) => (
                        <div key={idx} className="recommendation-item">
                          <div className="rec-header">
                            <div className="rec-title-container">
                              <span className="rec-title">{rec.title}</span>
                              <div className="rec-source-badges">
                                {getSourceBadges(rec).map((badge, i) => (
                                  <span 
                                    key={i} 
                                    className="source-badge"
                                    style={{ backgroundColor: badge.color }}
                                  >
                                    {badge.text}
                                  </span>
                                ))}
                              </div>
                            </div>
                            <span className="rec-score">{(rec.score * 100).toFixed(0)}%</span>
                          </div>
                          <div className="rec-explanations">
                            {getRecommendationExplanation(rec).map((explanation, i) => (
                              <span key={i} className="rec-explanation">{explanation}</span>
                            ))}
                          </div>
                        </div>
                      )) : (
                        <div className="empty-state">
                          <span>Рекомендации недоступны</span>
                        </div>
                      )
                    }
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Секция 2: Сравнение алгоритмов */}
        <section className="algorithms-section">
          <h2>🔍 Сравнение алгоритмов</h2>
          <div className="algorithms-comparison">
            <div className="metrics-table">
              <div className="table-header">
                <span>Алгоритм</span>
                <span>Точность попадания (%)</span>
                <span>Качество (NDCG)</span>
                <span>Покрытие товаров</span>
                <span>Скорость (мс)</span>
              </div>
              {algorithmMetrics.map(metric => (
                <div key={metric.algorithm} className="table-row">
                  <div className="algorithm-cell">
                    <span className="algorithm-name" style={{ color: metric.color }}>
                      {metric.algorithm}
                    </span>
                    <span className="algorithm-description">{metric.description}</span>
                  </div>
                  <span className="metric-value">
                    {metric.hitRate.toFixed(1)}%
                    {metric.algorithm === 'Popularity' && <span className="metric-badge best">Лучший</span>}
                  </span>
                  <span className="metric-value">
                    {metric.ndcg.toFixed(1)}
                    {metric.algorithm === 'Popularity' && <span className="metric-badge best">Лучший</span>}
                  </span>
                  <span className="metric-value">
                    {metric.coverage}
                    {metric.algorithm === 'Hybrid' && <span className="metric-badge best">Лучший</span>}
                  </span>
                  <span className="metric-value">
                    {metric.latency}ms
                    <span className={`metric-badge ${metric.latency <= 20 ? 'good' : metric.latency <= 35 ? 'ok' : 'slow'}`}>
                      {metric.latency <= 20 ? 'Быстро' : metric.latency <= 35 ? 'Норма' : 'Медленно'}
                    </span>
                  </span>
                </div>
              ))}
            </div>
          </div>
        </section>

        {/* Секция 3: ТОП товары */}
        <section className="top-products-section">
          <h2>🛒 ТОП рекомендуемые товары</h2>
          <div className="top-products-chart">
            {topProducts.map((product, idx) => (
              <div key={product.product_id} className="product-bar">
                <div className="product-info">
                  <span className="product-rank">#{idx + 1}</span>
                  <span className="product-title">{product.title}</span>
                  <span className="product-brand">{product.brand}</span>
                </div>
                <div className="product-metrics">
                  <div 
                    className="recommendation-bar"
                    style={{ 
                      width: `${(product.recommendation_count / 160) * 100}%`,
                      backgroundColor: ALGORITHM_COLORS.Hybrid 
                    }}
                  ></div>
                  <span className="recommendation-count">{product.recommendation_count}</span>
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  )
}

export default RecommendationsPage
