// ML API Service
// Сервис для работы с API машинного обучения

// API конфигурация
const ML_API_BASE = '/api/v1/ml'
const API_KEY = 'dev-token-12345' // В prod должен быть из переменных окружения

// Типы данных
export interface UserFeatures {
  recency_days?: number
  frequency_90d?: number
  monetary_180d?: number
  aov_180d?: number
  orders_lifetime?: number
  revenue_lifetime?: number
  categories_unique?: number
}

export interface PredictionRow {
  user_id: number
  snapshot_date: string
  features: UserFeatures
}

export interface PredictionResult {
  user_id: number
  snapshot_date: string
  prob_next_30d: number | null
  threshold_applied: boolean
  will_target?: boolean | null
  error?: string | null
}

export interface PredictionResponse {
  model_version: string
  count: number
  successful_predictions: number
  failed_predictions: number
  processing_time_ms: number
  results: PredictionResult[]
}

export interface ModelInfo {
  model_version: string
  load_timestamp: string
  feature_names: string[]
  feature_count: number
  model_performance: {
    precision?: number
    recall?: number
    f1_score?: number
    roc_auc?: number
  }
}

export interface MLHealthStatus {
  status: 'healthy' | 'unhealthy'
  model_version?: string
  model_loaded?: boolean
  timestamp: string
  reason?: string
}

// HTTP утилиты для ML API
async function mlFetch<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const url = `${ML_API_BASE}${endpoint}`
  
  const response = await fetch(url, {
    ...options,
    headers: {
      'Authorization': `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
      ...options.headers,
    },
  })

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'Unknown error')
    throw new Error(`ML API Error (${response.status}): ${errorText}`)
  }

  return response.json() as T
}

// Основные функции API

/**
 * Получение информации о модели
 */
export async function getModelInfo(): Promise<ModelInfo> {
  return mlFetch<ModelInfo>('/model-info')
}

/**
 * Проверка здоровья ML сервиса
 */
export async function getMLHealth(): Promise<MLHealthStatus> {
  return mlFetch<MLHealthStatus>('/health')
}

/**
 * Предсказание вероятности покупки для пользователей
 */
export async function getPurchaseProbabilities(
  rows: PredictionRow[],
  modelVersion?: string
): Promise<PredictionResponse> {
  const payload = {
    model_version: modelVersion,
    rows
  }

  return mlFetch<PredictionResponse>('/purchase-probability', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

/**
 * Предсказание с применением порога
 */
export async function getPurchaseProbabilitiesWithThreshold(
  rows: PredictionRow[],
  threshold: number,
  modelVersion?: string
): Promise<PredictionResponse> {
  const payload = {
    model_version: modelVersion,
    threshold,
    rows
  }

  return mlFetch<PredictionResponse>('/purchase-probability/apply-threshold', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

// Утилиты для работы с данными

/**
 * Получение общей статистики базы данных
 */
export async function getDatabaseStatistics(): Promise<{
  snapshot_date: string
  total_users_in_db: number
  users_with_orders: number
  users_with_features: number
  sample_size_used: number
}> {
  const response = await fetch('/api/v1/database-stats')

  if (!response.ok) {
    throw new Error(`Ошибка получения статистики БД: ${response.status}`)
  }
  return response.json()
}

/**
 * Получение реальных пользователей с их фичами из API (случайная выборка)
 */
export async function getRealUsersWithFeatures(limit: number = 500): Promise<{
  users: Array<{
    user_id: number
    snapshot_date: string
    features: UserFeatures
    explanation: string
  }>
  total_count: number
  snapshot_date: string
}> {
  const response = await fetch(`/api/v1/direct-users?limit=${limit}`)

  if (!response.ok) {
    throw new Error(`Ошибка получения реальных пользователей: ${response.status}`)
  }

  return response.json()
}

/**
 * Создание тестовых пользователей для демонстрации (оставляем для fallback)
 */
export function createTestUsers(count: number = 100): PredictionRow[] {
  const users: PredictionRow[] = []
  const today = new Date().toISOString().split('T')[0]

  for (let i = 1; i <= count; i++) {
    // Генерируем разнообразные профили пользователей
    const userType = Math.random()
    let features: UserFeatures

    if (userType < 0.3) {
      // VIP клиенты (30%)
      features = {
        recency_days: Math.random() * 15 + 1, // 1-15 дней
        frequency_90d: Math.floor(Math.random() * 10) + 5, // 5-15 заказов
        monetary_180d: Math.random() * 3000 + 1000, // $1000-4000
        aov_180d: Math.random() * 200 + 100, // $100-300
        orders_lifetime: Math.floor(Math.random() * 20) + 10, // 10-30 заказов
        revenue_lifetime: Math.random() * 8000 + 2000, // $2000-10000
        categories_unique: Math.floor(Math.random() * 5) + 5, // 5-10 категорий
      }
    } else if (userType < 0.6) {
      // Активные клиенты (30%)
      features = {
        recency_days: Math.random() * 30 + 10, // 10-40 дней
        frequency_90d: Math.floor(Math.random() * 5) + 2, // 2-7 заказов
        monetary_180d: Math.random() * 1500 + 300, // $300-1800
        aov_180d: Math.random() * 150 + 50, // $50-200
        orders_lifetime: Math.floor(Math.random() * 10) + 3, // 3-13 заказов
        revenue_lifetime: Math.random() * 3000 + 500, // $500-3500
        categories_unique: Math.floor(Math.random() * 3) + 2, // 2-5 категорий
      }
    } else {
      // Редкие клиенты (40%)
      features = {
        recency_days: Math.random() * 180 + 60, // 60-240 дней
        frequency_90d: Math.floor(Math.random() * 2), // 0-1 заказ
        monetary_180d: Math.random() * 500 + 50, // $50-550
        aov_180d: Math.random() * 100 + 30, // $30-130
        orders_lifetime: Math.floor(Math.random() * 3) + 1, // 1-4 заказа
        revenue_lifetime: Math.random() * 800 + 100, // $100-900
        categories_unique: Math.floor(Math.random() * 2) + 1, // 1-3 категории
      }
    }

    users.push({
      user_id: i,
      snapshot_date: today,
      features
    })
  }

  return users
}

/**
 * Группировка результатов по уровням вероятности
 */
export function groupByProbabilityLevel(results: PredictionResult[]) {
  const levels = {
    high: { min: 0.6, max: 1.0, count: 0, users: [] as PredictionResult[] },
    medium: { min: 0.3, max: 0.6, count: 0, users: [] as PredictionResult[] },
    low: { min: 0.0, max: 0.3, count: 0, users: [] as PredictionResult[] },
  }

  results.forEach(result => {
    if (result.prob_next_30d === null) return

    const prob = result.prob_next_30d
    if (prob >= 0.6) {
      levels.high.count++
      levels.high.users.push(result)
    } else if (prob >= 0.3) {
      levels.medium.count++
      levels.medium.users.push(result)
    } else {
      levels.low.count++
      levels.low.users.push(result)
    }
  })

  return levels
}

/**
 * Получение человеко-понятного объяснения для пользователя
 */
export function getUserExplanation(user: PredictionResult, features: UserFeatures): string {
  const explanations: string[] = []

  // Проверяем ключевые факторы
  if (features.orders_lifetime && features.orders_lifetime >= 10) {
    explanations.push(`${features.orders_lifetime} заказов за всё время`)
  }

  if (features.categories_unique && features.categories_unique >= 5) {
    explanations.push(`покупает в ${features.categories_unique} категориях`)
  }

  if (features.recency_days && features.recency_days <= 15) {
    explanations.push(`покупал недавно (${Math.round(features.recency_days)} дней назад)`)
  }

  if (features.frequency_90d && features.frequency_90d >= 3) {
    explanations.push(`${features.frequency_90d} заказов за 90 дней`)
  }

  if (features.monetary_180d && features.monetary_180d >= 1000) {
    explanations.push(`потратил $${Math.round(features.monetary_180d)} за полгода`)
  }

  // Если нет ярких факторов, добавляем общие
  if (explanations.length === 0) {
    if (features.orders_lifetime && features.orders_lifetime > 1) {
      explanations.push(`${features.orders_lifetime} заказов за всё время`)
    } else {
      explanations.push('новый клиент')
    }
  }

  return explanations.slice(0, 2).join(', ') // Максимум 2 фактора
}

/**
 * Форматирование процентов
 */
export function formatProbability(prob: number | null): string {
  if (prob === null) return 'N/A'
  return `${Math.round(prob * 100)}%`
}

/**
 * Получение цвета для уровня вероятности
 */
export function getProbabilityColor(prob: number | null): string {
  if (prob === null) return '#94a3b8' // gray
  if (prob >= 0.6) return '#10b981' // green
  if (prob >= 0.3) return '#f59e0b' // yellow
  return '#ef4444' // red
}

/**
 * Интерпретация важности признаков для маркетинга
 */
export function getFeatureInterpretation(featureName: string): { 
  label: string 
  description: string 
  icon: string 
} {
  const interpretations: Record<string, { label: string; description: string; icon: string }> = {
    'orders_lifetime': {
      label: 'История лояльности',
      description: 'Сколько заказов клиент сделал за всё время',
      icon: '🏆'
    },
    'categories_unique': {
      label: 'Разнообразие покупок',
      description: 'В скольких категориях покупает клиент',
      icon: '🛍️'
    },
    'frequency_90d': {
      label: 'Недавняя активность',
      description: 'Количество заказов за последние 90 дней',
      icon: '📈'
    },
    'recency_days': {
      label: 'Время с последней покупки',
      description: 'Сколько дней прошло с последнего заказа',
      icon: '⏰'
    },
    'monetary_180d': {
      label: 'Потраченная сумма',
      description: 'Сколько денег потратил за полгода',
      icon: '💰'
    },
    'aov_180d': {
      label: 'Средний чек',
      description: 'Средняя сумма одного заказа',
      icon: '🧾'
    },
    'revenue_lifetime': {
      label: 'Общая ценность',
      description: 'Вся выручка от клиента за всё время',
      icon: '💎'
    }
  }

  return interpretations[featureName] || {
    label: featureName,
    description: 'Неизвестный признак',
    icon: '❓'
  }
}
