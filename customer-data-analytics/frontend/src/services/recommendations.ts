import { httpGet } from './http'

export interface RecommendationItem {
  product_id: number
  title: string
  brand: string
  category: string
  price: number
  score: number
  popularity_score: number
  rating?: number
  recommendation_reason: string
}

export interface UserPurchase {
  product_id: number
  title: string
  brand: string
  category: string
  price: number
  amount: number
  quantity: number
  days_ago: number
  purchase_date: string
}

// Получение гибридных рекомендаций
export async function getHybridRecommendations(userId: number, k: number = 20): Promise<RecommendationItem[]> {
  return httpGet<RecommendationItem[]>('/api/v1/reco/user-hybrid', { user_id: userId, k }, false) // без кеша для демо
}

// Получение рекомендаций CF
export async function getCFRecommendations(userId: number, k: number = 20): Promise<RecommendationItem[]> {
  return httpGet<RecommendationItem[]>('/api/v1/reco/user-cf', { user_id: userId, k }, false)
}

// Получение контентных рекомендаций
export async function getContentRecommendations(userId: number, k: number = 20): Promise<RecommendationItem[]> {
  return httpGet<RecommendationItem[]>('/api/v1/reco/user-content', { user_id: userId, k }, false)
}

// Получение истории покупок пользователя
export async function getUserPurchases(userId: number, limit: number = 5): Promise<UserPurchase[]> {
  return httpGet<UserPurchase[]>('/api/v1/reco/user-purchases', { user_id: userId, limit }, false)
}

// Заглушка для получения топ рекомендуемых товаров
export interface TopProduct {
  product_id: number
  title: string
  brand: string
  recommendation_count: number
  avg_score: number
  sources: string[]
}

export async function getTopRecommendedProducts(limit: number = 10): Promise<TopProduct[]> {
  return httpGet<TopProduct[]>('/api/v1/reco/top-products', { limit }, false)
}

// Метрики алгоритмов (из evaluation результатов)
export interface AlgorithmMetrics {
  algorithm: string
  hitRate: number
  ndcg: number
  coverage: number
  latency: number
  description: string
}

export function getAlgorithmMetrics(): AlgorithmMetrics[] {
  return [
    {
      algorithm: 'Popularity',
      hitRate: 79.0,
      ndcg: 22.3,
      coverage: 20,
      latency: 15,
      description: 'Популярные товары без персонализации'
    },
    {
      algorithm: 'Hybrid',
      hitRate: 9.0,
      ndcg: 1.6,
      coverage: 313,
      latency: 35,
      description: 'Комбинация CF + Content + Popularity'
    },
    {
      algorithm: 'CF (Item-kNN)',
      hitRate: 5.0,
      ndcg: 1.0,
      coverage: 150,
      latency: 25,
      description: 'Коллаборативная фильтрация по ко-покупкам'
    },
    {
      algorithm: 'Content-Based',
      hitRate: 3.0,
      ndcg: 0.8,
      coverage: 280,
      latency: 20,
      description: 'Похожесть по атрибутам товаров'
    }
  ]
}
