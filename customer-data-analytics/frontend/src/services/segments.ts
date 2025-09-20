import { httpGet } from './http'

export type ClusterMeta = { id: number; name: string; description?: string }

export type DistributionItem = { cluster_id: number; users_count: number; share: number }
export type DistributionResponse = {
  date: string
  timezone: string
  available: boolean
  total_users: number
  segments: DistributionItem[]
  last_available_date?: string
  note?: string
}

export type DynamicsPoint = { date: string; count: number }
export type DynamicsSeries = { cluster_id: number; points: DynamicsPoint[] }
export type DynamicsResponse = {
  from_date: string
  to_date: string
  granularity: 'day' | 'week' | 'month'
  timezone: string
  available: boolean
  series: DynamicsSeries[]
  note?: string
}

export type MigrationCell = { from_cluster: number; to_cluster: number; count: number }
export type MigrationResponse = {
  date: string
  timezone: string
  available: boolean
  matrix: MigrationCell[]
  note?: string
}

const API_BASE = '/api/v1/segments'

export function fetchMeta() {
  return httpGet<ClusterMeta[]>(`${API_BASE}/meta`)
}

export function fetchDistribution(params?: { date?: string }) {
  return httpGet<DistributionResponse>(`${API_BASE}/distribution`, params)
}

export function fetchDynamics(params: { from_date: string; to_date: string; granularity: 'day' | 'week' | 'month' }) {
  return httpGet<DynamicsResponse>(`${API_BASE}/dynamics`, params)
}

export function fetchMigration(params: { date: string }) {
  return httpGet<MigrationResponse>(`${API_BASE}/migration`, params)
}



