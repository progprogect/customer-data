type HttpMethod = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'

const cache = new Map<string, unknown>()

function buildKey(url: string, params?: Record<string, unknown>): string {
  const qp = params ? '?' + new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v ?? '')])).toString() : ''
  return url + qp
}

export async function httpGet<T>(url: string, params?: Record<string, unknown>, useCache = true): Promise<T> {
  const key = buildKey(url, params)
  if (useCache && cache.has(key)) {
    return cache.get(key) as T
  }

  const fullUrl = new URL(url, window.location.origin)
  if (params) {
    for (const [k, v] of Object.entries(params)) fullUrl.searchParams.set(k, String(v ?? ''))
  }

  const res = await fetch(fullUrl.toString(), { method: 'GET' satisfies HttpMethod })
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`HTTP ${res.status}: ${text || res.statusText}`)
  }
  const data = (await res.json()) as T
  if (useCache) cache.set(key, data)
  return data
}

export function invalidateCache(prefix?: string) {
  if (!prefix) return cache.clear()
  for (const k of cache.keys()) if (k.startsWith(prefix)) cache.delete(k)
}


