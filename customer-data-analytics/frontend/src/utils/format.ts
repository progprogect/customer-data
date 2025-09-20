const numberRu = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 0 })
const percentRu = new Intl.NumberFormat('ru-RU', { minimumFractionDigits: 1, maximumFractionDigits: 1 })

export function formatInt(n: number) {
  return numberRu.format(n)
}

export function formatPercentFraction01(fraction: number) {
  return percentRu.format(fraction * 100) + '%'
}

export function toISODate(d: Date) {
  return d.toISOString().slice(0, 10)
}

export const TIMEZONE_LABEL = 'Europe/Warsaw'



