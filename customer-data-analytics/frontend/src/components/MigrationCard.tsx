import type { MigrationResponse, ClusterMeta } from '../services/segments'
import { TIMEZONE_LABEL } from '../utils/format'

export default function MigrationCard({ 
  data, 
  title, 
  meta, 
  hiddenClusters = [] 
}: { 
  data: MigrationResponse; 
  title: string; 
  meta?: ClusterMeta[];
  hiddenClusters?: number[];
}) {
  if (!data.available || data.matrix.length === 0) {
    return (
      <section className="card" style={{ gridColumn: 'span 12' }}>
        <h3>{title}</h3>
        <div className="muted">Дата: {data.date}; TZ: {TIMEZONE_LABEL}</div>
        <div>Данные недоступны за выбранную дату. Попробуйте другую дату.</div>
      </section>
    )
  }

  // Фильтруем матрицу по скрытым кластерам
  const visibleMatrix = data.matrix.filter(c => 
    !hiddenClusters.includes(c.from_cluster) && !hiddenClusters.includes(c.to_cluster)
  )
  
  const clusters = Array.from(new Set(visibleMatrix.flatMap(c => [c.from_cluster, c.to_cluster]))).sort((a,b)=>a-b)
  const table: Record<number, Record<number, number>> = {}
  for (const c of visibleMatrix) {
    table[c.from_cluster] = table[c.from_cluster] || {}
    table[c.from_cluster][c.to_cluster] = c.count
  }
  
  const getClusterName = (clusterId: number) => meta?.find(m => m.id === clusterId)?.name ?? `#${clusterId}`

  return (
    <section className="card" style={{ gridColumn: 'span 12' }}>
      <h3>{title}</h3>
      <div className="muted">Дата: {data.date}; TZ: {TIMEZONE_LABEL}</div>
      <div style={{ overflowX: 'auto' }}>
        <table>
          <thead>
            <tr>
              <th>from↓ to→</th>
              {clusters.map((c) => (
                <th key={c}>{getClusterName(c)}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {clusters.map((from) => (
              <tr key={from}>
                <td>{getClusterName(from)}</td>
                {clusters.map((to) => (
                  <td key={to} style={{ textAlign: 'right' }}>{table[from]?.[to] ?? 0}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}


