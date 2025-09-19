import { Doughnut } from 'react-chartjs-2'
import './charts/ChartSetup'
import type { DistributionResponse, ClusterMeta } from '../services/segments'
import { colorByClusterId } from '../utils/colors'
import { formatInt, formatPercentFraction01, TIMEZONE_LABEL } from '../utils/format'

export default function DistributionCard({ 
  data, 
  title, 
  meta, 
  hiddenClusters = [] 
}: { 
  data: DistributionResponse; 
  title: string; 
  meta?: ClusterMeta[];
  hiddenClusters?: number[];
}) {
  // Фильтруем сегменты по скрытым кластерам
  const visibleSegments = data.segments.filter(s => !hiddenClusters.includes(s.cluster_id))
  
  const chartData = {
    labels: visibleSegments.map((s) => meta?.find(m => m.id === s.cluster_id)?.name ?? `#${s.cluster_id}`),
    datasets: [
      {
        data: visibleSegments.map((s) => Math.max(s.share * 100, 0)),
        backgroundColor: visibleSegments.map((s) => colorByClusterId(s.cluster_id)),
        borderWidth: 0,
      },
    ],
  }
  return (
    <section className="card" style={{ gridColumn: 'span 6' }}>
      <h3>{title}</h3>
      <div className="muted">Последнее обновление: {data.date}; TZ: {TIMEZONE_LABEL}</div>
      <div style={{ display: 'flex', gap: 16, alignItems: 'center', flexWrap: 'wrap' }}>
        <div style={{ width: 260, height: 260 }}>
          <Doughnut data={chartData} options={{ plugins: { legend: { display: false } } }} />
        </div>
        <div>
          <table>
            <thead>
              <tr>
                <th style={{ textAlign: 'left' }}>Кластер</th>
                <th style={{ textAlign: 'right' }}>%</th>
                <th style={{ textAlign: 'right' }}>Пользователи</th>
              </tr>
            </thead>
            <tbody>
              {visibleSegments.map((s) => (
                <tr key={s.cluster_id}>
                  <td>{meta?.find(m => m.id === s.cluster_id)?.name ?? `#${s.cluster_id}`}</td>
                  <td style={{ textAlign: 'right' }}>{formatPercentFraction01(s.share)}</td>
                  <td style={{ textAlign: 'right' }}>{formatInt(s.users_count)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  )
}


