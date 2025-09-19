import { Line } from 'react-chartjs-2'
import './charts/ChartSetup'
import type { DynamicsResponse, ClusterMeta } from '../services/segments'
import { colorByClusterId } from '../utils/colors'
import { TIMEZONE_LABEL } from '../utils/format'

export default function DynamicsCard({ 
  data, 
  title, 
  meta, 
  hiddenClusters = [] 
}: { 
  data: DynamicsResponse; 
  title: string; 
  meta?: ClusterMeta[];
  hiddenClusters?: number[];
}) {
  // Фильтруем серии по скрытым кластерам
  const visibleSeries = data.series.filter(s => !hiddenClusters.includes(s.cluster_id))
  
  const labels = Array.from(new Set(visibleSeries.flatMap(s => s.points.map(p => p.date)))).sort()
  const chartData = {
    labels,
    datasets: visibleSeries.map(s => ({
      label: meta?.find(m => m.id === s.cluster_id)?.name ?? `#${s.cluster_id}`,
      data: labels.map(l => s.points.find(p => p.date === l)?.count ?? null),
      borderColor: colorByClusterId(s.cluster_id),
      backgroundColor: colorByClusterId(s.cluster_id) + '33',
      tension: 0.25,
      pointRadius: 0,
    })),
  }
  return (
    <section className="card" style={{ gridColumn: 'span 6' }}>
      <h3>{title}</h3>
      <div className="muted">Период: {data.from_date} → {data.to_date}; TZ: {TIMEZONE_LABEL}</div>
      <Line data={chartData} options={{ plugins: { legend: { position: 'bottom' } }, scales: { x: { ticks: { maxRotation: 0 } } } }} />
    </section>
  )
}


