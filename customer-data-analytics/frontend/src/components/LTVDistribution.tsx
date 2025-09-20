/**
 * LTVDistribution Component
 * Компонент для отображения распределения LTV
 */

import React, { useState } from 'react';

interface LTVDistributionData {
  range_min: number;
  range_max: number;
  count: number;
  percentage: number;
}

interface LTVDistributionResponse {
  distribution: LTVDistributionData[];
  total_users: number;
  metric_type: string;
}

interface LTVDistributionProps {
  data: LTVDistributionResponse;
  onMetricChange: (metricType: string) => void;
}

const LTVDistribution: React.FC<LTVDistributionProps> = ({ data, onMetricChange }) => {
  const [selectedMetric, setSelectedMetric] = useState(data.metric_type);

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('ru-RU', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  };

  const formatNumber = (value: number) => {
    return new Intl.NumberFormat('ru-RU').format(value);
  };

  const getMetricLabel = (metricType: string) => {
    switch (metricType) {
      case 'revenue_3m':
        return 'LTV за 3 месяца';
      case 'revenue_6m':
        return 'LTV за 6 месяцев';
      case 'revenue_12m':
        return 'LTV за 12 месяцев';
      case 'lifetime_revenue':
        return 'LTV за весь период';
      default:
        return 'LTV';
    }
  };

  const handleMetricChange = (metricType: string) => {
    setSelectedMetric(metricType);
    onMetricChange(metricType);
  };

  const maxCount = Math.max(...data.distribution.map(d => d.count));

  return (
    <div>
      <div className="section-header">
        <h3>📈 Распределение LTV</h3>
        <div className="input-group">
          <label>Метрика:</label>
          <select 
            value={selectedMetric} 
            onChange={(e) => handleMetricChange(e.target.value)}
          >
            <option value="revenue_3m">LTV за 3 месяца</option>
            <option value="revenue_6m">LTV за 6 месяцев</option>
            <option value="revenue_12m">LTV за 12 месяцев</option>
            <option value="lifetime_revenue">LTV за весь период</option>
          </select>
        </div>
      </div>

      <div className="cards-grid">
        <div className="card">
          <h4>Всего пользователей</h4>
          <p className="metric-value">{formatNumber(data.total_users)}</p>
        </div>
        <div className="card">
          <h4>Диапазонов</h4>
          <p className="metric-value">{data.distribution.length}</p>
        </div>
      </div>

      <div className="chart-section">
        <h4>Распределение {getMetricLabel(selectedMetric)}</h4>
        
        <div className="chart-container">
          {data.distribution.map((item, index) => {
            const height = (item.count / maxCount) * 100;
            const isHighlighted = item.percentage > 10;
            
            return (
              <div key={index} className="chart-bar">
                <div 
                  className={`bar ${isHighlighted ? 'highlighted' : ''}`}
                  style={{ height: `${height}%` }}
                  title={`${formatNumber(item.count)} пользователей (${item.percentage.toFixed(1)}%)`}
                />
                <div className="bar-label">
                  <div>{formatCurrency(item.range_min)} - {formatCurrency(item.range_max)}</div>
                  <div>{formatNumber(item.count)} ({item.percentage.toFixed(1)}%)</div>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      <div className="insights-section">
        <h4>💡 Инсайты по распределению</h4>
        <div className="cards-grid">
          <div className="card">
            <h4>Самый популярный диапазон</h4>
            <p>
              {formatCurrency(data.distribution.reduce((max, item) => 
                item.count > max.count ? item : max, data.distribution[0]
              ).range_min)} - {formatCurrency(data.distribution.reduce((max, item) => 
                item.count > max.count ? item : max, data.distribution[0]
              ).range_max)}
            </p>
          </div>
          <div className="card">
            <h4>Пользователей с LTV &gt; $100</h4>
            <p>
              {formatNumber(
                data.distribution
                  .filter(item => item.range_min >= 100)
                  .reduce((sum, item) => sum + item.count, 0)
              )}
            </p>
          </div>
          <div className="card">
            <h4>VIP клиенты (LTV &gt; $1000)</h4>
            <p>
              {formatNumber(
                data.distribution
                  .filter(item => item.range_min >= 1000)
                  .reduce((sum, item) => sum + item.count, 0)
              )}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LTVDistribution;
