/**
 * LTVSummary Component
 * Компонент для отображения сводной статистики LTV
 */

import React from 'react';

interface LTVSummaryData {
  metric_name: string;
  value_3m: number;
  value_6m: number;
  value_12m: number;
  value_lifetime: number;
}

interface LTVSummaryResponse {
  summary: LTVSummaryData[];
  total_users: number;
  calculated_at: string;
}

interface LTVSummaryProps {
  data: LTVSummaryResponse;
}

const LTVSummary: React.FC<LTVSummaryProps> = ({ data }) => {
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('ru-RU', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(value);
  };

  const formatNumber = (value: number) => {
    return new Intl.NumberFormat('ru-RU').format(value);
  };

  const getMetricIcon = (metricName: string) => {
    switch (metricName) {
      case 'Average LTV':
        return '📊';
      case 'Median LTV':
        return '📈';
      case '95th Percentile LTV':
        return '🎯';
      case 'Users with Orders':
        return '👥';
      default:
        return '📋';
    }
  };

  const getMetricDescription = (metricName: string) => {
    switch (metricName) {
      case 'Average LTV':
        return 'Средняя ценность клиента';
      case 'Median LTV':
        return 'Медианная ценность клиента (убирает перекос от VIP)';
      case '95th Percentile LTV':
        return '95-й перцентиль (топ 5% клиентов)';
      case 'Users with Orders':
        return 'Количество клиентов с заказами';
      default:
        return '';
    }
  };

  return (
    <div>
      <div className="section-header">
        <h2>📊 Сводка LTV</h2>
        <div className="section-meta">
          <span>👥 Всего пользователей: <strong>{formatNumber(data.total_users)}</strong></span>
          <span>🕒 Обновлено: {new Date(data.calculated_at).toLocaleString('ru-RU')}</span>
        </div>
      </div>

      <div className="cards-grid">
        {data.summary.map((metric, index) => (
          <div key={index} className="card">
            <div className="card-header">
              <span className="card-icon">{getMetricIcon(metric.metric_name)}</span>
              <h3>{metric.metric_name}</h3>
            </div>
            <p className="card-description">{getMetricDescription(metric.metric_name)}</p>
            
            <div className="metric-list">
              <div className="metric-item">
                <span>3 месяца:</span>
                <span className="metric-value">
                  {metric.metric_name === 'Users with Orders' 
                    ? formatNumber(metric.value_3m)
                    : formatCurrency(metric.value_3m)
                  }
                </span>
              </div>
              <div className="metric-item">
                <span>6 месяцев:</span>
                <span className="metric-value">
                  {metric.metric_name === 'Users with Orders' 
                    ? formatNumber(metric.value_6m)
                    : formatCurrency(metric.value_6m)
                  }
                </span>
              </div>
              <div className="metric-item">
                <span>12 месяцев:</span>
                <span className="metric-value">
                  {metric.metric_name === 'Users with Orders' 
                    ? formatNumber(metric.value_12m)
                    : formatCurrency(metric.value_12m)
                  }
                </span>
              </div>
              <div className="metric-item highlight">
                <span>За весь период:</span>
                <span className="metric-value">
                  {metric.metric_name === 'Users with Orders' 
                    ? formatNumber(metric.value_lifetime)
                    : formatCurrency(metric.value_lifetime)
                  }
                </span>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="insights-section">
        <h3>💡 Ключевые инсайты</h3>
        <div className="cards-grid">
          <div className="card">
            <h4>🎯 Рекомендуемый CAC</h4>
            <p>
              На основе среднего LTV за 6 месяцев, рекомендуемый CAC (Customer Acquisition Cost) 
              не должен превышать <strong>
                {formatCurrency(data.summary.find(m => m.metric_name === 'Average LTV')?.value_6m || 0)}
              </strong>
            </p>
          </div>
          <div className="card">
            <h4>📈 Рост ценности</h4>
            <p>
              Клиенты приносят в среднем <strong>
                {formatCurrency(data.summary.find(m => m.metric_name === 'Average LTV')?.value_12m || 0)}
              </strong> за год, что в <strong>
                {((data.summary.find(m => m.metric_name === 'Average LTV')?.value_12m || 0) / 
                  (data.summary.find(m => m.metric_name === 'Average LTV')?.value_6m || 1)).toFixed(1)}x
              </strong> больше чем за полгода
            </p>
          </div>
          <div className="card">
            <h4>👥 Активность клиентов</h4>
            <p>
              <strong>
                {((data.summary.find(m => m.metric_name === 'Users with Orders')?.value_lifetime || 0) / 
                  data.total_users * 100).toFixed(1)}%
              </strong> клиентов совершили хотя бы одну покупку
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LTVSummary;
