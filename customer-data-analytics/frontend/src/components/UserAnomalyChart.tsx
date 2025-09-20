/**
 * UserAnomalyChart Component
 * График динамики поведения пользователя с подсветкой аномалий
 */

import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { anomaliesApi } from '../services/anomalies';

interface UserAnomalyChartProps {
  userId: number;
  onClose: () => void;
}

interface ChartDataPoint {
  week_start: string;
  orders_count: number;
  monetary_sum: number;
  categories_count: number;
  anomaly_score: number;
  is_anomaly: boolean;
  triggers: string[];
}

const UserAnomalyChart: React.FC<UserAnomalyChartProps> = ({ userId, onClose }) => {
  const [chartData, setChartData] = useState<ChartDataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [userStats, setUserStats] = useState<{
    total_anomalies: number;
    anomaly_rate: number;
    top_triggers: Array<{ trigger: string; count: number }>;
  } | null>(null);

  useEffect(() => {
    loadUserData();
  }, [userId]);

  const loadUserData = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await anomaliesApi.getUserAnomalies(userId, 20, 0.0);
      
      // Преобразуем данные для графика
      const chartData = response.anomalies.map(anomaly => ({
        week_start: anomaly.week_start,
        orders_count: 0, // Будем получать из behavior_weekly
        monetary_sum: 0, // Будем получать из behavior_weekly
        categories_count: 0, // Будем получать из behavior_weekly
        anomaly_score: anomaly.anomaly_score,
        is_anomaly: anomaly.is_anomaly,
        triggers: anomaly.triggers
      }));

      setChartData(chartData);
      setUserStats({
        total_anomalies: response.total_anomalies,
        anomaly_rate: response.anomaly_rate,
        top_triggers: response.top_triggers
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка загрузки данных');
    } finally {
      setLoading(false);
    }
  };

  const formatTriggers = (triggers: string[]) => {
    const triggerMap: { [key: string]: string } = {
      'z_orders>=3': 'Z-заказы ≥ 3',
      'ratio_orders>=3': 'Ratio-заказы ≥ 3',
      'z_orders<=-3': 'Z-заказы ≤ -3',
      'orders_drop_to_zero': 'Заказы → 0',
      'z_monetary>=3': 'Z-траты ≥ 3',
      'ratio_monetary>=3': 'Ratio-траты ≥ 3',
      'z_monetary<=-3': 'Z-траты ≤ -3'
    };
    return triggers.map(trigger => triggerMap[trigger] || trigger).join(', ');
  };

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="custom-tooltip">
          <p className="tooltip-label">Неделя: {label}</p>
          <p className="tooltip-item">Заказы: {data.orders_count}</p>
          <p className="tooltip-item">Сумма: {data.monetary_sum.toFixed(2)}</p>
          <p className="tooltip-item">Категории: {data.categories_count}</p>
          {data.is_anomaly && (
            <>
              <p className="tooltip-anomaly">🚨 Аномалия!</p>
              <p className="tooltip-triggers">
                Триггеры: {formatTriggers(data.triggers)}
              </p>
            </>
          )}
        </div>
      );
    }
    return null;
  };

  if (loading) {
    return (
      <div className="user-chart-modal">
        <div className="modal-content">
          <div className="modal-header">
            <h3>📊 Детализация пользователя #{userId}</h3>
            <button onClick={onClose} className="close-btn">×</button>
          </div>
          <div className="modal-body">
            <div className="loading-spinner">⏳</div>
            <p>Загрузка данных...</p>
          </div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="user-chart-modal">
        <div className="modal-content">
          <div className="modal-header">
            <h3>📊 Детализация пользователя #{userId}</h3>
            <button onClick={onClose} className="close-btn">×</button>
          </div>
          <div className="modal-body">
            <div className="error-icon">❌</div>
            <p>Ошибка: {error}</p>
            <button onClick={loadUserData} className="retry-button">
              Попробовать снова
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="user-chart-modal">
      <div className="modal-content">
        <div className="modal-header">
          <h3>📊 Детализация пользователя #{userId}</h3>
          <button onClick={onClose} className="close-btn">×</button>
        </div>
        
        <div className="modal-body">
          {userStats && (
            <div className="user-stats">
              <div className="stat-card">
                <span className="stat-label">Аномалий:</span>
                <span className="stat-value">{userStats.total_anomalies}</span>
              </div>
              <div className="stat-card">
                <span className="stat-label">Процент аномалий:</span>
                <span className="stat-value">{userStats.anomaly_rate.toFixed(1)}%</span>
              </div>
            </div>
          )}

          <div className="chart-container">
            <h4>Динамика поведения</h4>
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis 
                  dataKey="week_start" 
                  tick={{ fontSize: 12 }}
                  angle={-45}
                  textAnchor="end"
                  height={60}
                />
                <YAxis />
                <Tooltip content={<CustomTooltip />} />
                <Legend />
                
                <Line 
                  type="monotone" 
                  dataKey="anomaly_score" 
                  stroke="#8884d8" 
                  strokeWidth={2}
                  name="Скор аномалии"
                  dot={(props: any) => {
                    const { cx, cy, payload } = props;
                    return (
                      <circle
                        cx={cx}
                        cy={cy}
                        r={4}
                        fill={payload.is_anomaly ? "#ff4444" : "#8884d8"}
                        stroke={payload.is_anomaly ? "#cc0000" : "#6666cc"}
                        strokeWidth={2}
                      />
                    );
                  }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {userStats && userStats.top_triggers.length > 0 && (
            <div className="top-triggers">
              <h4>🔥 Топ триггеров</h4>
              <div className="triggers-list">
                {userStats.top_triggers.map((trigger, index) => (
                  <div key={index} className="trigger-item">
                    <span className="trigger-name">{trigger.trigger}</span>
                    <span className="trigger-count">{trigger.count}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default UserAnomalyChart;
