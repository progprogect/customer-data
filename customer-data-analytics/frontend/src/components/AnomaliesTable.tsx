/**
 * AnomaliesTable Component
 * Таблица с аномалиями пользователей
 */

import React, { useState, useEffect } from 'react';
import { type UserAnomalyWeekly, anomaliesApi } from '../services/anomalies';

interface AnomaliesTableProps {
}

const AnomaliesTable: React.FC<AnomaliesTableProps> = () => {
  const [anomalies, setAnomalies] = useState<UserAnomalyWeekly[]>([]);
  const [behaviorData, setBehaviorData] = useState<Map<string, any>>(new Map());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalCount, setTotalCount] = useState(0);
  const [weekDate, setWeekDate] = useState<string>('');

  const itemsPerPage = 50;

  useEffect(() => {
    loadAnomalies();
  }, [currentPage]);

  const loadAnomalies = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await anomaliesApi.getWeeklyAnomalies(
        undefined, // последняя неделя с аномалиями
        3.0, // min_score
        itemsPerPage
      );
      
      setAnomalies(response.anomalies);
      setTotalCount(response.total_count);
      setWeekDate(response.week_date);
      
      // Загружаем данные поведения для каждого пользователя
      const behaviorMap = new Map();
      for (const anomaly of response.anomalies) {
        try {
          const userResponse = await anomaliesApi.getUserAnomalies(anomaly.user_id, 2, 0.0);
          if (userResponse.behavior_data.length > 0) {
            const currentWeek = userResponse.behavior_data.find(b => b.week_start === anomaly.week_start);
            const prevWeek = userResponse.behavior_data.find(b => 
              new Date(b.week_start) < new Date(anomaly.week_start)
            );
            
            if (currentWeek) {
              behaviorMap.set(`${anomaly.user_id}-${anomaly.week_start}`, {
                current: currentWeek,
                previous: prevWeek
              });
            }
          }
        } catch (err) {
          console.warn(`Failed to load behavior data for user ${anomaly.user_id}:`, err);
        }
      }
      setBehaviorData(behaviorMap);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка загрузки данных');
    } finally {
      setLoading(false);
    }
  };

  const formatTriggers = (triggers: string[]) => {
    return triggers.map(trigger => {
      const triggerMap: { [key: string]: string } = {
        'z_orders>=3': 'Z-заказы ≥ 3',
        'ratio_orders>=3': 'Ratio-заказы ≥ 3',
        'z_orders<=-3': 'Z-заказы ≤ -3',
        'orders_drop_to_zero': 'Заказы → 0',
        'z_monetary>=3': 'Z-траты ≥ 3',
        'ratio_monetary>=3': 'Ratio-траты ≥ 3',
        'z_monetary<=-3': 'Z-траты ≤ -3'
      };
      return triggerMap[trigger] || trigger;
    }).join(', ');
  };

  const formatAnomalyScore = (score: number) => {
    return score.toFixed(2);
  };

  const getBehaviorContext = (userId: number, weekStart: string) => {
    const key = `${userId}-${weekStart}`;
    const data = behaviorData.get(key);
    if (!data || !data.current) return null;
    
    const { current, previous } = data;
    const ordersChange = previous ? 
      ((current.orders_count - previous.orders_count) / previous.orders_count * 100) : 0;
    const monetaryChange = previous ? 
      ((current.monetary_sum - previous.monetary_sum) / previous.monetary_sum * 100) : 0;
    const categoriesChange = previous ? 
      ((current.categories_count - previous.categories_count) / previous.categories_count * 100) : 0;
    
    return {
      orders: {
        current: current.orders_count,
        previous: previous?.orders_count || 0,
        change: ordersChange
      },
      monetary: {
        current: current.monetary_sum,
        previous: previous?.monetary_sum || 0,
        change: monetaryChange
      },
      categories: {
        current: current.categories_count,
        previous: previous?.categories_count || 0,
        change: categoriesChange
      }
    };
  };

  const totalPages = Math.ceil(totalCount / itemsPerPage);

  if (loading) {
    return (
      <div className="anomalies-table-loading">
        <div className="loading-spinner">⏳</div>
        <p>Загрузка аномалий...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="anomalies-table-error">
        <div className="error-icon">❌</div>
        <p>Ошибка: {error}</p>
        <button onClick={loadAnomalies} className="retry-button">
          Попробовать снова
        </button>
      </div>
    );
  }

  return (
    <div className="anomalies-table">
      <div className="table-header">
        <h3>🚨 Аномалии поведения пользователей</h3>
        <div className="table-info">
          <span>Неделя: {weekDate}</span>
          <span>Всего аномалий: {totalCount}</span>
        </div>
      </div>

      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Пользователь</th>
              <th>Неделя</th>
              <th>Изменения</th>
              <th>Триггеры</th>
            </tr>
          </thead>
          <tbody>
            {anomalies.map((anomaly) => {
              const context = getBehaviorContext(anomaly.user_id, anomaly.week_start);
              return (
                <tr key={`${anomaly.user_id}-${anomaly.week_start}`} className="anomaly-row">
                  <td className="user-cell">
                    <span className="user-id">#{anomaly.user_id}</span>
                  </td>
                  <td className="week-cell">
                    {anomaly.week_start}
                  </td>
                  <td className="changes-cell">
                    {context ? (
                      <div className="behavior-changes">
                        <div className="change-item">
                          <span className="change-icon">🛒</span>
                          <span className="change-text">
                            {context.orders.previous} → {context.orders.current}
                            {context.orders.change !== 0 && (
                              <span className={`change-percent ${context.orders.change > 0 ? 'positive' : 'negative'}`}>
                                {context.orders.change > 0 ? '+' : ''}{context.orders.change.toFixed(0)}%
                              </span>
                            )}
                          </span>
                        </div>
                        <div className="change-item">
                          <span className="change-icon">💰</span>
                          <span className="change-text">
                            ${context.monetary.previous.toFixed(0)} → ${context.monetary.current.toFixed(0)}
                            {context.monetary.change !== 0 && (
                              <span className={`change-percent ${context.monetary.change > 0 ? 'positive' : 'negative'}`}>
                                {context.monetary.change > 0 ? '+' : ''}{context.monetary.change.toFixed(0)}%
                              </span>
                            )}
                          </span>
                        </div>
                        <div className="change-item">
                          <span className="change-icon">📦</span>
                          <span className="change-text">
                            {context.categories.previous} → {context.categories.current}
                            {context.categories.change !== 0 && (
                              <span className={`change-percent ${context.categories.change > 0 ? 'positive' : 'negative'}`}>
                                {context.categories.change > 0 ? '+' : ''}{context.categories.change.toFixed(0)}%
                              </span>
                            )}
                          </span>
                        </div>
                      </div>
                    ) : (
                      <div className="no-context">
                        <span className="score ${anomaly.anomaly_score > 10 ? 'high' : anomaly.anomaly_score > 5 ? 'medium' : 'low'}">
                          Скор: {formatAnomalyScore(anomaly.anomaly_score)}
                        </span>
                      </div>
                    )}
                  </td>
                  <td className="triggers-cell">
                    <div className="triggers">
                      {formatTriggers(anomaly.triggers)}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="pagination">
          <button 
            onClick={() => setCurrentPage(prev => Math.max(1, prev - 1))}
            disabled={currentPage === 1}
            className="pagination-btn"
          >
            ← Назад
          </button>
          
          <span className="pagination-info">
            Страница {currentPage} из {totalPages}
          </span>
          
          <button 
            onClick={() => setCurrentPage(prev => Math.min(totalPages, prev + 1))}
            disabled={currentPage === totalPages}
            className="pagination-btn"
          >
            Вперед →
          </button>
        </div>
      )}

      {anomalies.length === 0 && (
        <div className="no-anomalies">
          <div className="no-data-icon">✅</div>
          <p>Аномалий не найдено</p>
        </div>
      )}
    </div>
  );
};

export default AnomaliesTable;
