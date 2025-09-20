/**
 * AnomaliesTable Component
 * Таблица с аномалиями пользователей
 */

import React, { useState, useEffect } from 'react';
import { type UserAnomalyWeekly, anomaliesApi } from '../services/anomalies';

interface AnomaliesTableProps {
  onUserClick: (userId: number) => void;
}

const AnomaliesTable: React.FC<AnomaliesTableProps> = ({ onUserClick }) => {
  const [anomalies, setAnomalies] = useState<UserAnomalyWeekly[]>([]);
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
              <th>Скор аномалии</th>
              <th>Триггеры</th>
              <th>Действия</th>
            </tr>
          </thead>
          <tbody>
            {anomalies.map((anomaly) => (
              <tr key={`${anomaly.user_id}-${anomaly.week_start}`} className="anomaly-row">
                <td className="user-cell">
                  <span className="user-id">#{anomaly.user_id}</span>
                </td>
                <td className="week-cell">
                  {anomaly.week_start}
                </td>
                <td className="score-cell">
                  <span className={`score ${anomaly.anomaly_score > 10 ? 'high' : anomaly.anomaly_score > 5 ? 'medium' : 'low'}`}>
                    {formatAnomalyScore(anomaly.anomaly_score)}
                  </span>
                </td>
                <td className="triggers-cell">
                  <div className="triggers">
                    {formatTriggers(anomaly.triggers)}
                  </div>
                </td>
                <td className="actions-cell">
                  <button 
                    onClick={() => onUserClick(anomaly.user_id)}
                    className="view-details-btn"
                  >
                    📊 Детали
                  </button>
                </td>
              </tr>
            ))}
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
