/**
 * LTVPage Component
 * Страница с анализом LTV (Lifetime Value) клиентов
 */

import React, { useEffect, useState } from 'react';
import LTVSummary from '../components/LTVSummary';
// import './LTVPage.css'; // Используем глобальные стили

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

const LTVPage: React.FC = () => {
  const [summaryData, setSummaryData] = useState<LTVSummaryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Загрузка данных
  const fetchSummaryData = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/ltv/summary');
      if (!response.ok) throw new Error('Ошибка загрузки LTV статистики');
      const data = await response.json();
      setSummaryData(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка загрузки данных');
    }
  };


  // Загрузка данных при монтировании
  useEffect(() => {
    fetchSummaryData();
  }, []);

  const handleRecalculate = async () => {
    try {
      setLoading(true);
      const response = await fetch('http://localhost:8000/api/v1/ltv/calculate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({})
      });
      
      if (!response.ok) throw new Error('Ошибка пересчета LTV');
      
      // Перезагружаем данные после пересчета
      await fetchSummaryData();
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Ошибка пересчета LTV');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="page">
      <div className="page-header">
        <h1>💰 LTV (Lifetime Value) Анализ</h1>
        <p className="page-description">
          Анализ ценности клиентов по горизонтам 3, 6, 12 месяцев и за весь период
        </p>
        <div className="page-actions">
          <button 
            className="btn btn-primary"
            onClick={handleRecalculate}
            disabled={loading}
          >
            {loading ? 'Пересчет...' : '🔄 Пересчитать LTV'}
          </button>
        </div>
      </div>

      {error && (
        <div className="alert alert-error">
          <p>❌ {error}</p>
          <button className="btn btn-sm" onClick={() => setError(null)}>Закрыть</button>
        </div>
      )}

      {summaryData ? (
        <LTVSummary data={summaryData} />
      ) : (
        <div className="loading">Загрузка сводки LTV...</div>
      )}
    </div>
  );
};

export default LTVPage;
