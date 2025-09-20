/**
 * AnomaliesPage Component
 * Страница с аномалиями поведения пользователей
 */

import React from 'react';
import AnomaliesTable from '../components/AnomaliesTable';
import AlgorithmInfo from '../components/AlgorithmInfo';

const AnomaliesPage: React.FC = () => {

  return (
    <div className="anomalies-page">
      <div className="page-header">
        <h1>🚨 Аномалии поведения пользователей</h1>
        <p className="page-description">
          Детекция и анализ необычных паттернов в поведении клиентов
        </p>
      </div>

      <div className="page-content">
        {/* Информация об алгоритме */}
        <div className="algorithm-section">
          <AlgorithmInfo />
        </div>

        {/* Таблица аномалий */}
        <div className="anomalies-section">
          <AnomaliesTable />
        </div>
      </div>
    </div>
  );
};

export default AnomaliesPage;
