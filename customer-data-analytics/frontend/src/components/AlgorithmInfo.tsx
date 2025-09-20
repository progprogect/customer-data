/**
 * AlgorithmInfo Component
 * Компонент с описанием алгоритма детекции аномалий
 */

import React from 'react';

const AlgorithmInfo: React.FC = () => {
  return (
    <div className="algorithm-info">
      <h3>🔍 Алгоритм детекции аномалий</h3>
      
      <div className="algorithm-details">
        <div className="algorithm-section">
          <h4>📊 Анализируемые признаки:</h4>
          <ul>
            <li><strong>Orders Count</strong> - количество заказов за неделю</li>
            <li><strong>Monetary Sum</strong> - сумма покупок за неделю</li>
            <li><strong>Categories Count</strong> - разнообразие покупок (уникальные категории)</li>
          </ul>
        </div>

        <div className="algorithm-section">
          <h4>🧮 Методы детекции:</h4>
          <ul>
            <li><strong>Z-Score</strong> - статистическое отклонение от нормы пользователя</li>
            <li><strong>Week-to-Week Ratio</strong> - отношение к предыдущей неделе</li>
            <li><strong>Rolling Average</strong> - скользящее среднее за 4 недели</li>
          </ul>
        </div>

        <div className="algorithm-section">
          <h4>⚡ Правила аномалий:</h4>
          <ul>
            <li><strong>Всплеск заказов:</strong> z-score ≥ 3 или ratio ≥ 3.0</li>
            <li><strong>Провал заказов:</strong> z-score ≤ -3 или падение до 0</li>
            <li><strong>Всплеск трат:</strong> z-score ≥ 3 или ratio ≥ 3.0</li>
            <li><strong>Провал трат:</strong> z-score ≤ -3</li>
          </ul>
        </div>

        <div className="algorithm-section">
          <h4>📈 Требования к данным:</h4>
          <ul>
            <li>Минимум 4 недели истории для расчета нормы</li>
            <li>Только завершенные заказы (status = 'completed')</li>
            <li>Период анализа: последние 6 месяцев</li>
          </ul>
        </div>
      </div>

      <div className="algorithm-note">
        <p>
          <strong>💡 Примечание:</strong> Аномалии выявляются на основе индивидуальных 
          паттернов поведения каждого пользователя, что позволяет обнаружить 
          необычные изменения в их покупательской активности.
        </p>
      </div>
    </div>
  );
};

export default AlgorithmInfo;
