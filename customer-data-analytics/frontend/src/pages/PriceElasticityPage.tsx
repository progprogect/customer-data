import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './PriceElasticityPage.css';

interface TopCategory {
  category: string;
  elasticity_coefficient: number;
  r_squared: number;
  data_points: number;
  sensitivity_level: string;
}

interface PriceScenariosResponse {
  category: string;
  elasticity_coefficient: number;
  scenarios: {
    [key: string]: number;
  };
  interpretation: string;
  recommendation: string;
  sensitivity_level: string;
}

interface ElasticityDataPoint {
  week_start: string;
  avg_price: number;
  units_sold: number;
  revenue: number;
}

const PriceElasticityPage: React.FC = () => {
  const [categories, setCategories] = useState<string[]>([]);
  const [selectedCategory, setSelectedCategory] = useState<string>('');
  const [topCategories, setTopCategories] = useState<TopCategory[]>([]);
  const [priceScenarios, setPriceScenarios] = useState<PriceScenariosResponse | null>(null);
  const [elasticityData, setElasticityData] = useState<ElasticityDataPoint[]>([]);
  const [loading, setLoading] = useState(false);

  // Загружаем категории при монтировании
  useEffect(() => {
    const fetchCategories = async () => {
      try {
        const response = await axios.get('http://localhost:8000/api/v1/price-elasticity/categories');
        setCategories(response.data);
        if (response.data.length > 0) {
          setSelectedCategory(response.data[0]);
        }
      } catch (error) {
        console.error('Ошибка загрузки категорий:', error);
      }
    };

    fetchCategories();
  }, []);

  // Загружаем топ категории
  useEffect(() => {
    const fetchTopCategories = async () => {
      try {
        const response = await axios.get('http://localhost:8000/api/v1/price-elasticity/top-categories?limit=3');
        setTopCategories(response.data);
      } catch (error) {
        console.error('Ошибка загрузки топ категорий:', error);
      }
    };

    fetchTopCategories();
  }, []);

  // Загружаем данные при изменении категории
  useEffect(() => {
    if (selectedCategory) {
      const fetchData = async () => {
        setLoading(true);
        try {
          const [scenariosResponse, dataResponse] = await Promise.all([
            axios.get(`http://localhost:8000/api/v1/price-elasticity/scenarios/${selectedCategory}`),
            axios.get(`http://localhost:8000/api/v1/price-elasticity/data/${selectedCategory}?min_units_sold=10`)
          ]);
          
          setPriceScenarios(scenariosResponse.data);
          setElasticityData(dataResponse.data);
        } catch (error) {
          console.error('Ошибка загрузки данных:', error);
        } finally {
          setLoading(false);
        }
      };

      fetchData();
    }
  }, [selectedCategory]);

  const getSensitivityColor = (level: string) => {
    switch (level) {
      case 'Высокая': return '#ef4444';
      case 'Средняя': return '#f59e0b';
      case 'Низкая': return '#10b981';
      default: return '#6b7280';
    }
  };

  const getSensitivityLabel = (level: string) => {
    switch (level) {
      case 'Высокая': return 'Высокая';
      case 'Средняя': return 'Средняя';
      case 'Низкая': return 'Низкая';
      default: return 'Неизвестно';
    }
  };

  return (
    <div className="price-elasticity-page">
      <div className="page-header">
        <h1>💰 Ценовая эластичность</h1>
        <p>Анализ влияния изменения цен на объем продаж по категориям товаров</p>
      </div>

      {/* Селектор категории */}
      <div className="category-selector">
        <label htmlFor="category-select">Выберите категорию:</label>
        <select
          id="category-select"
          value={selectedCategory}
          onChange={(e) => setSelectedCategory(e.target.value)}
          className="category-select"
        >
          {categories.map((category) => (
            <option key={category} value={category}>
              {category}
            </option>
          ))}
        </select>
      </div>

      {/* Что важно знать */}
      <div className="top-categories-section">
        <h2>🎯 Что важно знать</h2>
        <div className="top-categories-grid">
          {topCategories.map((category, index) => (
            <div key={category.category} className="top-category-card">
              <div className="rank-badge">#{index + 1}</div>
              <h3>{category.category}</h3>
              <div className="elasticity-value">
                {Math.abs(category.elasticity_coefficient).toFixed(1)}
              </div>
              <div 
                className="sensitivity-badge"
                style={{ backgroundColor: getSensitivityColor(category.sensitivity_level) }}
              >
                {getSensitivityLabel(category.sensitivity_level)}
              </div>
              <p className="business-recommendation">
                {category.sensitivity_level === 'Высокая' 
                  ? 'Количество продаж сильно меняется в зависимости от цены'
                  : category.sensitivity_level === 'Средняя'
                  ? 'Спрос реагирует на изменения цены'
                  : 'Спрос мало зависит от цены'
                }
              </p>
            </div>
          ))}
        </div>
      </div>

      {/* Анализ выбранной категории */}
      {priceScenarios && (
        <div className="analysis-section">
          <h2>📊 Анализ категории: {selectedCategory}</h2>
          
          <div className="business-metrics">
            <div className="business-metric-card">
              <div className="metric-icon">📈</div>
              <h3>Чувствительность к цене</h3>
              <div className="sensitivity-display">
                <span 
                  className="sensitivity-badge"
                  style={{ 
                    backgroundColor: getSensitivityColor(priceScenarios.sensitivity_level),
                    color: 'white'
                  }}
                >
                  {priceScenarios.sensitivity_level}
                </span>
              </div>
              <p className="card-description">
                {priceScenarios.elasticity_coefficient > 0 
                  ? 'Повышение цены увеличивает продажи' 
                  : 'Снижение цены увеличивает продажи'
                }
              </p>
            </div>

            <div className="analysis-card">
              <div className="card-icon">💡</div>
              <h3>Рекомендация</h3>
              <p className="recommendation-text">{priceScenarios.recommendation}</p>
            </div>
          </div>

          {/* Сценарии изменения цен */}
          <div className="scenarios-section">
            <h3>💰 Сценарии изменения цен</h3>
            <p className="data-source-note">
              📊 Данные рассчитаны на основе реальных продаж за {priceScenarios ? 'последние недели' : 'период'}
            </p>
            <div className="scenarios-grid">
              <div className="scenario-card">
                <h4>📉 Снижение цен</h4>
                <div className="scenario-list">
                  <div className="scenario-item">
                    <span className="price-change">Цена -5%</span>
                    <span className="arrow">→</span>
                    <span className="demand-change">Продажи {priceScenarios.scenarios['price_-5%'] > 0 ? '+' : ''}{priceScenarios.scenarios['price_-5%'].toFixed(0)}%</span>
                  </div>
                  <div className="scenario-item">
                    <span className="price-change">Цена -10%</span>
                    <span className="arrow">→</span>
                    <span className="demand-change">Продажи {priceScenarios.scenarios['price_-10%'] > 0 ? '+' : ''}{priceScenarios.scenarios['price_-10%'].toFixed(0)}%</span>
                  </div>
                  <div className="scenario-item">
                    <span className="price-change">Цена -15%</span>
                    <span className="arrow">→</span>
                    <span className="demand-change">Продажи {priceScenarios.scenarios['price_-15%'] > 0 ? '+' : ''}{priceScenarios.scenarios['price_-15%'].toFixed(0)}%</span>
                  </div>
                </div>
              </div>
              <div className="scenario-card">
                <h4>📈 Повышение цен</h4>
                <div className="scenario-list">
                  <div className="scenario-item">
                    <span className="price-change">Цена +5%</span>
                    <span className="arrow">→</span>
                    <span className="demand-change">Продажи {priceScenarios.scenarios['price_+5%'] > 0 ? '+' : ''}{priceScenarios.scenarios['price_+5%'].toFixed(0)}%</span>
                  </div>
                  <div className="scenario-item">
                    <span className="price-change">Цена +10%</span>
                    <span className="arrow">→</span>
                    <span className="demand-change">Продажи {priceScenarios.scenarios['price_+10%'] > 0 ? '+' : ''}{priceScenarios.scenarios['price_+10%'].toFixed(0)}%</span>
                  </div>
                  <div className="scenario-item">
                    <span className="price-change">Цена +15%</span>
                    <span className="arrow">→</span>
                    <span className="demand-change">Продажи {priceScenarios.scenarios['price_+15%'] > 0 ? '+' : ''}{priceScenarios.scenarios['price_+15%'].toFixed(0)}%</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Простой график */}
      {elasticityData.length > 0 && (
        <div className="chart-section">
          <h2>📊 График: Цена vs Продажи</h2>
          <p className="chart-description">
            Каждая точка = неделя.
          </p>
          <div className="chart-legend">
            <div className="legend-item">
              <span className="legend-color min-color"></span>
              <span>Начальное значение</span>
            </div>
            <div className="legend-item">
              <span className="legend-color max-color"></span>
              <span>Конечное значение</span>
            </div>
          </div>
          <div className="chart-container">
            <div className="chart-scatter">
              {elasticityData.map((point, index) => (
                <div
                  key={index}
                  className="data-point"
                  style={{
                    left: `${((point.avg_price - Math.min(...elasticityData.map(p => p.avg_price))) / 
                      (Math.max(...elasticityData.map(p => p.avg_price)) - Math.min(...elasticityData.map(p => p.avg_price)))) * 100}%`,
                    bottom: `${((point.units_sold - Math.min(...elasticityData.map(p => p.units_sold))) / 
                      (Math.max(...elasticityData.map(p => p.units_sold)) - Math.min(...elasticityData.map(p => p.units_sold)))) * 100}%`
                  }}
                  title={`Цена: $${point.avg_price.toFixed(2)}, Продано: ${point.units_sold}`}
                ></div>
              ))}
            </div>
            <div className="chart-axes">
              <div className="x-axis">
                <span>Цена ($)</span>
                <div className="axis-labels">
                  <span className="axis-min">${Math.min(...elasticityData.map(p => p.avg_price)).toFixed(0)}</span>
                  <span className="axis-max">${Math.max(...elasticityData.map(p => p.avg_price)).toFixed(0)}</span>
                </div>
              </div>
              <div className="y-axis">
                <span>Продажи (ед.)</span>
                <div className="axis-labels">
                  <span className="axis-min">{Math.min(...elasticityData.map(p => p.units_sold))}</span>
                  <span className="axis-max">{Math.max(...elasticityData.map(p => p.units_sold))}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

export default PriceElasticityPage;
