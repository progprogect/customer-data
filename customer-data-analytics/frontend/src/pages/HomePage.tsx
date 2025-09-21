import { Link } from 'react-router-dom'

function HomePage() {
  return (
    <div className="home-page">
      <div className="hero-section">
        <h1>Добро пожаловать в Customer Data Analytics</h1>
        <p className="hero-subtitle">
          Платформа для анализа поведения клиентов и сегментации аудитории
        </p>
      </div>

      <div className="analytics-modules">
        <h2>Модули аналитики</h2>
        <div className="modules-grid">
          <Link to="/segmentation" className="module-card">
            <div className="module-icon">📊</div>
            <h3>Сегментация клиентов</h3>
            <p>
              Анализ распределения пользователей по сегментам, динамика изменений 
              и матрица переходов между кластерами.
            </p>
            <div className="module-stats">
              <span className="stat">3 виджета</span>
              <span className="stat">Реальные данные</span>
              <span className="stat">Интерактивные фильтры</span>
            </div>
          </Link>

          <Link to="/recommendations" className="module-card">
            <div className="module-icon">🎯</div>
            <h3>Рекомендации</h3>
            <p>
              Примеры персональных рекомендаций для пользователей на основе 
              гибридной системы (CF + Content-based + Popularity).
            </p>
            <div className="module-stats">
              <span className="stat">Hybrid алгоритм</span>
              <span className="stat">Живые данные</span>
              <span className="stat">3 источника</span>
            </div>
          </Link>

          <Link to="/purchase-prediction" className="module-card">
            <div className="module-icon">📈</div>
            <h3>Прогноз покупок</h3>
            <p>
              Предсказание вероятности покупки в ближайшие 30 дней 
              на основе машинного обучения.
            </p>
            <div className="module-stats">
              <span className="stat">ML модель</span>
              <span className="stat">Интерпретируемость</span>
              <span className="stat">Real-time API</span>
            </div>
          </Link>

          <Link to="/churn-prediction" className="module-card">
            <div className="module-icon">💔</div>
            <h3>Отток клиентов</h3>
            <p>
              Предсказание риска оттока клиентов в следующие 60 дней 
              с анализом причин и рекомендациями.
            </p>
            <div className="module-stats">
              <span className="stat">XGBoost модель</span>
              <span className="stat">Анализ причин</span>
              <span className="stat">KPI дашборд</span>
            </div>
          </Link>

          <Link to="/price-elasticity" className="module-card">
            <div className="module-icon">💰</div>
            <h3>Ценовая эластичность</h3>
            <p>
              Анализ влияния изменения цен на объем продаж по категориям товаров 
              с коэффициентами эластичности и рекомендациями.
            </p>
            <div className="module-stats">
              <span className="stat">Регрессионный анализ</span>
              <span className="stat">5 категорий</span>
              <span className="stat">Визуализация</span>
            </div>
          </Link>

          <Link to="/anomalies" className="module-card">
            <div className="module-icon">🚨</div>
            <h3>Аномалии поведения</h3>
            <p>
              Детекция необычных паттернов в поведении пользователей 
              с помощью z-score и ratio анализа по неделям.
            </p>
            <div className="module-stats">
              <span className="stat">Z-score анализ</span>
              <span className="stat">Временные ряды</span>
              <span className="stat">Триггеры</span>
            </div>
          </Link>

          <Link to="/ltv" className="module-card">
            <div className="module-icon">💰</div>
            <h3>LTV Анализ</h3>
            <p>
              Анализ ценности клиентов (Lifetime Value) по горизонтам 
              3, 6, 12 месяцев и за весь период для расчета CAC.
            </p>
            <div className="module-stats">
              <span className="stat">Ретроспективный LTV</span>
              <span className="stat">4 горизонта</span>
              <span className="stat">CAC расчеты</span>
            </div>
          </Link>
        </div>
      </div>

      <div className="platform-features">
        <h2>Возможности платформы</h2>
        <div className="features-list">
          <div className="feature">
            <span className="feature-icon">🔄</span>
            <div>
              <h4>Обновление в реальном времени</h4>
              <p>Данные обновляются ежедневно из продуктивной базы</p>
            </div>
          </div>
          <div className="feature">
            <span className="feature-icon">📊</span>
            <div>
              <h4>Интерактивные дашборды</h4>
              <p>Современные графики с возможностью фильтрации и drill-down</p>
            </div>
          </div>
          <div className="feature">
            <span className="feature-icon">🤖</span>
            <div>
              <h4>Машинное обучение</h4>
              <p>Алгоритмы кластеризации, прогнозирования и рекомендаций</p>
            </div>
          </div>
          <div className="feature">
            <span className="feature-icon">🔒</span>
            <div>
              <h4>Безопасность данных</h4>
              <p>Защищенное соединение и контроль доступа к данным</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default HomePage
