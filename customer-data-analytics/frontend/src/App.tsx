import { Link, Outlet } from 'react-router-dom'
import './App.css'

function App() {
  return (
    <div className="app">
      <header className="header">
        <h1>Customer Data Analytics</h1>
        <nav className="nav">
          <Link to="/">Главная</Link>
          <Link to="/segmentation">Сегментация</Link>
          <Link to="/recommendations">Рекомендации</Link>
          <Link to="/purchase-prediction">Прогноз покупок</Link>
          <Link to="/churn-prediction">Отток клиентов</Link>
          <Link to="/price-elasticity">Ценовая эластичность</Link>
          <Link to="/anomalies">Аномалии</Link>
          <Link to="/ltv">LTV Анализ</Link>
        </nav>
      </header>
      <main className="main">
        <Outlet />
      </main>
    </div>
  )
}

export default App
