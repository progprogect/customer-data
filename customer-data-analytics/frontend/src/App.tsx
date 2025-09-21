import { Link, Outlet, useLocation } from 'react-router-dom'
import MobileNavigation from './components/MobileNavigation'
import './App.css'

function App() {
  const location = useLocation()

  return (
    <div className="app">
      {/* Mobile Navigation */}
      <MobileNavigation />
      
      {/* Desktop Header */}
      <header className="header">
        <h1>Customer Data Analytics</h1>
        <nav className="nav">
          <Link 
            to="/" 
            className={location.pathname === '/' ? 'active' : ''}
          >
            Главная
          </Link>
          <Link 
            to="/segmentation"
            className={location.pathname === '/segmentation' ? 'active' : ''}
          >
            Сегментация
          </Link>
          <Link 
            to="/recommendations"
            className={location.pathname === '/recommendations' ? 'active' : ''}
          >
            Рекомендации
          </Link>
          <Link 
            to="/purchase-prediction"
            className={location.pathname === '/purchase-prediction' ? 'active' : ''}
          >
            Прогноз покупок
          </Link>
          <Link 
            to="/churn-prediction"
            className={location.pathname === '/churn-prediction' ? 'active' : ''}
          >
            Отток клиентов
          </Link>
          <Link 
            to="/price-elasticity"
            className={location.pathname === '/price-elasticity' ? 'active' : ''}
          >
            Ценовая эластичность
          </Link>
          <Link 
            to="/anomalies"
            className={location.pathname === '/anomalies' ? 'active' : ''}
          >
            Аномалии
          </Link>
          <Link 
            to="/ltv"
            className={location.pathname === '/ltv' ? 'active' : ''}
          >
            LTV Анализ
          </Link>
        </nav>
      </header>
      
      <main className="main">
        <Outlet />
      </main>
    </div>
  )
}

export default App
