import React, { useState, useEffect } from 'react'
import { Link, useLocation } from 'react-router-dom'
import './MobileNavigation.css'

interface NavigationItem {
  to: string
  label: string
  icon: string
  description: string
}

const navigationItems: NavigationItem[] = [
  {
    to: '/',
    label: 'Главная',
    icon: '🏠',
    description: 'Обзор системы'
  },
  {
    to: '/segmentation',
    label: 'Сегментация',
    icon: '📊',
    description: 'Анализ клиентов'
  },
  {
    to: '/recommendations',
    label: 'Рекомендации',
    icon: '🎯',
    description: 'Персональные советы'
  },
  {
    to: '/purchase-prediction',
    label: 'Прогноз покупок',
    icon: '📈',
    description: 'ML предсказания'
  },
  {
    to: '/churn-prediction',
    label: 'Отток клиентов',
    icon: '💔',
    description: 'Анализ рисков'
  },
  {
    to: '/price-elasticity',
    label: 'Ценовая эластичность',
    icon: '💰',
    description: 'Анализ цен'
  },
  {
    to: '/anomalies',
    label: 'Аномалии',
    icon: '🚨',
    description: 'Детекция отклонений'
  },
  {
    to: '/ltv',
    label: 'LTV Анализ',
    icon: '💎',
    description: 'Ценность клиентов'
  }
]

const MobileNavigation: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false)
  const [isScrolled, setIsScrolled] = useState(false)
  const location = useLocation()

  // Handle scroll effect
  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 10)
    }

    window.addEventListener('scroll', handleScroll)
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  // Close menu when route changes
  useEffect(() => {
    setIsOpen(false)
  }, [location])

  // Prevent body scroll when menu is open
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden'
    } else {
      document.body.style.overflow = 'unset'
    }

    return () => {
      document.body.style.overflow = 'unset'
    }
  }, [isOpen])

  const toggleMenu = () => {
    setIsOpen(!isOpen)
  }

  const closeMenu = () => {
    setIsOpen(false)
  }

  return (
    <>
      {/* Mobile Header */}
      <header className={`mobile-header ${isScrolled ? 'scrolled' : ''}`}>
        <div className="mobile-header-content">
          <Link to="/" className="mobile-logo" onClick={closeMenu}>
            <span className="logo-icon">📊</span>
            <span className="logo-text">Customer Analytics</span>
          </Link>
          
          <button
            className={`mobile-menu-button ${isOpen ? 'open' : ''}`}
            onClick={toggleMenu}
            aria-label="Открыть меню"
            aria-expanded={isOpen}
          >
            <span className="hamburger-line"></span>
            <span className="hamburger-line"></span>
            <span className="hamburger-line"></span>
          </button>
        </div>
      </header>

      {/* Mobile Menu Overlay */}
      {isOpen && (
        <div className="mobile-menu-overlay" onClick={closeMenu}>
          <nav className="mobile-menu" onClick={(e) => e.stopPropagation()}>
            <div className="mobile-menu-header">
              <h2 className="mobile-menu-title">Навигация</h2>
              <button
                className="mobile-menu-close"
                onClick={closeMenu}
                aria-label="Закрыть меню"
              >
                ✕
              </button>
            </div>

            <div className="mobile-menu-content">
              <ul className="mobile-menu-list">
                {navigationItems.map((item) => (
                  <li key={item.to} className="mobile-menu-item">
                    <Link
                      to={item.to}
                      className={`mobile-menu-link ${
                        location.pathname === item.to ? 'active' : ''
                      }`}
                      onClick={closeMenu}
                    >
                      <div className="menu-item-content">
                        <div className="menu-item-icon">{item.icon}</div>
                        <div className="menu-item-text">
                          <div className="menu-item-label">{item.label}</div>
                          <div className="menu-item-description">
                            {item.description}
                          </div>
                        </div>
                      </div>
                    </Link>
                  </li>
                ))}
              </ul>

              <div className="mobile-menu-footer">
                <div className="menu-footer-info">
                  <div className="footer-icon">🚀</div>
                  <div className="footer-text">
                    <div className="footer-title">Customer Data Analytics</div>
                    <div className="footer-subtitle">v2.0.0</div>
                  </div>
                </div>
              </div>
            </div>
          </nav>
        </div>
      )}
    </>
  )
}

export default MobileNavigation
