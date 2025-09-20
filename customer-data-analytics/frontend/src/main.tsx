import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import './index.css'
import App from './App.tsx'
import HomePage from './pages/HomePage.tsx'
import SegmentationPage from './pages/SegmentationPage.tsx'
import PurchasePredictionPage from './pages/PurchasePredictionPage.tsx'
import RecommendationsPage from './pages/RecommendationsPage.tsx'
import ChurnPredictionPage from './pages/ChurnPredictionPage.tsx'
import PriceElasticityPage from './pages/PriceElasticityPage.tsx'
import AnomaliesPage from './pages/AnomaliesPage.tsx'
import LTVPage from './pages/LTVPage.tsx'
import { FiltersProvider } from './store/filters.tsx'

const router = createBrowserRouter([
  {
    path: '/',
    element: <App />,
    children: [
      {
        index: true,
        element: <HomePage />,
      },
      {
        path: 'segmentation',
        element: <SegmentationPage />,
      },
      {
        path: 'purchase-prediction',
        element: <PurchasePredictionPage />,
      },
      {
        path: 'recommendations',
        element: <RecommendationsPage />,
      },
      {
        path: 'churn-prediction',
        element: <ChurnPredictionPage />,
      },
      {
        path: 'price-elasticity',
        element: <PriceElasticityPage />,
      },
      {
        path: 'anomalies',
        element: <AnomaliesPage />,
      },
      {
        path: 'ltv',
        element: <LTVPage />,
      },
    ],
  },
])

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <FiltersProvider>
      <RouterProvider router={router} />
    </FiltersProvider>
  </StrictMode>,
)
