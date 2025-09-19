import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import './index.css'
import App from './App.tsx'
import HomePage from './pages/HomePage.tsx'
import SegmentationPage from './pages/SegmentationPage.tsx'
import PurchasePredictionPage from './pages/PurchasePredictionPage.tsx'
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
