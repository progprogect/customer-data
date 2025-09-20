/**
 * LTVTable Component
 * Компонент для отображения таблицы LTV данных пользователей
 */

import React from 'react';

interface LTVUser {
  user_id: number;
  signup_date: string;
  revenue_3m: number;
  revenue_6m: number;
  revenue_12m: number;
  lifetime_revenue: number;
  orders_3m: number;
  orders_6m: number;
  orders_12m: number;
  orders_lifetime: number;
  avg_order_value_3m?: number;
  avg_order_value_6m?: number;
  avg_order_value_12m?: number;
  avg_order_value_lifetime?: number;
  last_order_date?: string;
  first_order_date?: string;
  days_since_last_order?: number;
  created_at: string;
  updated_at: string;
}

interface LTVUsersResponse {
  users: LTVUser[];
  total_count: number;
  page: number;
  page_size: number;
}

interface LTVTableProps {
  data: LTVUsersResponse;
  onPageChange: (page: number) => void;
  loading: boolean;
}

const LTVTable: React.FC<LTVTableProps> = ({ data, onPageChange, loading }) => {
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('ru-RU', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2
    }).format(value);
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('ru-RU');
  };

  const getLTVSegment = (ltv: number) => {
    if (ltv >= 1000) return { label: 'VIP', color: '#e74c3c' };
    if (ltv >= 500) return { label: 'High', color: '#f39c12' };
    if (ltv >= 100) return { label: 'Medium', color: '#3498db' };
    if (ltv >= 50) return { label: 'Low', color: '#95a5a6' };
    return { label: 'New', color: '#bdc3c7' };
  };

  const totalPages = Math.ceil(data.total_count / data.page_size);

  return (
    <div>
      <div className="section-header">
        <h3>👥 LTV по пользователям</h3>
        <div className="section-meta">
          <span>Всего: {data.total_count.toLocaleString('ru-RU')}</span>
          <span>Страница: {data.page} из {totalPages}</span>
        </div>
      </div>

      {loading ? (
        <div className="loading">Загрузка данных...</div>
      ) : (
        <>
          <div className="table-container">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Регистрация</th>
                  <th>LTV 3м</th>
                  <th>LTV 6м</th>
                  <th>LTV 12м</th>
                  <th>LTV Lifetime</th>
                  <th>Заказы</th>
                  <th>AOV</th>
                  <th>Последний заказ</th>
                  <th>Сегмент</th>
                </tr>
              </thead>
              <tbody>
                {data.users.map((user) => {
                  const segment = getLTVSegment(user.lifetime_revenue);
                  return (
                    <tr key={user.user_id}>
                      <td>#{user.user_id}</td>
                      <td>{formatDate(user.signup_date)}</td>
                      <td>{formatCurrency(user.revenue_3m)}</td>
                      <td>{formatCurrency(user.revenue_6m)}</td>
                      <td>{formatCurrency(user.revenue_12m)}</td>
                      <td>
                        <strong>{formatCurrency(user.lifetime_revenue)}</strong>
                      </td>
                      <td>{user.orders_lifetime}</td>
                      <td>
                        {user.avg_order_value_lifetime 
                          ? formatCurrency(user.avg_order_value_lifetime)
                          : '-'
                        }
                      </td>
                      <td>
                        {user.last_order_date 
                          ? formatDate(user.last_order_date)
                          : '-'
                        }
                      </td>
                      <td>
                        <span 
                          className="badge"
                          style={{ backgroundColor: segment.color }}
                        >
                          {segment.label}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {totalPages > 1 && (
            <div className="pagination">
              <button
                className="btn btn-sm"
                onClick={() => onPageChange(data.page - 1)}
                disabled={data.page <= 1}
              >
                ← Предыдущая
              </button>
              
              <span className="pagination-info">
                Страница {data.page} из {totalPages}
              </span>
              
              <button
                className="btn btn-sm"
                onClick={() => onPageChange(data.page + 1)}
                disabled={data.page >= totalPages}
              >
                Следующая →
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default LTVTable;
