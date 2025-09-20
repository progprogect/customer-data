/**
 * LTVFilters Component
 * Компонент для фильтрации LTV данных
 */

import React from 'react';

interface LTVFiltersProps {
  filters: {
    page: number;
    page_size: number;
    min_revenue_6m?: number;
    max_revenue_6m?: number;
    min_revenue_12m?: number;
    max_revenue_12m?: number;
    min_orders_lifetime?: number;
    max_orders_lifetime?: number;
    signup_date_from?: string;
    signup_date_to?: string;
    sort_by: string;
    sort_order: 'asc' | 'desc';
  };
  onFilterChange: (filters: Partial<LTVFiltersProps['filters']>) => void;
}

const LTVFilters: React.FC<LTVFiltersProps> = ({ filters, onFilterChange }) => {
  const handleInputChange = (field: string, value: string | number) => {
    const newValue = value === '' ? undefined : value;
    onFilterChange({ [field]: newValue });
  };

  const handleSortChange = (field: string, value: string) => {
    onFilterChange({ [field]: value });
  };

  const clearFilters = () => {
    onFilterChange({
      min_revenue_6m: undefined,
      max_revenue_6m: undefined,
      min_revenue_12m: undefined,
      max_revenue_12m: undefined,
      min_orders_lifetime: undefined,
      max_orders_lifetime: undefined,
      signup_date_from: undefined,
      signup_date_to: undefined,
      sort_by: 'lifetime_revenue',
      sort_order: 'desc'
    });
  };

  const hasActiveFilters = Object.values(filters).some(value => 
    value !== undefined && value !== '' && value !== 'lifetime_revenue' && value !== 'desc'
  );

  return (
    <div className="filters-section">
      <div className="section-header">
        <h4>🔍 Фильтры</h4>
        {hasActiveFilters && (
          <button 
            className="btn btn-sm"
            onClick={clearFilters}
          >
            Очистить фильтры
          </button>
        )}
      </div>

      <div className="filters-grid">
        <div className="filter-group">
          <label>💰 LTV за 6 месяцев</label>
          <div className="input-group">
            <input
              type="number"
              placeholder="От"
              value={filters.min_revenue_6m || ''}
              onChange={(e) => handleInputChange('min_revenue_6m', parseFloat(e.target.value) || '')}
            />
            <span>—</span>
            <input
              type="number"
              placeholder="До"
              value={filters.max_revenue_6m || ''}
              onChange={(e) => handleInputChange('max_revenue_6m', parseFloat(e.target.value) || '')}
            />
          </div>
        </div>

        <div className="filter-group">
          <label>💰 LTV за 12 месяцев</label>
          <div className="input-group">
            <input
              type="number"
              placeholder="От"
              value={filters.min_revenue_12m || ''}
              onChange={(e) => handleInputChange('min_revenue_12m', parseFloat(e.target.value) || '')}
            />
            <span>—</span>
            <input
              type="number"
              placeholder="До"
              value={filters.max_revenue_12m || ''}
              onChange={(e) => handleInputChange('max_revenue_12m', parseFloat(e.target.value) || '')}
            />
          </div>
        </div>

        <div className="filter-group">
          <label>🛒 Количество заказов</label>
          <div className="input-group">
            <input
              type="number"
              placeholder="От"
              value={filters.min_orders_lifetime || ''}
              onChange={(e) => handleInputChange('min_orders_lifetime', parseInt(e.target.value) || '')}
            />
            <span>—</span>
            <input
              type="number"
              placeholder="До"
              value={filters.max_orders_lifetime || ''}
              onChange={(e) => handleInputChange('max_orders_lifetime', parseInt(e.target.value) || '')}
            />
          </div>
        </div>

        <div className="filter-group">
          <label>🔄 Сортировка</label>
          <div className="input-group">
            <select
              value={filters.sort_by}
              onChange={(e) => handleSortChange('sort_by', e.target.value)}
            >
              <option value="lifetime_revenue">LTV за весь период</option>
              <option value="revenue_6m">LTV за 6 месяцев</option>
              <option value="revenue_12m">LTV за 12 месяцев</option>
              <option value="orders_lifetime">Количество заказов</option>
            </select>
            <select
              value={filters.sort_order}
              onChange={(e) => handleSortChange('sort_order', e.target.value as 'asc' | 'desc')}
            >
              <option value="desc">По убыванию</option>
              <option value="asc">По возрастанию</option>
            </select>
          </div>
        </div>
      </div>
    </div>
  );
};

export default LTVFilters;
