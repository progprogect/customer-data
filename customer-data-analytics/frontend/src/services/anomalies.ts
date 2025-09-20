/**
 * Anomalies API Service
 * Сервис для работы с API аномалий
 */

import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000';

// Типы данных
export interface UserAnomalyWeekly {
  user_id: number;
  week_start: string;
  anomaly_score: number;
  is_anomaly: boolean;
  triggers: string[];
  insufficient_history: boolean;
}

export interface AnomaliesWeeklyResponse {
  anomalies: UserAnomalyWeekly[];
  total_count: number;
  week_date: string;
  summary_stats: {
    total_anomalies: number;
    avg_score: number;
    max_score: number;
    unique_users: number;
  };
}

export interface UserBehaviorData {
  week_start: string;
  orders_count: number;
  monetary_sum: number;
  categories_count: number;
  aov_weekly: number | null;
}

export interface UserAnomaliesResponse {
  user_id: number;
  anomalies: UserAnomalyWeekly[];
  behavior_data: UserBehaviorData[];
  total_anomalies: number;
  anomaly_rate: number;
  top_triggers: Array<{
    trigger: string;
    count: number;
  }>;
}

export interface AnomalyStatsResponse {
  total_weeks: number;
  total_anomalies: number;
  anomaly_rate: number;
  top_users: Array<{
    user_id: number;
    anomaly_count: number;
    avg_score: number;
    max_score: number;
  }>;
  top_triggers: Array<{
    trigger: string;
    count: number;
  }>;
  weekly_distribution: Array<{
    week_start: string;
    anomaly_count: number;
  }>;
}

// API функции
export const anomaliesApi = {
  // Получение аномалий за неделю
  async getWeeklyAnomalies(
    date?: string,
    minScore: number = 3.0,
    limit: number = 100
  ): Promise<AnomaliesWeeklyResponse> {
    const params = new URLSearchParams();
    if (date) params.append('date', date);
    params.append('min_score', minScore.toString());
    params.append('limit', limit.toString());

    const response = await axios.get(
      `${API_BASE_URL}/api/v1/anomalies/weekly?${params.toString()}`
    );
    return response.data;
  },

  // Получение истории аномалий пользователя
  async getUserAnomalies(
    userId: number,
    weeks: number = 12,
    minScore: number = 0.0
  ): Promise<UserAnomaliesResponse> {
    const params = new URLSearchParams();
    params.append('weeks', weeks.toString());
    params.append('min_score', minScore.toString());

    const response = await axios.get(
      `${API_BASE_URL}/api/v1/anomalies/user/${userId}?${params.toString()}`
    );
    return response.data;
  },

  // Получение статистики аномалий
  async getAnomalyStats(): Promise<AnomalyStatsResponse> {
    const response = await axios.get(`${API_BASE_URL}/api/v1/anomalies/stats`);
    return response.data;
  },

  // Запуск детекции аномалий
  async detectAnomalies(): Promise<{
    success: boolean;
    message: string;
    stats: {
      total_records: number;
      anomaly_count: number;
      insufficient_history_count: number;
      anomaly_rate: number;
    };
  }> {
    const response = await axios.post(`${API_BASE_URL}/api/v1/anomalies/detect`);
    return response.data;
  },

  // Обновление аномалий
  async updateAnomalies(): Promise<{
    success: boolean;
    message: string;
  }> {
    const response = await axios.post(`${API_BASE_URL}/api/v1/anomalies/update`);
    return response.data;
  }
};

export default anomaliesApi;
