import axios from 'axios'

const api = axios.create({
  baseURL: '',
})

export const getStatus        = ()              => api.get('/api/status')
export const getRecommendations = (limit = 20) => api.get('/api/recommendations', { params: { limit } })
export const getStockPrice    = (id, days = 60) => api.get(`/api/stocks/${id}/price`, { params: { days } })
export const getInstitutional = (id, days = 30) => api.get(`/api/stocks/${id}/institutional`, { params: { days } })
export const getInstitutionalTop = (limit = 20) => api.get('/api/institutional/top', { params: { limit } })
export const getConferences   = (limit = 30)   => api.get('/api/conferences', { params: { limit } })
export const getWatchlist     = ()              => api.get('/api/watchlist')
export const getEps           = (id)           => api.get(`/api/stocks/${id}/eps`)
export const getIndicators    = (id, days=120) => api.get(`/api/stocks/${id}/indicators`, { params: { days } })
export const getMarketIndex   = (days=60)      => api.get('/api/market/index', { params: { days } })
export const getAllIndices     = (days=60)      => api.get('/api/market/all-indices', { params: { days } })
export const getUsOverview    = ()             => api.get('/api/us/overview')
export const getAsiaOverview  = ()             => api.get('/api/asia/overview')
