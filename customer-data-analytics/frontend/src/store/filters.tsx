import { createContext, useContext, useMemo, useReducer } from 'react'

type Period = 7 | 30 | 90
type Granularity = 'day' | 'week' | 'month'

export type FiltersState = {
  date: string | null
  period: Period
  granularity: Granularity
  hiddenClusters: number[]
}

type Action =
  | { type: 'setDate'; date: string | null }
  | { type: 'setPeriod'; period: Period }
  | { type: 'setGranularity'; granularity: Granularity }
  | { type: 'toggleCluster'; clusterId: number }
  | { type: 'setHiddenClusters'; clusterIds: number[] }

function reducer(state: FiltersState, action: Action): FiltersState {
  switch (action.type) {
    case 'setDate':
      return { ...state, date: action.date }
    case 'setPeriod':
      return { ...state, period: action.period }
    case 'setGranularity':
      return { ...state, granularity: action.granularity }
    case 'toggleCluster':
      return state.hiddenClusters.includes(action.clusterId)
        ? { ...state, hiddenClusters: state.hiddenClusters.filter((id) => id !== action.clusterId) }
        : { ...state, hiddenClusters: [...state.hiddenClusters, action.clusterId] }
    case 'setHiddenClusters':
      return { ...state, hiddenClusters: action.clusterIds }
    default:
      return state
  }
}

const FiltersContext = createContext<{
  state: FiltersState
  dispatch: React.Dispatch<Action>
} | null>(null)

export function FiltersProvider({ children }: { children: React.ReactNode }) {
  const [state, dispatch] = useReducer(reducer, {
    date: null,
    period: 30,
    granularity: 'day',
    hiddenClusters: [],
  })
  const value = useMemo(() => ({ state, dispatch }), [state])
  return <FiltersContext.Provider value={value}>{children}</FiltersContext.Provider>
}

export function useFilters() {
  const ctx = useContext(FiltersContext)
  if (!ctx) throw new Error('useFilters must be used within FiltersProvider')
  return ctx
}

export function computeFromTo(date: string, period: Period) {
  const to = new Date(date)
  const from = new Date(to)
  from.setDate(from.getDate() - (period - 1))
  const fmt = (d: Date) => d.toISOString().slice(0, 10)
  return { from: fmt(from), to: fmt(to) }
}



