const PALETTE = [
  '#4F46E5', '#10B981', '#F59E0B', '#EF4444', '#06B6D4',
  '#8B5CF6', '#22C55E', '#EAB308', '#F43F5E', '#14B8A6',
]

export function colorByClusterId(clusterId: number) {
  return PALETTE[clusterId % PALETTE.length]
}



