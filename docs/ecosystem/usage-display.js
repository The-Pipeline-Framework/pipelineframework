export const bands = ['0', '1', '2–5', '6–10', '11–25', '26–50', '51–100', '101+', 'unknown']

const number = new Intl.NumberFormat('en-GB')
export const displayCount = value => value == null ? '—' : number.format(value)
export const hasCount = week => week.status === 'published'
export function describeStatus(week) {
  if (week?.status === 'insufficient_contributors') return 'Too few reporting installations'
  if (week?.status === 'missing') return 'No published result'
  return 'Published'
}
export function sortedBins(distribution, order) {
  if (distribution?.status !== 'published') return []
  const counts = distribution.counts
  return (order ?? Object.keys(counts).sort((a, b) => {
    if (a === 'unknown') return 1
    if (b === 'unknown') return -1
    return b.localeCompare(a, 'en-GB', { numeric: true })
  })).filter(key => Object.hasOwn(counts, key)).map(key => [key, counts[key]])
}
export function adoption(distribution) {
  const bins = sortedBins(distribution, bands)
  const known = bins.filter(([band]) => band !== 'unknown')
  const denominator = known.reduce((sum, [, count]) => sum + count, 0)
  if (!denominator) return null
  return Math.round(100 * (denominator - (distribution.counts['0'] ?? 0)) / denominator)
}
export function weekEnd(start) {
  const date = new Date(start)
  date.setUTCDate(date.getUTCDate() + 6)
  return date.toISOString().slice(0, 10)
}
export function stale(snapshot, now) {
  const nextClosure = new Date(snapshot.latest_complete_week)
  nextClosure.setUTCDate(nextClosure.getUTCDate() + 15)
  return now >= nextClosure
}
