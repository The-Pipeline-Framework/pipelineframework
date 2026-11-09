// For isolated local tests only. Never imported by the production data loader.
export function syntheticSnapshot() {
  const last = '2026-10-19'
  const date = index => new Date(Date.parse(last) - (11 - index) * 7 * 86400000).toISOString().slice(0, 10)
  const published = counts => ({ status: 'published', counts })
  const suppressed = { status: 'suppressed' }
  const sample = {
    status: 'published', reporting_installations: 40, reporting_application_instances: 60,
    distributions: {
      compiler_version: published({ '26.11.2-dev': 40, unknown: 20 }),
      contracts_version: published({ '26.11.1': 60 }),
      java_major: published({ 21: 40, 25: 20 }),
      sizes: { pipelines: published({ 1: 60 }), steps: published({ '6–10': 40, '11–25': 20 }), types: published({ '2–5': 40, unknown: 20 }), blocks: published({ 0: 60 }) },
      feature_counts: {
        nested_invocation: published({ 0: 40, '2–5': 20 }),
        one_to_many: published({ 0: 20, 1: 40 }),
        many_to_one: published({ 0: 60 }),
        query: published({ 0: 20, 1: 20, unknown: 20 }),
        command: published({ unknown: 60 }),
        await: suppressed
      },
      transport: published({ LOCAL: 40, GRPC: 20 }),
      platform: published({ COMPUTE: 60 }),
      runtime_layout: published({ MONOLITH: 40, UNSPECIFIED: 20 })
    }
  }
  return {
    schema_version: 1,
    observation_schema_version: 1,
    generated_at: '2026-10-27T04:00:00.000Z',
    latest_complete_week: last,
    method: 'latest-per-instance-utc-iso-week-rounded-10-min-20-installations',
    weeks: Array.from({ length: 12 }, (_, index) => ({
      week_start: date(index),
      ...(index === 7 ? { status: 'insufficient_contributors' } : [5, 9, 11].includes(index) ? sample : { status: 'missing' })
    }))
  }
}
