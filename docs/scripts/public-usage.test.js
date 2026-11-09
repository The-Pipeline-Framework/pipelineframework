import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { approvedSnapshot, latestClosedWeek, parseSnapshot } from './public-usage.js'
import { adoption, sortedBins, stale } from '../ecosystem/usage-display.js'
import { syntheticSnapshot } from './fixtures/synthetic-usage.js'

const synthetic = () => syntheticSnapshot()
const encode = snapshot => JSON.stringify(snapshot)
const release = { CF_PAGES_BRANCH: 'release-docs', TPF_USAGE_SNAPSHOT: 'approved' }

test('closed weeks use UTC Monday boundaries and the late-report window', () => {
  assert.equal(latestClosedWeek('2027-01-04T23:59:59Z'), '2026-12-21')
  assert.equal(latestClosedWeek('2027-01-05T00:00:00Z'), '2026-12-28')
})
test('strict schema rejects IDs, cross-filters, unexpected bins and unrounded values', () => {
  const mutations = [
    s => { s.weeks[11].installation_id = 'private' },
    s => { s.weeks[11].distributions.version_by_platform = {} },
    s => { s.weeks[11].distributions.transport.counts.private_provider = 20 },
    s => { s.weeks[11].reporting_application_instances = 61 },
    s => { s.weeks[11].distributions.feature_counts.await.counts = { '1': 20 } },
    s => { s.weeks[11].distributions.compiler_version.counts['private-app'] = 20 }
  ]
  for (const change of mutations) {
    const snapshot = synthetic()
    change(snapshot)
    assert.throws(() => parseSnapshot(encode(snapshot)), /schema/)
  }
})
test('invalid or empty publication cannot replace the last good site build', async () => {
  for (const mutate of [
    s => { s.weeks[11] = { week_start: s.latest_complete_week, status: 'missing' } },
    s => { s.weeks[11].week_start = s.weeks[10].week_start },
    s => { s.latest_complete_week = '2026-10-26' },
    s => { s.weeks = s.weeks.map(({ week_start }) => ({ week_start, status: 'missing' })) },
    s => { s.weeks[11].distributions.transport.counts.GRPC = 100 }
  ]) {
    const snapshot = synthetic()
    mutate(snapshot)
    await assert.rejects(approvedSnapshot(release, async () => new Response(encode(snapshot))))
  }
  await assert.rejects(approvedSnapshot(release, async () => new Response('offline', { status: 503 })), /unavailable/)
  await assert.rejects(approvedSnapshot(release, async () => new Response('x'.repeat(129 * 1024))), /size limit/)
})
test('only explicitly configured release builds read an approved export, once', async () => {
  let calls = 0
  const fetch = async (url, options) => {
    calls++
    assert.match(url, /pipelineframework-census\/usage-public\/usage\.snapshot\.json$/)
    assert.equal(options.redirect, 'error')
    return new Response(encode(synthetic()))
  }
  assert.equal(await approvedSnapshot({}, fetch), null)
  assert.equal(await approvedSnapshot({ ...release, CF_PAGES_BRANCH: 'main' }, fetch), null)
  assert.deepEqual(await approvedSnapshot(release, fetch), synthetic())
  assert.equal(calls, 1)
})
test('sparse, unknown, missing and stale data keep their distinct semantics', () => {
  const snapshot = parseSnapshot(encode(synthetic()))
  const latest = snapshot.weeks[11]
  const features = latest.distributions.feature_counts
  assert.equal(adoption(features.query), 50) // 20 nonzero / (20 zero + 20 nonzero), not / 60
  assert.equal(adoption(features.many_to_one), 0)
  assert.equal(adoption(features.command), null)
  assert.deepEqual(sortedBins(features.await), [])
  assert.equal(snapshot.weeks[7].status, 'insufficient_contributors')
  assert.equal(snapshot.weeks[10].status, 'missing')
  assert.equal(stale(snapshot, new Date('2026-11-02T23:59:59Z')), false)
  assert.equal(stale(snapshot, new Date('2026-11-03T00:00:00Z')), true)
})
test('Pages Functions routing retains the API prefix and excludes every dashboard asset', async () => {
  const routes = JSON.parse(await readFile(new URL('../public/_routes.json', import.meta.url)))
  assert.deepEqual(routes, { version: 1, include: ['/api/*'], exclude: [] })
  const headers = await readFile(new URL('../public/_headers', import.meta.url), 'utf8')
  assert.match(headers, /\/ecosystem\/usage\n  Cache-Control: public, max-age=0, must-revalidate/)
  assert.match(headers, /\/ecosystem\/snapshots\/\*\n  Cache-Control: public, max-age=31536000, immutable/)
})
