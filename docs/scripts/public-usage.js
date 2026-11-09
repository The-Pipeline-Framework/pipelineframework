import Ajv from 'ajv'
import addFormats from 'ajv-formats'
import { createHash } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const schema = JSON.parse(await readFile(new URL('../public/ecosystem/schema/public-usage-v1.json', import.meta.url), 'utf8'))
const ajv = new Ajv({ strict: true })
addFormats(ajv)
const validate = ajv.compile(schema)
const maxBytes = 128 * 1024
const input = 'https://raw.githubusercontent.com/The-Pipeline-Framework/pipelineframework-census/usage-public/usage.snapshot.json'
const prepared = new URL('../.vitepress/cache/usage.snapshot.json', import.meta.url)

export function latestClosedWeek(now) {
  const date = new Date(now)
  const weekday = (date.getUTCDay() + 6) % 7
  date.setUTCDate(date.getUTCDate() - weekday - (weekday === 0 ? 14 : 7))
  return date.toISOString().slice(0, 10)
}

export function parseSnapshot(raw, now = new Date()) {
  if (Buffer.byteLength(raw) > maxBytes) throw new Error('Public usage snapshot exceeds its size limit')
  const snapshot = JSON.parse(raw)
  if (!validate(snapshot)) throw new Error('Public usage snapshot does not match schema v1')
  const generated = new Date(snapshot.generated_at)
  if (generated.toISOString() !== snapshot.generated_at || generated > now ||
      snapshot.latest_complete_week !== latestClosedWeek(generated)) {
    throw new Error('Public usage snapshot has an invalid generation time or reporting period')
  }
  if (snapshot.weeks.at(-1).status === 'missing') {
    throw new Error('The latest complete week has not been materialised; keep the previous deployment')
  }
  for (const [index, week] of snapshot.weeks.entries()) {
    const expected = new Date(snapshot.latest_complete_week)
    expected.setUTCDate(expected.getUTCDate() - (11 - index) * 7)
    if (week.week_start !== expected.toISOString().slice(0, 10)) throw new Error('Public usage weeks are not consecutive')
    if (week.status !== 'published') continue
    if (week.reporting_installations > week.reporting_application_instances) throw new Error('Invalid reporting totals')
    const { sizes, feature_counts, ...dimensions } = week.distributions
    for (const dimension of [...Object.values(dimensions), ...Object.values(sizes), ...Object.values(feature_counts)]) {
      if (dimension.status !== 'published') continue
      const counts = Object.values(dimension.counts)
      const total = week.reporting_application_instances
      const sum = counts.reduce((a, b) => a + b, 0)
      if (!Number.isSafeInteger(sum) || counts.some(count => count > total) ||
          Math.abs(sum - total) > 5 * (counts.length + 1)) throw new Error('Invalid public distribution totals')
    }
  }
  return snapshot
}

/** Only release-docs builds with explicit operator configuration may contact the approved export branch. */
export async function approvedSnapshot(env, fetchSnapshot = fetch) {
  if (env.CF_PAGES_BRANCH !== 'release-docs' || env.TPF_USAGE_SNAPSHOT !== 'approved') return null
  const response = await fetchSnapshot(input, { redirect: 'error', signal: AbortSignal.timeout(8000) })
  if (!response.ok || !response.body) throw new Error('Approved usage snapshot is unavailable')
  const parts = []
  let size = 0
  for await (const part of response.body) {
    size += part.byteLength
    if (size > maxBytes) throw new Error('Approved usage snapshot exceeds its size limit')
    parts.push(part)
  }
  return parseSnapshot(Buffer.concat(parts).toString('utf8'))
}

export async function preparePublicUsage() {
  const snapshot = await approvedSnapshot(process.env)
  await mkdir(new URL('.', prepared), { recursive: true })
  await writeFile(prepared, JSON.stringify(snapshot))
}

export async function readPreparedUsage() {
  let raw
  try {
    raw = await readFile(prepared, 'utf8')
  } catch (error) {
    if (error.code !== 'ENOENT') throw error
    if (process.env.CF_PAGES_BRANCH === 'release-docs' && process.env.TPF_USAGE_SNAPSHOT === 'approved') {
      throw new Error('Run the site build command to prepare approved usage data')
    }
    return { snapshot: null, download: null }
  }
  // Never serve a cached release build's data from a developer or preview build.
  if (process.env.CF_PAGES_BRANCH !== 'release-docs' || process.env.TPF_USAGE_SNAPSHOT !== 'approved') {
    return { snapshot: null, download: null }
  }
  const snapshot = parseSnapshot(raw)
  const hash = createHash('sha256').update(raw).digest('hex')
  return { snapshot, download: `/ecosystem/snapshots/v1-${hash}.json` }
}

export async function emitPublicUsage(outDir) {
  const { snapshot, download } = await readPreparedUsage()
  if (!snapshot) return
  await mkdir(join(outDir, 'ecosystem/snapshots'), { recursive: true })
  await writeFile(join(outDir, download), JSON.stringify(snapshot))
}
