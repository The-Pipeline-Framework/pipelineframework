/** Local SSR and browser proof. Synthetic fixtures only exist inside a temporary site. */
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { cp, mkdir, mkdtemp, readFile, rm, symlink, writeFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { chromium } from 'playwright'
import { parseSnapshot } from './public-usage.js'
import { syntheticSnapshot } from './fixtures/synthetic-usage.js'

const docs = fileURLToPath(new URL('../', import.meta.url))
const tmp = await mkdtemp(join(tmpdir(), 'tpf-usage-synthetic-'))
const execute = promisify(execFile)
let browser
let server
try {
  await symlink(join(docs, 'node_modules'), join(tmp, 'node_modules'), 'dir')
  await mkdir(join(tmp, '.vitepress'))
  await writeFile(join(tmp, '.vitepress/config.js'), "export default { cleanUrls: true, title: 'The Pipeline Framework' }\n")
  await mkdir(join(tmp, 'ecosystem'))
  for (const name of ['UsageDashboard.vue', 'UsageDistribution.vue', 'usage-display.js']) {
    await cp(join(docs, 'ecosystem', name), join(tmp, 'ecosystem', name))
  }
  const snapshot = parseSnapshot(JSON.stringify(syntheticSnapshot()))
  const cases = [
    ['usage', snapshot],
    ['sparse', { ...snapshot, weeks: [...snapshot.weeks.slice(0, -1), { week_start: snapshot.latest_complete_week, status: 'insufficient_contributors' }] }],
    ['missing', { ...snapshot, weeks: [...snapshot.weeks.slice(0, -1), { week_start: snapshot.latest_complete_week, status: 'missing' }] }]
  ]
  for (const [name, fixture] of cases) {
    await writeFile(join(tmp, 'ecosystem', name + '.json'), JSON.stringify(fixture))
    await writeFile(join(tmp, 'ecosystem', name + '.md'), `---
sidebar: false
aside: false
head:
  - [meta, { name: robots, content: noindex }]
---
<script setup>
import Dashboard from './UsageDashboard.vue'
import snapshot from './${name}.json'
</script>

# TPF Ecosystem Usage

<Dashboard :snapshot="snapshot" synthetic />
`)
  }
  await execute(process.execPath, [join(docs, 'node_modules/vitepress/bin/vitepress.js'), 'build', tmp], { timeout: 90000 })
  const syntheticDist = join(tmp, '.vitepress/dist')
  const publicDist = join(docs, '.vitepress/dist')
  const publicHtml = await readFile(join(publicDist, 'ecosystem/usage.html'), 'utf8')
  assert.match(publicHtml, /No approved snapshot has been published/)
  assert.doesNotMatch(publicHtml, /Synthetic preview|26\.11\.2-dev/)
  assert.match(await readFile(join(syntheticDist, 'ecosystem/usage.html'), 'utf8'), /≈50%/)
  let attemptedBackend = 0
  server = createServer(async (request, response) => {
    const url = new URL(request.url, 'http://localhost')
    if (url.pathname.startsWith('/api/')) {
      attemptedBackend++
      response.writeHead(503).end()
      return
    }
    const base = url.pathname.startsWith('/public/') ? publicDist : syntheticDist
    const pathname = url.pathname.replace(/^\/public(?=\/)/, '')
    const file = resolve(base, '.' + pathname + (pathname.includes('.') ? '' : '.html'))
    if (!file.startsWith(base + '/')) return response.writeHead(404).end()
    const types = { html: 'text/html', js: 'application/javascript', css: 'text/css', json: 'application/json' }
    try { response.setHeader('Content-Type', types[file.split('.').at(-1)] ?? 'application/octet-stream'); response.end(await readFile(file)) }
    catch { response.writeHead(404).end() }
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const base = 'http://127.0.0.1:' + server.address().port
  const responses = await Promise.all(Array.from({ length: 100 }, (_, i) =>
    fetch(base + '/public/ecosystem/usage?crawler=' + i, { headers: { 'user-agent': 'UsageTestBot', cookie: 'test=' + i } })))
  assert.ok(responses.every(r => r.ok))
  assert.ok((await Promise.all(responses.map(r => r.text()))).every(text => text === publicHtml))
  browser = await chromium.launch()
  const navigationPage = await browser.newPage()
  const publishingHtml = await readFile(join(publicDist, 'evolve/publishing.html'), 'utf8')
  const navigation = await navigationPage.evaluate(html => {
    const document = new DOMParser().parseFromString(html, 'text/html')
    const heading = document.querySelector('#VPSidebarNav .level-1 > .item > a[href="/evolve/publishing"]')
    const group = heading?.closest('.VPSidebarItem')
    return {
      releaseLinks: Array.from(group?.querySelectorAll('a[href]') ?? [], link => link.getAttribute('href')),
      topLevelUsage: Boolean(document.querySelector('.VPNavBarMenu > a[href="/ecosystem/usage"]'))
    }
  }, publishingHtml)
  assert.ok(navigation.releaseLinks.includes('/ecosystem/usage'))
  assert.equal(navigation.topLevelUsage, false)
  for (const href of new Set(navigation.releaseLinks)) {
    await readFile(join(publicDist, href + '.html'))
  }
  await navigationPage.close()
  for (const width of [1280, 360]) {
    const page = await browser.newPage({ viewport: { width, height: 840 }, javaScriptEnabled: false })
    await page.goto(base + '/ecosystem/usage')
    assert.equal(await page.getByText('Synthetic preview', { exact: false }).count(), 1)
    assert.equal(await page.getByText('≈50%').count(), 1)
    assert.equal(await page.getByText('Withheld to protect small cohorts.').count(), 1)
    if (process.env.TPF_USAGE_SCREENSHOTS) {
      await page.setViewportSize({ width, height: width === 360 ? 1160 : 960 })
      await page.screenshot({ path: join(process.env.TPF_USAGE_SCREENSHOTS, `tpf-usage-${width}.png`) })
    }
    const summary = page.getByText('Weekly reporting totals (table)')
    await summary.focus()
    await page.keyboard.press('Enter')
    assert.equal(await page.getByRole('row', { name: /Too few reporting installations/ }).count(), 1)
    assert.equal(await page.getByRole('row', { name: /No published result/ }).count(), 8)
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
    await page.close()
  }
  const page = await browser.newPage()
  const outside = []
  await page.route('**/*', route => {
    if (route.request().url().startsWith(base)) return route.continue()
    outside.push(route.request().url())
    return route.abort()
  })
  await page.goto(base + '/ecosystem/usage')
  await page.getByText('This snapshot is out of date.', { exact: false }).waitFor()
  await page.goto(base + '/ecosystem/sparse')
  assert.equal(await page.getByText('Too few reporting installations for the latest complete week.').count(), 1)
  await page.goto(base + '/ecosystem/missing')
  assert.equal(await page.getByText('No published result for the latest complete week.').count(), 1)
  assert.deepEqual(outside, [])
  assert.equal(attemptedBackend, 0)
  console.log('Usage SSR, release navigation, stale and unavailable states, mobile/desktop, keyboard table, and 100 static crawler requests passed.')
} finally {
  await browser?.close()
  if (server) await new Promise(resolve => server.close(resolve))
  await rm(tmp, { recursive: true, force: true })
}
