import assert from 'node:assert/strict'
import {readFileSync} from 'node:fs'
import test from 'node:test'

const config = readFileSync(new URL('../.vitepress/config.js', import.meta.url), 'utf8')
const evolveIndex = readFileSync(new URL('../evolve/index.md', import.meta.url), 'utf8')
const documentationAudit = readFileSync(
  new URL('../evolve/documentation-audit-2026-09.md', import.meta.url),
  'utf8'
)
const versions = readFileSync(new URL('../versions.md', import.meta.url), 'utf8')

test('the documentation audit remains an unlisted internal record', () => {
  assert.doesNotMatch(config, /documentation-audit-2026-09/)
  assert.doesNotMatch(evolveIndex, /documentation-audit-2026-09/)
  assert.match(documentationAudit, /^---\nsearch: false\n/)
  assert.match(documentationAudit, /name: robots\n\s+content: noindex/)
})

test('the versions page identifies the current and recent releases', () => {
  assert.match(versions, /- \[v26\.9\.3]\(\/\) - Current released documentation/)
  assert.match(versions, /releases\/tag\/v26\.9\.2/)
  assert.match(versions, /releases\/tag\/v26\.9\.1/)
})
