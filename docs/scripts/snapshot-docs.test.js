import test from 'node:test'
import assert from 'node:assert/strict'

import {
  compareVersionsDesc,
  normalizeVersion,
  updateVersionSelectorContent,
  updateVersionsPageContent
} from './snapshot-docs.js'

test('normalizeVersion accepts minor and patch releases', () => {
  assert.equal(normalizeVersion('26.4'), 'v26.4')
  assert.equal(normalizeVersion('26.4.2'), 'v26.4.2')
  assert.equal(normalizeVersion('v2.6.4'), 'v2.6.4')
})

test('compareVersionsDesc distinguishes full semantic versions numerically', () => {
  const versions = ['v2.6.4', 'v26.4.3', 'v26.4.2', 'v26.4', 'v10.9.8']
  assert.deepEqual(
    versions.sort(compareVersionsDesc),
    ['v26.4.3', 'v26.4.2', 'v26.4', 'v10.9.8', 'v2.6.4']
  )
})

test('updateVersionsPageContent inserts patch versions in descending order', () => {
  const content = `# Versions

## Previous Versions

- [v26.4.1](/versions/v26.4.1/) - Snapshot of the v26.4.1 docs
- [v2.6.4](/versions/v2.6.4/) - Snapshot of the v2.6.4 docs

## About Versioning
`

  const updated = updateVersionsPageContent(content, '26.4.2')

  assert.match(
    updated,
    /- \[v26\.4\.2]\(\/versions\/v26\.4\.2\/\) - Snapshot of the v26\.4\.2 docs\n- \[v26\.4\.1]\(\/versions\/v26\.4\.1\/\) - Snapshot of the v26\.4\.1 docs\n- \[v2\.6\.4]\(\/versions\/v2\.6\.4\/\) - Snapshot of the v2\.6\.4 docs/
  )
})

test('updateVersionSelectorContent inserts snapshots using semantic version order', () => {
  const content = `export default {
  data() {
    return {
      versions: [
        { name: 'v26.4', url: '/', current: true },
        { name: 'v26.4.1', url: '/versions/v26.4.1/', current: false },
        { name: 'v2.6.4', url: '/versions/v2.6.4/', current: false }
      ]
    }
  }
}
`

  const updated = updateVersionSelectorContent(content, '26.4.2')

  assert.match(
    updated,
    /\{ name: 'v26\.4', url: '\/', current: true },\n        \{ name: 'v26\.4\.2', url: '\/versions\/v26\.4\.2\/', current: false },\n        \{ name: 'v26\.4\.1', url: '\/versions\/v26\.4\.1\/', current: false },\n        \{ name: 'v2\.6\.4', url: '\/versions\/v2\.6\.4\/', current: false }/
  )
})
