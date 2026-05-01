import test from 'node:test'
import assert from 'node:assert/strict'

import {
  applySnapshotSpecificRewritesContent,
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

test('applySnapshotSpecificRewritesContent keeps versioned plugin redirects inside the snapshot', () => {
  const content = `---
title: Redirecting...
head:
  - - meta
    - http-equiv: refresh
      content: 2;url=/guide/development/using-plugins
search: false
---

<script setup>
window.location.replace('/guide/development/using-plugins')
</script>
`

  const updated = applySnapshotSpecificRewritesContent(content, 'guide/build/using-plugins.md', '26.4.5')

  assert.match(updated, /content: 2;url=\/versions\/v26\.4\.5\/guide\/development\/using-plugins/)
  assert.match(updated, /window\.location\.replace\('\/versions\/v26\.4\.5\/guide\/development\/using-plugins'\)/)
})

test('applySnapshotSpecificRewritesContent replaces versioned testing policy with current-policy pointer', () => {
  const content = `---
search: false
---

# Testing Guidelines for This Project

Detailed policy.
`

  const updated = applySnapshotSpecificRewritesContent(content, 'guide/evolve/testing-guidelines.md', 'v26.4.5')

  assert.equal(updated, `---
search: false
---

# Testing Guidelines for This Project

This page is maintainer process guidance rather than versioned user documentation. Contributors should use the current internal policy at [Testing Guidelines](/guide/evolve/testing-guidelines).
`)
})
