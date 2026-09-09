import assert from 'node:assert/strict'
import {readFileSync} from 'node:fs'
import test from 'node:test'

const mermaidEnhancer = readFileSync(
  new URL('../.vitepress/theme/components/MermaidDiagramEnhancer.vue', import.meta.url),
  'utf8'
)
const mermaidStyles = readFileSync(
  new URL('../.vitepress/theme/mermaid.css', import.meta.url),
  'utf8'
)
const glossaryStyles = readFileSync(
  new URL('../.vitepress/theme/glossary.css', import.meta.url),
  'utf8'
)

test('Mermaid lightbox scopes cloned SVG identifiers', () => {
  assert.match(mermaidEnhancer, /activeSvg\.value = lightboxSvgMarkup\(svg\)/)
  assert.doesNotMatch(mermaidEnhancer, /activeSvg\.value = svg\.outerHTML/)
  assert.match(mermaidEnhancer, /element\.id = replacement/)
  assert.match(mermaidEnhancer, /scopeReferences\(style\.textContent/)
})

test('expanded Mermaid diagrams stay readable at desktop and mobile widths', () => {
  assert.match(mermaidStyles, /min-width: min\(100%, 1400px\)/)
  assert.match(mermaidStyles, /\.tpf-mermaid-lightbox__stage \{[\s\S]*?overflow: auto/)
  assert.doesNotMatch(mermaidStyles, /min-width: min\(1400px, 160vw\)/)
})

test('glossary tooltips wrap and remain viewport bounded', () => {
  assert.match(glossaryStyles, /white-space: normal !important/)
  assert.match(glossaryStyles, /max-width: min\(20rem, calc\(100vw - 2rem\)\)/)
  assert.match(glossaryStyles, /position: fixed/)
})
