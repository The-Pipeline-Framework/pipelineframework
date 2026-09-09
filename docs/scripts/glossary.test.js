import assert from 'node:assert/strict'
import test from 'node:test'
import {createMarkdownRenderer} from 'vitepress'
import {markdownGlossaryPlugin} from 'vitepress-plugin-glossary'
import glossary from '../.vitepress/glossary.json' with {type: 'json'}

test('public glossary covers the core semantic distinctions', () => {
  assert.ok(Object.keys(glossary).length >= 50)
  for (const term of [
    'Pipeline',
    'Functional core',
    'Imperative shell',
    'Block',
    'Expansion',
    'Connector',
    'Query',
    'Command',
    'Await boundary',
    'Public OpenAPI contract',
    'Runtime layout',
    'Build topology'
  ]) {
    assert.equal(typeof glossary[term], 'string', `missing glossary term: ${term}`)
  }
  assert.match(glossary.Expansion, /not a cardinality/)
})

test('glossary plugin renders one tooltip per repeated term', async () => {
  const markdown = await createMarkdownRenderer(process.cwd())
  markdown.use(markdownGlossaryPlugin, {
    glossary,
    firstOccurrenceOnly: true
  })

  const rendered = markdown.render('A Pipeline contains another Pipeline. An Expansion is not Fan-out.')
  assert.equal((rendered.match(/<GlossaryTooltip/g) ?? []).length, 3)
  assert.match(rendered, />Pipeline<\/GlossaryTooltip>/)
  assert.match(rendered, />Expansion<\/GlossaryTooltip>/)
  assert.match(rendered, />Fan-out<\/GlossaryTooltip>/)
})
