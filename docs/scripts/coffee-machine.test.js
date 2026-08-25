import assert from 'node:assert/strict'
import test from 'node:test'
import {
  buildPublicDataset,
  validatePrivatePoll
} from '../.vitepress/coffee-machine/content-model.js'
import {
  assertPublicProjection,
  isCoffeeMachinePage,
  stripAuthorOnlyFrontmatter
} from '../.vitepress/coffee-machine/visibility-policy.js'
import {
  chooseDifferentIndex,
  searchableFaqText
} from '../.vitepress/theme/coffee-machine/public-utils.js'
import { loadCoffeeMachineSource } from './coffee-machine-source.js'

function sourcePage(id = 'example-faq', overrides = {}) {
  const base = {
    url: `/value/coffee-machine/testing/${id}`,
    frontmatter: {
      title: 'Example FAQ',
      faq: { id, track: 'testing', question: 'Does this example work?', added: '2026-08-22' },
      coffeeMachine: { personas: [{ persona: 'test-terry', text: 'The test is the architecture.' }] },
      social: { poll: { question: 'Which test wins?', options: ['Unit', 'Integration'], preferred: 'Integration' } },
      fortune: { quote: `A unique quote for ${id}.` },
      related: [],
      tags: ['testing']
    }
  }
  return {
    ...base,
    ...overrides,
    frontmatter: { ...base.frontmatter, ...(overrides.frontmatter || {}) }
  }
}

test('the real Coffee Machine corpus builds a public-safe dataset', async () => {
  const dataset = buildPublicDataset(await loadCoffeeMachineSource())
  assert.equal(dataset.faqs.length, 64)
  assert.equal(dataset.quotes.length, 64)
  assert.equal(dataset.personas.length, 20)
  assert.equal(dataset.topics.length, 10)
  const serialized = JSON.stringify(dataset)
  assert.doesNotMatch(serialized, /"social"\s*:/)
  assert.doesNotMatch(serialized, /"poll"\s*:/)
  assert.doesNotMatch(serialized, /"preferred"\s*:/)
})

test('author-only frontmatter is stripped recursively as one visibility class', () => {
  const frontmatter = { title: 'Public', social: { poll: { question: 'Private' }, future: { campaign: 'Private' } } }
  assert.deepEqual(stripAuthorOnlyFrontmatter(frontmatter), { title: 'Public' })
})

test('Coffee Machine visibility applies to current and versioned pages', () => {
  assert.equal(isCoffeeMachinePage('value/coffee-machine/testing/example.md'), true)
  assert.equal(isCoffeeMachinePage('versions/v26.8.1/value/coffee-machine/testing/example.md'), true)
  assert.equal(isCoffeeMachinePage('versions/v26.8.1/value/overview.md'), false)
})

test('public projection rejects private field names', () => {
  assert.throws(() => assertPublicProjection({ faq: { social: {} } }), /author-only field/)
  assert.throws(() => assertPublicProjection({ faq: { poll: {} } }), /author-only field/)
  assert.throws(() => assertPublicProjection({ faq: { preferred: 'A' } }), /author-only field/)
})

test('private poll validation enforces LinkedIn constraints', () => {
  const page = sourcePage()
  assert.equal(validatePrivatePoll(page).preferred, 'Integration')

  const tooFew = sourcePage('too-few')
  tooFew.frontmatter.social.poll.options = ['Only one']
  assert.throws(() => validatePrivatePoll(tooFew), /between 2 and 4/)

  const tooMany = sourcePage('too-many')
  tooMany.frontmatter.social.poll.options = ['A', 'B', 'C', 'D', 'E']
  assert.throws(() => validatePrivatePoll(tooMany), /between 2 and 4/)

  const tooLong = sourcePage('too-long')
  tooLong.frontmatter.social.poll.options = ['A', 'This LinkedIn option is definitely too long']
  assert.throws(() => validatePrivatePoll(tooLong), /exceeds 30 characters/)

  const duplicateOptions = sourcePage('duplicate-options')
  duplicateOptions.frontmatter.social.poll.options = ['Java-first', 'Java-first']
  duplicateOptions.frontmatter.social.poll.preferred = 'Java-first'
  assert.throws(() => validatePrivatePoll(duplicateOptions), /must not contain duplicates/)

  const noPreferredMatch = sourcePage('no-preferred')
  noPreferredMatch.frontmatter.social.poll.preferred = 'Something else'
  assert.throws(() => validatePrivatePoll(noPreferredMatch), /must exactly match/)
})

test('public dataset validation rejects invalid contracts', () => {
  const duplicateId = [sourcePage('same'), sourcePage('same', { url: '/other' })]
  assert.throws(() => buildPublicDataset(duplicateId), /duplicate faq.id/)

  const duplicateQuote = [sourcePage('one'), sourcePage('two')]
  duplicateQuote[1].frontmatter.fortune.quote = duplicateQuote[0].frontmatter.fortune.quote.toUpperCase()
  assert.throws(() => buildPublicDataset(duplicateQuote), /duplicate quote/)

  const unknownPersona = sourcePage('unknown-persona')
  unknownPersona.frontmatter.coffeeMachine = { personas: [{ persona: 'mystery-person', text: 'Surprise.' }] }
  assert.throws(() => buildPublicDataset([unknownPersona]), /unknown persona/)

  const missingQuote = sourcePage('missing-quote')
  missingQuote.frontmatter.fortune = {}
  assert.throws(() => buildPublicDataset([missingQuote]), /missing fortune.quote/)

  const brokenRelated = sourcePage('broken-related')
  brokenRelated.frontmatter.related = ['does-not-exist']
  assert.throws(() => buildPublicDataset([brokenRelated]), /broken related FAQ id/)

  const normalizedInvalidDate = sourcePage('invalid-date')
  normalizedInvalidDate.frontmatter.faq = {
    ...normalizedInvalidDate.frontmatter.faq,
    added: '2026-02-29'
  }
  assert.throws(() => buildPublicDataset([normalizedInvalidDate]), /valid ISO date/)

  const validLeapDate = sourcePage('valid-leap-date')
  validLeapDate.frontmatter.faq = {
    ...validLeapDate.frontmatter.faq,
    added: '2024-02-29'
  }
  assert.doesNotThrow(() => buildPublicDataset([validLeapDate]))
})

test('public utilities keep random selection and FAQ search deterministic', () => {
  assert.equal(chooseDifferentIndex(1, 0, () => 0.9), 0)
  assert.equal(chooseDifferentIndex(4, 1, () => 0), 0)
  assert.equal(chooseDifferentIndex(4, 1, () => 0.99), 3)

  const faq = buildPublicDataset([sourcePage()]).faqs[0]
  const personas = new Map([['test-terry', { name: 'Test Terry', description: 'Tests claims', worldview: 'Evidence first' }]])
  const searchable = searchableFaqText(faq, personas)
  assert.match(searchable, /test terry/)
  assert.match(searchable, /unique quote/)
  assert.match(searchable, /testing/)
})
