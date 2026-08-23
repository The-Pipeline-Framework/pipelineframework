import { PERSONAS } from './personas.js'
import { TOPICS } from './topics.js'
import { assertPublicProjection } from './visibility-policy.js'

const ISO_DATE = /^\d{4}-\d{2}-\d{2}$/
const FAQ_ID = /^[a-z0-9]+(?:-[a-z0-9]+)*$/

const personaById = new Map(PERSONAS.map((persona) => [persona.id, persona]))
const topicById = new Map(TOPICS.map((topic) => [topic.id, topic]))
const topicOrder = new Map(TOPICS.map((topic, index) => [topic.id, index]))

function fail(page, message) {
  const source = page?.url || page?.frontmatter?.faq?.id || 'unknown Coffee Machine FAQ'
  throw new Error(`[coffee-machine] ${source}: ${message}`)
}

function requireString(page, value, field) {
  if (typeof value !== 'string' || value.trim() === '') {
    fail(page, `missing ${field}`)
  }
  return value.trim()
}

function normalizeQuote(quote) {
  return quote.trim().replace(/\s+/g, ' ').toLocaleLowerCase('en-US')
}

export function validatePrivatePoll(page) {
  const poll = page.frontmatter?.social?.poll
  if (!poll || typeof poll !== 'object') {
    fail(page, 'missing author-only social.poll')
  }

  const question = requireString(page, poll.question, 'social.poll.question')
  if (!Array.isArray(poll.options) || poll.options.length < 2 || poll.options.length > 4) {
    fail(page, 'social.poll.options must contain between 2 and 4 options')
  }
  const options = poll.options.map((option, index) => {
    const normalized = requireString(page, option, `social.poll.options[${index}]`)
    if (normalized.length > 30) {
      fail(page, `LinkedIn option exceeds 30 characters: "${normalized}"`)
    }
    return normalized
  })
  if (new Set(options).size !== options.length) {
    fail(page, 'social.poll.options must not contain duplicates')
  }
  const preferred = requireString(page, poll.preferred, 'social.poll.preferred')
  if (!options.includes(preferred)) {
    fail(page, 'social.poll.preferred must exactly match one option')
  }
  return Object.freeze({ question, options: Object.freeze(options), preferred })
}

function normalizeFaq(page) {
  const frontmatter = page.frontmatter || {}
  const faq = frontmatter.faq || {}
  const id = requireString(page, faq.id, 'faq.id')
  if (!FAQ_ID.test(id)) {
    fail(page, `faq.id must be lowercase kebab-case: "${id}"`)
  }
  const title = requireString(page, frontmatter.title, 'title')
  const track = requireString(page, faq.track, 'faq.track')
  if (!topicById.has(track)) {
    fail(page, `unknown faq.track "${track}"`)
  }
  const question = requireString(page, faq.question, 'faq.question')
  const added = requireString(page, faq.added, 'faq.added')
  const parsedAdded = Date.parse(`${added}T00:00:00Z`)
  if (
    !ISO_DATE.test(added)
    || Number.isNaN(parsedAdded)
    || new Date(parsedAdded).toISOString().slice(0, 10) !== added
  ) {
    fail(page, `faq.added must be a valid ISO date: "${added}"`)
  }

  const statements = frontmatter.coffeeMachine?.personas
  if (!Array.isArray(statements) || statements.length === 0) {
    fail(page, 'coffeeMachine.personas must contain at least one misconception')
  }
  const personas = statements.map((entry, index) => {
    const persona = requireString(page, entry?.persona, `coffeeMachine.personas[${index}].persona`)
    if (!personaById.has(persona)) {
      fail(page, `unknown persona "${persona}"`)
    }
    return Object.freeze({
      persona,
      text: requireString(page, entry?.text, `coffeeMachine.personas[${index}].text`)
    })
  })

  const quote = requireString(page, frontmatter.fortune?.quote, 'fortune.quote')
  const related = frontmatter.related
  if (!Array.isArray(related)) {
    fail(page, 'related must be an array of FAQ ids')
  }
  const relatedIds = related.map((id, index) => requireString(page, id, `related[${index}]`))
  if (new Set(relatedIds).size !== relatedIds.length) {
    fail(page, 'related contains duplicate FAQ ids')
  }
  if (relatedIds.includes(id)) {
    fail(page, 'related must not refer to the current FAQ')
  }

  if (!Array.isArray(frontmatter.tags) || frontmatter.tags.length === 0) {
    fail(page, 'tags must contain at least one tag')
  }
  const tags = frontmatter.tags.map((tag, index) => requireString(page, tag, `tags[${index}]`))
  if (new Set(tags).size !== tags.length) {
    fail(page, 'tags contains duplicates')
  }

  validatePrivatePoll(page)

  return Object.freeze({
    id,
    title,
    route: requireString(page, page.url, 'route'),
    track,
    question,
    added,
    personas: Object.freeze(personas),
    quote,
    related: Object.freeze(relatedIds),
    tags: Object.freeze(tags)
  })
}

function sourceFaqPages(pages) {
  return pages.filter((page) => page.frontmatter?.faq)
}

export function buildPublicDataset(pages) {
  const faqs = sourceFaqPages(pages).map(normalizeFaq)
  if (faqs.length === 0) {
    throw new Error('[coffee-machine] no FAQ pages found')
  }

  const faqById = new Map()
  const quoteByValue = new Map()
  for (const faq of faqs) {
    if (faqById.has(faq.id)) {
      throw new Error(`[coffee-machine] duplicate faq.id "${faq.id}"`)
    }
    faqById.set(faq.id, faq)

    const quoteKey = normalizeQuote(faq.quote)
    if (quoteByValue.has(quoteKey)) {
      throw new Error(`[coffee-machine] duplicate quote in "${quoteByValue.get(quoteKey)}" and "${faq.id}"`)
    }
    quoteByValue.set(quoteKey, faq.id)
  }

  for (const faq of faqs) {
    for (const relatedId of faq.related) {
      if (!faqById.has(relatedId)) {
        throw new Error(`[coffee-machine] ${faq.id}: broken related FAQ id "${relatedId}"`)
      }
    }
  }

  faqs.sort((left, right) =>
    topicOrder.get(left.track) - topicOrder.get(right.track) ||
    left.question.localeCompare(right.question, 'en')
  )

  const quotes = faqs.map(({ id: faqId, title, route, track, quote }) =>
    Object.freeze({ faqId, title, route, track, quote })
  )

  const personas = PERSONAS.map((persona) => {
    const appearances = faqs.flatMap((faq) => faq.personas
      .filter((entry) => entry.persona === persona.id)
      .map((entry) => Object.freeze({ faqId: faq.id, title: faq.title, route: faq.route, text: entry.text })))
    return Object.freeze({
      ...persona,
      appearances: Object.freeze(appearances),
      favoriteMisconceptions: Object.freeze(appearances.slice(0, 3))
    })
  }).filter((persona) => persona.appearances.length > 0)

  const topics = TOPICS.map((topic) => Object.freeze({
    ...topic,
    count: faqs.filter((faq) => faq.track === topic.id).length
  }))

  return assertPublicProjection(Object.freeze({
    faqs: Object.freeze(faqs),
    quotes: Object.freeze(quotes),
    personas: Object.freeze(personas),
    topics: Object.freeze(topics)
  }))
}

export function extractAuthorPolls(pages) {
  return sourceFaqPages(pages)
    .map((page) => {
      const poll = validatePrivatePoll(page)
      return Object.freeze({
        title: requireString(page, page.frontmatter.title, 'title'),
        route: requireString(page, page.url, 'route'),
        ...poll
      })
    })
    .sort((left, right) => left.title.localeCompare(right.title, 'en'))
}
