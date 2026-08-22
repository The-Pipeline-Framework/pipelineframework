import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { PERSONAS } from '../.vitepress/coffee-machine/personas.js'

const docsRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const coffeeRoot = path.join(docsRoot, 'value', 'coffee-machine')

const personaAliases = Object.freeze({
  'Annotation Annie': 'Test Terry',
  'Architecture Arlo': 'Kubernetes Kai',
  'Autonomy Amy': 'Platform Priya',
  'Big-Bang Bert': 'Test Terry',
  'Business Bob': 'Retry Rita',
  'Cloud Cory': 'Kubernetes Kai',
  'Committee Clara': 'Platform Priya',
  'Compiler Clara': 'Build Barry',
  'Compliance Clara': 'Enterprise Edna',
  'Dashboard Deb': 'Platform Priya',
  'E2E Eddie': 'Test Terry',
  'Event Evan': 'DDD Diego',
  'Finance Finn': 'Retry Rita',
  'Fork Fiona': 'Framework Fred',
  'Function Fiona': 'Kubernetes Kai',
  'Git Gary': 'Platform Priya',
  'Java Jules': 'Microservice Mike',
  'Legacy Larry': 'Consultant Nigel',
  'Local Larry': 'Kubernetes Kai',
  'Log Larry': 'Platform Priya',
  'Marketing Max': 'Consultant Nigel',
  'Network Ned': 'Retry Rita',
  'Observability Ollie': 'Platform Priya',
  'Pipeline Pete': 'Framework Fred',
  'Polyglot Polly': 'Consultant Nigel',
  'Product Pat': 'Platform Priya',
  'Prompt Pete': 'AI Ada',
  'Purity Paula': 'Functional Fran',
  'Reactive Rhea': 'Async Andy',
  'Replay Ray': 'Retry Rita',
  'REST Rachel': 'Spring Sam',
  'Reviewer Rae': 'Codegen Carl',
  'Runtime Rita': 'Build Barry',
  'Schema Sam': 'DDD Diego',
  'Security Sid': 'Enterprise Edna',
  'Shortcut Shane': 'Framework Fred',
  'Sleepy Sam': 'Test Terry',
  'Startup Steve': 'Consultant Nigel',
  'Thread Theo': 'Async Andy',
  'Type Theo': 'DDD Diego',
  'Unit Uma': 'Mock Molly'
})

const personaIdByName = new Map(PERSONAS.map((persona) => [persona.name, persona.id]))
const stopTags = new Set(['a', 'an', 'and', 'are', 'is', 'it', 'not', 'the', 'without', 'with'])

function filesBelow(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const target = path.join(directory, entry.name)
    return entry.isDirectory() ? filesBelow(target) : [target]
  })
}

function parseSections(source, file) {
  const title = source.match(/^# (.+)$/m)?.[1]
  if (!title) {
    throw new Error(`${file}: missing H1 title`)
  }
  const matches = [...source.matchAll(/^## (.+)$/gm)]
  const sections = new Map()
  matches.forEach((match, index) => {
    const start = match.index + match[0].length
    const end = matches[index + 1]?.index ?? source.length
    sections.set(match[1], source.slice(start, end).trim())
  })
  return { title, sections }
}

function requiredSection(sections, name, file) {
  const value = sections.get(name)
  if (!value) {
    throw new Error(`${file}: missing section "${name}"`)
  }
  return value
}

function trackFor(directory, id) {
  if (directory === 'why-tpf-exists') return 'why-tpf-exists'
  if (directory === 'bring-your-existing-app') return 'bring-your-existing-app'
  if (directory === 'thinking-in-pipelines') return 'domain-modelling'
  if (directory === 'test-the-claim') return 'testing'
  if (directory === 'architecture-arguments') return id === 'connector-governance' ? 'connectors' : 'governance'
  if (directory === 'make-it-run') return id === 'platform-team-boundary' ? 'governance' : 'deployment'
  if (directory === 'keep-it-sane-in-production') {
    if (['connector-plugin-or-step', 'connectors-not-stationery'].includes(id)) return 'connectors'
    if (['dlq-and-replay', 'operational-timeline', 'retry-is-not-for-rejection'].includes(id)) return 'operations'
    return 'runtime'
  }
  if (directory === 'the-spiky-bits') {
    if (id.startsWith('ai-')) return 'ai'
    if (['compiler-without-compiler-degree', 'generation-and-diagnostics', 'typing-and-evolution'].includes(id)) return 'runtime'
    return 'governance'
  }
  throw new Error(`No topic mapping for ${directory}/${id}`)
}

function yamlScalar(value) {
  return JSON.stringify(value)
}

function yamlList(values, indent = '') {
  return values.map((value) => `${indent}- ${yamlScalar(value)}`).join('\n')
}

function migrate(file) {
  const source = fs.readFileSync(file, 'utf8')
  if (source.startsWith('---\n')) {
    return false
  }

  const { title, sections } = parseSections(source, file)
  const id = path.basename(file, '.md')
  const directory = path.basename(path.dirname(file))
  const track = trackFor(directory, id)
  const questionSection = requiredSection(sections, 'The question', file)
  const question = questionSection.match(/^\*\*(.+)\*\*$/s)?.[1]
  if (!question) throw new Error(`${file}: malformed question`)

  const misconceptions = [...requiredSection(sections, 'Three coffee-machine misconceptions', file)
    .matchAll(/^- \*\*(.+?):\*\*\s*[“"](.+?)[”"]$/gm)]
    .map((match) => {
      const canonicalName = personaAliases[match[1]] || match[1]
      const persona = personaIdByName.get(canonicalName)
      if (!persona) throw new Error(`${file}: unmapped persona "${match[1]}"`)
      return { persona, text: match[2] }
    })
  if (misconceptions.length !== 3) throw new Error(`${file}: expected three misconceptions`)

  const related = [...requiredSection(sections, 'Related FAQs', file).matchAll(/^- \[.+?\]\((.+?)\)$/gm)]
    .map((match) => path.basename(match[1].replace(/\/$/, '')))

  const pollSection = requiredSection(sections, 'Today’s coffee-machine poll', file)
  const pollQuestion = pollSection.match(/^\*\*(.+?)\*\*$/m)?.[1]
  const optionLines = [...pollSection.matchAll(/^- (.+)$/gm)].map((match) => match[1])
  const preferredLine = optionLines.find((option) => option.startsWith('**') && option.endsWith('**'))
  const options = optionLines.map((option) => option.replace(/^\*\*|\*\*$/g, ''))
  const preferred = preferredLine?.replace(/^\*\*|\*\*$/g, '')
  if (!pollQuestion || !preferred || options.length < 2) throw new Error(`${file}: malformed poll`)

  const quoteSection = requiredSection(sections, 'Pull quote', file)
  const quote = quoteSection.match(/^> [“"](.+?)[”"]$/s)?.[1]
  if (!quote) throw new Error(`${file}: malformed pull quote`)

  const tags = [...new Set([
    track,
    ...id.split('-').filter((tag) => !stopTags.has(tag))
  ])].slice(0, 6)

  const frontmatter = [
    '---',
    `title: ${yamlScalar(title)}`,
    'faq:',
    `  id: ${yamlScalar(id)}`,
    `  track: ${yamlScalar(track)}`,
    `  question: ${yamlScalar(question)}`,
    '  added: "2026-08-22"',
    'coffeeMachine:',
    '  personas:',
    ...misconceptions.flatMap((entry) => [
      `    - persona: ${yamlScalar(entry.persona)}`,
      `      text: ${yamlScalar(entry.text)}`
    ]),
    'social:',
    '  poll:',
    `    question: ${yamlScalar(pollQuestion)}`,
    '    options:',
    ...options.map((option) => `      - ${yamlScalar(option)}`),
    `    preferred: ${yamlScalar(preferred)}`,
    'fortune:',
    `  quote: ${yamlScalar(quote)}`,
    'related:',
    yamlList(related),
    'tags:',
    yamlList(tags),
    '---'
  ].join('\n')

  const body = [
    `# ${title}`,
    '',
    '## Elevator answer',
    '',
    requiredSection(sections, 'Elevator answer', file),
    '',
    '<CoffeeMisconceptions />',
    '',
    '## The real explanation',
    '',
    requiredSection(sections, 'The real explanation', file),
    '',
    '## Trade-offs',
    '',
    requiredSection(sections, 'Trade-offs', file),
    '',
    '## When TPF is not a good fit',
    '',
    requiredSection(sections, 'When TPF is not a good fit', file),
    ''
  ].join('\n')

  fs.writeFileSync(file, `${frontmatter}\n\n${body}`)
  return true
}

const faqFiles = filesBelow(coffeeRoot)
  .filter((file) => file.endsWith('.md') && path.basename(file) !== 'index.md')
  .sort()
const migrated = faqFiles.filter(migrate)
console.log(`Migrated ${migrated.length} Coffee Machine FAQs.`)
