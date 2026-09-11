import assert from 'node:assert/strict'
import {readFileSync} from 'node:fs'
import test from 'node:test'

const readDocsFile = path => readFileSync(new URL(`../` + path, import.meta.url), 'utf8')

const homepage = readDocsFile('index.md')
const repositoryReadme = readFileSync(new URL('../../README.md', import.meta.url), 'utf8')
const valuePages = [
  'value/index.md',
  'value/ai-and-agentic-applications.md',
  'value/saas-integration.md',
  'value/business-value.md',
  'value/developer-experience.md',
  'value/runtime-efficiency.md',
  'value/integration-flexibility.md',
  'value/state-replay-and-queryable-data.md',
  'value/deployment-evolution.md',
  'value/operational-confidence.md',
  'value/extensibility-and-platform.md'
]

test('homepage leads with current value instead of legacy entry points', () => {
  assert.match(homepage, /name: Build with AI\./)
  assert.match(homepage, /text: Run with guarantees\./)
  assert.match(homepage, /gh skill install The-Pipeline-Framework\/pipelineframework tpf-authoring/)
  assert.match(homepage, /Integration Capability Families/)
  assert.match(homepage, /GraphQL Expansion/)
  assert.match(homepage, /OpenAPI Expansion/)

  for (const legacyFurniture of [
    'Quick Start',
    'Code a Step',
    '<FeaturedArticles />',
    '<LatestReleases />'
  ]) {
    assert.equal(homepage.includes(legacyFurniture), false, `legacy homepage content: ` + legacyFurniture)
  }

  for (const releaseLedger of [
    'Current, Experimental, and Next',
    'OpenAPI Is Landing Next',
    'not shipped yet'
  ]) {
    assert.equal(homepage.includes(releaseLedger), false, `release ledger content: ` + releaseLedger)
  }
})

test('GitHub README carries the same release positioning without becoming a quick start', () => {
  assert.match(repositoryReadme, /## Build with AI\. Run with guarantees\./)
  assert.match(repositoryReadme, /Composable agentic applications/)
  assert.match(repositoryReadme, /GraphQL Expansion/)
  assert.match(repositoryReadme, /OpenAPI/)
  assert.match(repositoryReadme, /gh skill install The-Pipeline-Framework\/pipelineframework tpf-authoring/)
  assert.equal(repositoryReadme.includes('Download the generated application scaffold'), false)
  assert.equal(repositoryReadme.includes('pipelineframework.org/design/'), false)
})

test('value pages use diagrams and present the publication capability set', () => {
  for (const page of valuePages) {
    assert.match(readDocsFile(page), /```mermaid/, `missing Mermaid model: ` + page)
  }

  const saas = readDocsFile('value/saas-integration.md')
  assert.match(saas, /## OpenAPI Expansion/)
  assert.match(saas, /Command with await: deferred completion/)
  assert.match(saas, /Direct mapping/)
  assert.match(saas, /Bounded deterministic options/)
  assert.match(saas, /Optional LLM authoring Block/)
  assert.match(saas, /Curated DTO and Mapper/)
  assert.match(saas, /public OpenAPI contract filter[\s\S]*opposite problem/)
  assert.equal(/Landing Next|not shipped yet/.test(saas), false)

  const agentic = readDocsFile('value/ai-and-agentic-applications.md')
  assert.match(agentic, /The Loop Is Application Composition/)
  assert.match(agentic, /The GraphQL Expansion Proves the Model/)
  assert.match(agentic, /pipeline: graphql-agent/)
})
