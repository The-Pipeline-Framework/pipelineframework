import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { extractAuthorPolls } from '../.vitepress/coffee-machine/content-model.js'
import { loadCoffeeMachineSource } from './coffee-machine-source.js'

const docsRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const distRoot = path.join(docsRoot, '.vitepress', 'dist')
const textExtensions = new Set(['.html', '.js', '.json'])

function walk(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const target = path.join(directory, entry.name)
    return entry.isDirectory() ? walk(target) : [target]
  })
}

if (!fs.existsSync(distRoot)) {
  throw new Error('[coffee-machine] public build directory does not exist')
}

const polls = extractAuthorPolls(await loadCoffeeMachineSource())
const privateQuestions = polls.map((poll) => poll.question)
const forbiddenKeys = [/"social"\s*:/, /"poll"\s*:/, /"preferred"\s*:/]
const leaks = []

for (const file of walk(distRoot).filter((file) => textExtensions.has(path.extname(file)))) {
  const content = fs.readFileSync(file, 'utf8')
  for (const pattern of forbiddenKeys) {
    if (pattern.test(content)) {
      leaks.push(`${path.relative(distRoot, file)} contains ${pattern}`)
    }
  }
  for (const question of privateQuestions) {
    if (content.includes(question)) {
      leaks.push(`${path.relative(distRoot, file)} contains private poll question "${question}"`)
    }
  }
}

if (leaks.length > 0) {
  throw new Error(`[coffee-machine] public build leaked author-only metadata:\n${leaks.join('\n')}`)
}

console.log(`Coffee Machine privacy audit passed for ${polls.length} private polls.`)
