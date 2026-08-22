import { extractAuthorPolls } from '../.vitepress/coffee-machine/content-model.js'
import { loadCoffeeMachineSource } from './coffee-machine-source.js'

const polls = extractAuthorPolls(await loadCoffeeMachineSource())

if (process.argv.includes('--json')) {
  process.stdout.write(`${JSON.stringify(polls, null, 2)}\n`)
} else {
  for (const poll of polls) {
    console.log(`\n${poll.title}`)
    console.log(poll.route)
    console.log(poll.question)
    poll.options.forEach((option) => {
      const marker = option === poll.preferred ? '★' : '•'
      console.log(`  ${marker} ${option} (${option.length}/30)`)
    })
  }
  console.log(`\n${polls.length} author-only polls validated.`)
}
