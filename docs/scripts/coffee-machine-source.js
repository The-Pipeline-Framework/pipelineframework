import { createContentLoader, resolveConfig } from 'vitepress'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

const docsRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')

export async function loadCoffeeMachineSource() {
  await resolveConfig(docsRoot, 'build', 'production')
  const loader = createContentLoader('value/coffee-machine/**/*.md')
  return loader.load()
}
