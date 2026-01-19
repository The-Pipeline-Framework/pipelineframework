#!/usr/bin/env node
/*
 * Snapshot the current docs into docs/versions/<version>.
 */

import {promises as fs} from 'node:fs'
import path from 'node:path'
import process from 'node:process'

const args = process.argv.slice(2)
const versionFlagIndex = args.findIndex(arg => arg === '--version' || arg === '-v')
const version = versionFlagIndex >= 0 ? args[versionFlagIndex + 1] : null

if (!version) {
  console.error('Usage: npm run snapshot -- --version vX.Y.Z')
  process.exit(1)
}

const root = process.cwd()
const sourceDirs = ['guide']
const destRoot = path.join(root, 'versions', version)

async function exists(p) {
  try {
    await fs.access(p)
    return true
  } catch {
    return false
  }
}

async function ensureCleanDestination() {
  if (await exists(destRoot)) {
    throw new Error(`Destination already exists: ${destRoot}`)
  }
  await fs.mkdir(destRoot, {recursive: true})
}

async function copySources() {
  for (const dir of sourceDirs) {
    const sourcePath = path.join(root, dir)
    if (await exists(sourcePath)) {
      const destPath = path.join(destRoot, dir)
      await fs.cp(sourcePath, destPath, {recursive: true})
    }
  }
  const indexPath = path.join(root, 'index.md')
  if (await exists(indexPath)) {
    await fs.copyFile(indexPath, path.join(destRoot, 'index.md'))
  }
}

async function applySearchExclusion() {
  async function walk(dir) {
    const entries = await fs.readdir(dir, {withFileTypes: true})
    for (const entry of entries) {
      const entryPath = path.join(dir, entry.name)
      if (entry.isDirectory()) {
        await walk(entryPath)
      } else if (entry.isFile() && entry.name.endsWith('.md')) {
        await ensureFrontmatterFlag(entryPath, 'search', 'false')
        await rewriteInternalLinks(entryPath)
      }
    }
  }
  await walk(destRoot)
}

async function rewriteInternalLinks(filePath) {
  const content = await fs.readFile(filePath, 'utf8')
  const versionPrefix = `/versions/${version}`
  let updated = content

  const replacements = [
    {from: '](/)', to: `](${versionPrefix}/)`},
    {from: '](/index)', to: `](${versionPrefix}/index)`},
    {from: '](/guide)', to: `](${versionPrefix}/guide)`},
    {from: '](/guide/', to: `](${versionPrefix}/guide/`},
    {from: 'href="/"', to: `href="${versionPrefix}/"`},
    {from: 'href="/index"', to: `href="${versionPrefix}/index"`},
    {from: 'href="/guide"', to: `href="${versionPrefix}/guide"`},
    {from: 'href="/guide/', to: `href="${versionPrefix}/guide/`},
    {from: "href='/'", to: `href='${versionPrefix}/'`},
    {from: "href='/index'", to: `href='${versionPrefix}/index'`},
    {from: "href='/guide'", to: `href='${versionPrefix}/guide'`},
    {from: "href='/guide/", to: `href='${versionPrefix}/guide/`}
  ]

  for (const {from, to} of replacements) {
    updated = updated.split(from).join(to)
  }

  if (updated !== content) {
    await fs.writeFile(filePath, updated)
  }
}

async function ensureFrontmatterFlag(filePath, key, value) {
  const content = await fs.readFile(filePath, 'utf8')
  if (content.startsWith('---\n')) {
    const endIndex = content.indexOf('\n---', 4)
    if (endIndex !== -1) {
      const frontmatter = content.slice(4, endIndex + 1)
      if (frontmatter.includes(`${key}:`)) {
        return
      }
      const updatedFrontmatter = `${frontmatter}${key}: ${value}\n`
      const updated =
        `---\n${updatedFrontmatter}---` +
        content.slice(endIndex + '\n---'.length)
      await fs.writeFile(filePath, updated)
      return
    }
  }
  const updated = `---\n${key}: ${value}\n---\n\n${content}`
  await fs.writeFile(filePath, updated)
}

async function updateVersionsPage() {
  const versionsPath = path.join(root, 'versions.md')
  if (!(await exists(versionsPath))) {
    return
  }
  const content = await fs.readFile(versionsPath, 'utf8')
  const entry = `- [${version}](/versions/${version}/) - Snapshot of the ${version} docs`
  if (content.includes(entry)) {
    return
  }
  const updated = content.replace(
    /## Previous Versions\\n\\n/,
    `## Previous Versions\\n\\n${entry}\\n`
  )
  await fs.writeFile(versionsPath, updated)
}

async function updateVersionSelector() {
  const selectorPath = path.join(root, 'versions', 'version-selector.md')
  if (!(await exists(selectorPath))) {
    return
  }
  const content = await fs.readFile(selectorPath, 'utf8')
  const entry = `        { name: '${version}', url: '/versions/${version}/', current: false }`
  if (content.includes(entry)) {
    return
  }
  const updated = content.replace(
    'versions: [\n',
    `versions: [\n${entry},\n`
  )
  await fs.writeFile(selectorPath, updated)
}

async function main() {
  await ensureCleanDestination()
  await copySources()
  await applySearchExclusion()
  await updateVersionsPage()
  await updateVersionSelector()
  console.log(`Docs snapshot created at docs/versions/${version}`)
}

main().catch(err => {
  console.error(err.message)
  process.exit(1)
})
