#!/usr/bin/env node
/*
 * Snapshot the current docs into docs/versions/<version>.
 */

import {promises as fs} from 'node:fs'
import path from 'node:path'
import process from 'node:process'
import {fileURLToPath} from 'node:url'

const VERSION_PATTERN = /^v?(\d+)\.(\d+)(?:\.(\d+))?$/

export function parseVersion(versionValue) {
  if (!versionValue) {
    throw new Error('Missing version. Use --version vX.Y or --version vX.Y.Z')
  }

  const match = versionValue.match(VERSION_PATTERN)
  if (!match) {
    throw new Error(`Invalid version "${versionValue}". Expected vX.Y or vX.Y.Z`)
  }

  const [, major, minor, patch] = match
  return {
    major: Number.parseInt(major, 10),
    minor: Number.parseInt(minor, 10),
    patch: patch === undefined ? null : Number.parseInt(patch, 10)
  }
}

export function normalizeVersion(versionValue) {
  const parsed = parseVersion(versionValue)
  return parsed.patch === null
    ? `v${parsed.major}.${parsed.minor}`
    : `v${parsed.major}.${parsed.minor}.${parsed.patch}`
}

export function compareVersionsDesc(left, right) {
  const a = parseVersion(left)
  const b = parseVersion(right)
  return (
    b.major - a.major ||
    b.minor - a.minor ||
    (b.patch ?? -1) - (a.patch ?? -1)
  )
}

const root = process.cwd()
const sourceDirs = ['guide']
const isDirectExecution =
  process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
const args = process.argv.slice(2)
const versionFlagIndex = args.findIndex(arg => arg === '--version' || arg === '-v')
const versionArg = versionFlagIndex >= 0 ? args[versionFlagIndex + 1] : null
let version = null
let versionParseError = null

if (versionArg) {
  try {
    version = normalizeVersion(versionArg)
  } catch (error) {
    versionParseError = error
  }
}

const destRoot = version ? path.join(root, 'versions', version) : null

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
  const updated = updateVersionsPageContent(content, version)
  if (updated === content) {
    return
  }
  await fs.writeFile(versionsPath, updated)
}

async function updateVersionSelector() {
  const selectorPath = path.join(root, 'versions', 'version-selector.md')
  if (!(await exists(selectorPath))) {
    return
  }
  const content = await fs.readFile(selectorPath, 'utf8')
  const updated = updateVersionSelectorContent(content, version)
  if (updated === content) {
    return
  }
  await fs.writeFile(selectorPath, updated)
}

export function updateVersionsPageContent(content, versionValue) {
  const normalizedVersion = normalizeVersion(versionValue)
  const heading = '## Previous Versions\n\n'
  const startIndex = content.indexOf(heading)
  if (startIndex === -1) {
    return content
  }

  const sectionStart = startIndex + heading.length
  const nextHeadingIndex = content.indexOf('\n## ', sectionStart)
  const sectionEnd = nextHeadingIndex === -1 ? content.length : nextHeadingIndex
  const section = content.slice(sectionStart, sectionEnd)
  const versions = Array.from(
    section.matchAll(/^- \[([^\]]+)\]\(\/versions\/[^)]+\/\) - Snapshot of the .+ docs$/gm),
    match => match[1]
  )

  if (versions.includes(normalizedVersion)) {
    return content
  }

  const sortedVersions = [...versions, normalizedVersion].sort(compareVersionsDesc)
  const entries = sortedVersions
    .map(name => `- [${name}](/versions/${name}/) - Snapshot of the ${name} docs`)
    .join('\n')

  return content.slice(0, sectionStart) + `${entries}\n` + content.slice(sectionEnd)
}

export function updateVersionSelectorContent(content, versionValue) {
  const normalizedVersion = normalizeVersion(versionValue)
  const lines = content.split('\n')
  const startIndex = lines.findIndex(line => line.includes('versions: ['))
  if (startIndex === -1) {
    return content
  }

  const endIndex = lines.findIndex((line, index) => index > startIndex && line.trim() === ']')
  if (endIndex === -1) {
    return content
  }

  const entryPattern = /^(\s*)\{ name: '([^']+)', url: '([^']+)', current: (true|false) }[,]?$/
  const entries = lines
    .slice(startIndex + 1, endIndex)
    .map(line => {
      const match = line.match(entryPattern)
      if (!match) {
        return null
      }
      const [, indent, name, url, current] = match
      return {indent, name, url, current: current === 'true'}
    })
    .filter(Boolean)

  if (entries.some(entry => entry.name === normalizedVersion && entry.url === `/versions/${normalizedVersion}/`)) {
    return content
  }

  const indent = entries[0]?.indent ?? '        '
  const currentEntries = entries.filter(entry => entry.current)
  const snapshotEntries = entries.filter(entry => !entry.current)
  snapshotEntries.push({
    indent,
    name: normalizedVersion,
    url: `/versions/${normalizedVersion}/`,
    current: false
  })

  const dedupedSnapshots = Array.from(
    new Map(snapshotEntries.map(entry => [entry.url, entry])).values()
  ).sort((left, right) => compareVersionsDesc(left.name, right.name))

  const rebuiltEntries = [...currentEntries, ...dedupedSnapshots].map((entry, index, allEntries) => {
    const suffix = index === allEntries.length - 1 ? '' : ','
    return `${indent}{ name: '${entry.name}', url: '${entry.url}', current: ${entry.current} }${suffix}`
  })

  const updatedLines = [
    ...lines.slice(0, startIndex + 1),
    ...rebuiltEntries,
    ...lines.slice(endIndex)
  ]
  return updatedLines.join('\n')
}

async function main() {
  await ensureCleanDestination()
  await copySources()
  await applySearchExclusion()
  await updateVersionsPage()
  await updateVersionSelector()
  console.log(`Docs snapshot created at docs/versions/${version}`)
}

if (isDirectExecution) {
  if (!version || versionParseError) {
    if (versionParseError) {
      console.error(versionParseError.message)
    }
    console.error('Usage: npm run snapshot -- --version vX.Y[.Z]')
    process.exit(1)
  }

  main().catch(err => {
    console.error(err.message)
    process.exit(1)
  })
}
