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
        await applySnapshotSpecificRewrites(entryPath)
      }
    }
  }
  await walk(destRoot)
}

async function applySnapshotSpecificRewrites(filePath) {
  const relativePath = path.relative(destRoot, filePath)
  const content = await fs.readFile(filePath, 'utf8')
  const updated = applySnapshotSpecificRewritesContent(content, relativePath, version)
  if (updated !== content) {
    await fs.writeFile(filePath, updated)
  }
}

export function applySnapshotSpecificRewritesContent(content, relativePath, versionValue) {
  const normalizedVersion = normalizeVersion(versionValue)

  if (relativePath === path.join('guide', 'build', 'using-plugins.md')) {
    const target = `/versions/${normalizedVersion}/guide/development/using-plugins`
    return content
      .replace('content: 2;url=/guide/development/using-plugins', `content: 2;url=${target}`)
      .replace("window.location.replace('/guide/development/using-plugins')", `window.location.replace('${target}')`)
  }

  if (relativePath === path.join('guide', 'evolve', 'testing-guidelines.md')) {
    return `---
search: false
---

# Testing Guidelines for This Project

This page is maintainer process guidance rather than versioned user documentation. Contributors should use the current internal policy at [Testing Guidelines](/guide/evolve/testing-guidelines).
`
  }

  return content
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
  const versionLinePattern = /^- \[([^\]]+)\]\(\/versions\/[^)]+\/\) - Snapshot of the .+ docs$/
  const sectionLines = section.split('\n')

  const existingVersions = []
  for (const line of sectionLines) {
    const match = line.match(versionLinePattern)
    if (match) {
      existingVersions.push(match[1])
    }
  }

  if (existingVersions.includes(normalizedVersion)) {
    return content
  }

  const sortedVersions = [...existingVersions, normalizedVersion].sort(compareVersionsDesc)
  const newVersionLine = `- [${normalizedVersion}](/versions/${normalizedVersion}/) - Snapshot of the ${normalizedVersion} docs`
  const insertPosition = sortedVersions.indexOf(normalizedVersion)

  let versionCount = 0
  const updatedSectionLines = []
  let inserted = false

  for (const line of sectionLines) {
    const match = line.match(versionLinePattern)
    if (match) {
      if (!inserted && versionCount === insertPosition) {
        updatedSectionLines.push(newVersionLine)
        inserted = true
      }
      versionCount++
    }
    updatedSectionLines.push(line)
  }

  if (!inserted) {
    updatedSectionLines.push(newVersionLine)
  }

  const result = updatedSectionLines.join('\n')
  const needsTrailingNewline = section.endsWith('\n') && !result.endsWith('\n')
  return content.slice(0, sectionStart) + result + (needsTrailingNewline ? '\n' : '') + content.slice(sectionEnd)
}

export function updateVersionSelectorContent(content, versionValue) {
  const normalizedVersion = normalizeVersion(versionValue)
  const lines = content.split('\n')
  const startIndex = lines.findIndex(line => line.includes('versions: ['))
  if (startIndex === -1) {
    return content
  }

  const endIndex = lines.findIndex((line, index) => index > startIndex && /^\],?$/.test(line.trim()))
  if (endIndex === -1) {
    return content
  }

  const entryPattern = /^(\s*)\{ name: '([^']+)', url: '([^']+)', current: (true|false) }[,]?$/
  const parsedLines = lines
    .slice(startIndex + 1, endIndex)
    .map(rawLine => {
      const match = rawLine.match(entryPattern)
      if (!match) {
        return {rawLine, parsedEntry: null}
      }
      const [, indent, name, url, current] = match
      return {rawLine, parsedEntry: {indent, name, url, current: current === 'true'}}
    })

  const entries = parsedLines
    .filter(item => item.parsedEntry !== null)
    .map(item => item.parsedEntry)

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

  const sortedEntries = [...currentEntries, ...dedupedSnapshots]

  const rebuiltLines = []
  let entryIndex = 0
  let inserted = false

  for (const item of parsedLines) {
    if (item.parsedEntry === null) {
      rebuiltLines.push(item.rawLine)
      continue
    }

    const expectedEntry = sortedEntries[entryIndex]
    const insertPosition = sortedEntries.indexOf(expectedEntry)
    const shouldInsertBefore = !inserted && expectedEntry.name !== item.parsedEntry.name

    if (shouldInsertBefore) {
      const nextEntry = sortedEntries[entryIndex + 1]
      const hasMore = entryIndex + 1 < sortedEntries.length
      const suffix = hasMore ? ',' : ''
      rebuiltLines.push(`${expectedEntry.indent}{ name: '${expectedEntry.name}', url: '${expectedEntry.url}', current: ${expectedEntry.current} }${suffix}`)
      inserted = true
      entryIndex++
    }

    const currentEntry = sortedEntries[entryIndex]
    const hasMore = entryIndex + 1 < sortedEntries.length
    const suffix = hasMore ? ',' : ''
    rebuiltLines.push(`${currentEntry.indent}{ name: '${currentEntry.name}', url: '${currentEntry.url}', current: ${currentEntry.current} }${suffix}`)
    entryIndex++
  }

  if (!inserted && entryIndex < sortedEntries.length) {
    const lastEntry = sortedEntries[entryIndex]
    rebuiltLines.push(`${lastEntry.indent}{ name: '${lastEntry.name}', url: '${lastEntry.url}', current: ${lastEntry.current} }`)
  }

  const updatedLines = [
    ...lines.slice(0, startIndex + 1),
    ...rebuiltLines,
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
