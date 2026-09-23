#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    output: {type: 'string'},
    repositoriesOutput: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'output', 'repositoriesOutput']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
if (typeof process.env.PACKAGE_TOKEN !== 'string' || process.env.PACKAGE_TOKEN.length === 0) {
  throw new Error('PACKAGE_TOKEN is required');
}
if (typeof process.env.GITHUB_ACTOR !== 'string' || process.env.GITHUB_ACTOR.length === 0) {
  throw new Error('GITHUB_ACTOR is required');
}

const config = validateComponentsConfig(await readJson(values.components));
const maven = Object.entries(config.components).filter(([, component]) => component.kind === 'maven');
const escapeXml = (value) => value
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')
  .replaceAll("'", '&apos;');
const servers = maven.map(([name]) => [
  '<server>',
  `<id>github-${name}</id>`,
  `<username>${escapeXml(process.env.GITHUB_ACTOR)}</username>`,
  `<password>${escapeXml(process.env.PACKAGE_TOKEN)}</password>`,
  '</server>'
].join('')).join('');
const repositories = maven
  .map(([name, component]) => `github-${name}::default::https://maven.pkg.github.com/${component.repository}`)
  .join(',');
await writeFile(values.output, `<settings><servers>${servers}</servers></settings>\n`, {mode: 0o600});
await writeFile(values.repositoriesOutput, `${repositories}\n`);
