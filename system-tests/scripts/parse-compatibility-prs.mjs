#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { componentForRepository, readJson, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    input: {type: 'string'},
    setId: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'input', 'setId', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(values.setId)) {
  throw new Error('--setId must be a stable compatibility-set identifier');
}

const config = validateComponentsConfig(await readJson(values.components));
const tokens = (await readFile(values.input, 'utf8'))
  .split(/[\s,]+/)
  .map((value) => value.trim())
  .filter(Boolean);
if (tokens.length < 2 || tokens.length > 10) {
  throw new Error('a compatibility set must contain between 2 and 10 pull requests');
}

const requests = tokens.map((value) => {
  const match = /^https:\/\/github\.com\/([^/]+\/[^/]+)\/pull\/([1-9][0-9]*)\/?$/.exec(value);
  if (match === null) throw new Error(`invalid GitHub pull-request URL: ${value}`);
  const repository = match[1];
  const component = componentForRepository(config, repository);
  return {
    component,
    repository,
    repositoryName: repository.split('/')[1],
    pullRequestNumber: Number(match[2]),
    url: value.replace(/\/$/, '')
  };
});

const duplicateComponents = requests
  .map(({component}) => component)
  .filter((component, index, all) => all.indexOf(component) !== index);
if (duplicateComponents.length > 0) {
  throw new Error(`compatibility set contains multiple pull requests for: ${[...new Set(duplicateComponents)].join(', ')}`);
}

requests.sort((left, right) => left.component.localeCompare(right.component));
await writeFile(values.output, `${JSON.stringify({schemaVersion: 1, id: values.setId, requests}, null, 2)}\n`);
