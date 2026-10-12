#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { componentForRepository, readJson, validateComponentsConfig } from './lib/contracts.mjs';
import {stackRevision} from './lib/pull-stack.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    input: {type: 'string'},
    setId: {type: 'string'},
    output: {type: 'string'},
    stacks: {type: 'string'},
    revision: {type: 'string'}
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
if (tokens.length < 1 || tokens.length > 10) {
  throw new Error('a compatibility set must contain between 1 and 10 pull requests');
}

const requests = tokens.flatMap((value) => {
  const match = /^https:\/\/github\.com\/([^/]+\/[^/]+)\/pull\/([1-9][0-9]*)\/?$/.exec(value);
  if (match === null) throw new Error(`invalid GitHub pull-request URL: ${value}`);
  const repository = match[1];
  if (repository === config.coordinationRepository) {
    console.warn(`ignoring coordination-repository pull request in candidate set: ${value}`);
    return [];
  }
  const component = componentForRepository(config, repository);
  return [{
    component,
    repository,
    repositoryName: repository.split('/')[1],
    pullRequestNumber: Number(match[2]),
    url: value.replace(/\/$/, '')
  }];
});

const stacks = JSON.parse(values.stacks ?? '[]');
if (!Array.isArray(stacks) || stacks.length > 10) throw new Error('invalid stack snapshot');
if (stacks.length) {
  if (stackRevision(stacks) !== values.revision || stacks.length !== requests.length ||
      new Set(stacks.map(stack => stack.repository)).size !== stacks.length) throw new Error('invalid stack revision');
  for (const stack of stacks) {
    const request = requests.find(r => r.repository === stack.repository);
    if (!request || !Array.isArray(stack.layers) || !stack.layers.length || stack.layers.length > 50 ||
        stack.rootPullRequestNumber !== stack.layers[0].number || stack.rootBaseRef !== stack.layers[0].baseRef ||
        request.pullRequestNumber !== stack.layers.at(-1).number ||
        new Set(stack.layers.map(layer => layer.number)).size !== stack.layers.length ||
        stack.layers.some((layer, index) => !Number.isSafeInteger(layer.number) || layer.number < 1 ||
          !/^[a-f0-9]{40}$/.test(layer.headSha) || typeof layer.headRef !== 'string' || !layer.headRef ||
          typeof layer.baseRef !== 'string' || !layer.baseRef ||
          (index > 0 && layer.baseRef !== stack.layers[index - 1].headRef))) throw new Error('invalid stack layers');
  }
}
if (requests.length < 2 && !stacks.some(stack => stack.layers.length > 1)) {
  throw new Error('a compatibility set must contain at least two component pull requests');
}

const duplicateComponents = requests
  .map(({component}) => component)
  .filter((component, index, all) => all.indexOf(component) !== index);
if (duplicateComponents.length > 0) {
  throw new Error(`compatibility set contains multiple pull requests for: ${[...new Set(duplicateComponents)].join(', ')}`);
}

requests.sort((left, right) => left.component.localeCompare(right.component));
await writeFile(values.output, `${JSON.stringify({schemaVersion: 1, id: values.setId, requests,
  ...(stacks.length ? {stackSnapshot: stacks, stackRevision: values.revision} : {})}, null, 2)}\n`);
