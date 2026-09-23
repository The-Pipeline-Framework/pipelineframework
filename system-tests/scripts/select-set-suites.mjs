#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, selectSuites, suiteMatrix, validateComponentsConfig, validatePolicy } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    policy: {type: 'string'},
    resolvedSet: {type: 'string'},
    compatibilitySet: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'policy', 'resolvedSet', 'compatibilitySet', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const config = validateComponentsConfig(await readJson(values.components));
const policy = validatePolicy(await readJson(values.policy), config);
const resolvedSet = await readJson(values.resolvedSet);
const compatibilitySet = await readJson(values.compatibilitySet);
if (compatibilitySet.schemaVersion !== 1 || !Array.isArray(compatibilitySet.requests) || compatibilitySet.requests.length < 2) {
  throw new Error('compatibility set request is invalid');
}
const selected = new Set();
for (const request of compatibilitySet.requests) {
  const candidate = resolvedSet.candidates.find((value) => value.component === request.component);
  if (candidate === undefined) throw new Error(`resolved set is missing candidate ${request.component}`);
  for (const suite of selectSuites(request.component, policy, [], candidate.suiteHints ?? [])) selected.add(suite);
}
const matrix = suiteMatrix([...selected].sort(), policy, resolvedSet, config);
await writeFile(values.output, `${JSON.stringify({include: matrix}, null, 2)}\n`);
