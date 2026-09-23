#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, suiteMatrix, validateComponentsConfig, validatePolicy } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    policy: {type: 'string'},
    resolvedSet: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'policy', 'resolvedSet', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
const policy = validatePolicy(await readJson(values.policy), config);
const resolvedSet = await readJson(values.resolvedSet);
await writeFile(values.output, `${JSON.stringify({include: suiteMatrix(policy.fullTrain, policy, resolvedSet, config)}, null, 2)}\n`);
