#!/usr/bin/env node
import {writeFile} from 'node:fs/promises';
import {parseArgs} from 'node:util';
import {readJson, validateComponentsConfig} from './lib/contracts.mjs';
import {augmentCompatibilityTargets} from './lib/compatibility-bootstrap.mjs';

const {values} = parseArgs({
  options: {
    components: {type: 'string'},
    baseline: {type: 'string'},
    targets: {type: 'string'},
    currentHeads: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'baseline', 'targets', 'currentHeads', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const config = validateComponentsConfig(await readJson(values.components));
const augmented = augmentCompatibilityTargets(
  config,
  await readJson(values.baseline),
  await readJson(values.targets),
  await readJson(values.currentHeads)
);
await writeFile(values.output, `${JSON.stringify(augmented, null, 2)}\n`);
