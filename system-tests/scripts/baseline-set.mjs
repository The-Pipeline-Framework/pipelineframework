#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, validateBaseline, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    baseline: {type: 'string'},
    baselineDigest: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'baseline', 'baselineDigest', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
if (!/^sha256:[0-9a-f]{64}$/.test(values.baselineDigest)) throw new Error('--baselineDigest must be immutable');
const config = validateComponentsConfig(await readJson(values.components));
const baseline = validateBaseline(await readJson(values.baseline), config);
const resolved = {
  schemaVersion: 1,
  baselineDigest: values.baselineDigest,
  baselineRevision: baseline.revision,
  components: baseline.components,
  testHarnesses: baseline.testHarnesses,
  images: baseline.images,
  candidates: []
};
await writeFile(values.output, `${JSON.stringify(resolved, null, 2)}\n`);
