#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { overlayBaseline, readJson, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    baseline: {type: 'string'},
    baselineDigest: {type: 'string'},
    candidates: {type: 'string', multiple: true},
    candidateDigests: {type: 'string', multiple: true},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'baseline', 'baselineDigest', 'candidates', 'candidateDigests', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
const baseline = await readJson(values.baseline);
const candidates = await Promise.all(values.candidates.map((path) => readJson(path)));
const resolved = overlayBaseline(baseline, values.baselineDigest, candidates, values.candidateDigests, config);
await writeFile(values.output, `${JSON.stringify(resolved, null, 2)}\n`);
