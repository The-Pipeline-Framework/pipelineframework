#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, reconcileMainCandidates, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    baseline: {type: 'string'},
    baselineDigest: {type: 'string'},
    candidates: {type: 'string', multiple: true},
    candidateDigests: {type: 'string', multiple: true},
    currentHeads: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'baseline', 'baselineDigest', 'currentHeads', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
const candidates = await Promise.all((values.candidates ?? []).map((path) => readJson(path)));
const resolved = reconcileMainCandidates(
  await readJson(values.baseline),
  values.baselineDigest,
  candidates,
  values.candidateDigests ?? [],
  await readJson(values.currentHeads),
  config
);
await writeFile(values.output, `${JSON.stringify(resolved, null, 2)}\n`);
