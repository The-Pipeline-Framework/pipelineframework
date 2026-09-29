#!/usr/bin/env node
import {parseArgs} from 'node:util';
import {readJson, validateComponentsConfig} from './lib/contracts.mjs';
import {discoverCompatibilitySet} from './lib/compatibility-discovery.mjs';

const {values} = parseArgs({
  options: {
    components: {type: 'string'},
    pulls: {type: 'string'},
    sourceRepository: {type: 'string'},
    sourcePullRequest: {type: 'string'},
    sourceSha: {type: 'string'},
    setId: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'pulls', 'sourceRepository', 'sourcePullRequest', 'sourceSha']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const sourcePullRequest = Number(values.sourcePullRequest);
if (!Number.isSafeInteger(sourcePullRequest) || sourcePullRequest < 1) throw new Error('--sourcePullRequest must be a positive integer');

const result = discoverCompatibilitySet(
  validateComponentsConfig(await readJson(values.components)),
  await readJson(values.pulls),
  {
    sourceRepository: values.sourceRepository,
    sourcePullRequest,
    sourceSha: values.sourceSha,
    requestedSetId: values.setId ?? null
  }
);
process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
