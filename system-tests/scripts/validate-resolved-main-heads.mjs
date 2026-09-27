#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { readJson, validateComponentsConfig, validateResolvedCandidateHeads } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    resolvedSet: {type: 'string'},
    currentHeads: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'resolvedSet', 'currentHeads']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
validateResolvedCandidateHeads(await readJson(values.resolvedSet), await readJson(values.currentHeads), config);
