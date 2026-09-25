#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, selectPostMergeSuites, selectSuites, shardMatrix, validateComponentsConfig, validatePolicy } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    policy: {type: 'string'},
    resolvedSet: {type: 'string'},
    component: {type: 'string'},
    changedPaths: {type: 'string'},
    hints: {type: 'string'},
    includePostMerge: {type: 'boolean', default: false},
    includeHeavy: {type: 'boolean', default: false},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'policy', 'resolvedSet', 'component', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
const policy = validatePolicy(await readJson(values.policy), config);
const resolvedSet = await readJson(values.resolvedSet);
const changedPaths = values.changedPaths === undefined ? [] : (await readFile(values.changedPaths, 'utf8')).split(/\r?\n/).filter(Boolean);
const hints = values.hints === undefined ? [] : values.hints.split(',').map((value) => value.trim()).filter(Boolean);
const selected = selectSuites(values.component, policy, changedPaths, hints);
if (values.includePostMerge && selected.length > 0) selected.push(...selectPostMergeSuites(values.component, policy));
if (values.includeHeavy) selected.push(...policy.componentPolicy[values.component].heavy);
const uniqueSelected = [...new Set(selected)].sort();
const matrix = shardMatrix(uniqueSelected, policy, resolvedSet, config);
await writeFile(values.output, `${JSON.stringify({include: matrix}, null, 2)}\n`);
