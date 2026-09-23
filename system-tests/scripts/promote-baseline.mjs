#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { nextBaseline, readJson, validateBaseline, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    current: {type: 'string'},
    resolvedSet: {type: 'string'},
    generatedAt: {type: 'string'},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'current', 'resolvedSet', 'generatedAt', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
const current = validateBaseline(await readJson(values.current), config);
const next = nextBaseline(current, await readJson(values.resolvedSet), values.generatedAt);
validateBaseline(next, config);
await writeFile(values.output, `${JSON.stringify(next, null, 2)}\n`);
