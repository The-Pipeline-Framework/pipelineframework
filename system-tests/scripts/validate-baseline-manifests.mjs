#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { join } from 'node:path';
import { readJson, validateBaseline, validateCandidateManifest, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    baseline: {type: 'string'},
    manifests: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'baseline', 'manifests']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const config = validateComponentsConfig(await readJson(values.components));
const baseline = validateBaseline(await readJson(values.baseline), config);
for (const [component, pin] of Object.entries(baseline.components)) {
  const manifest = validateCandidateManifest(
    await readJson(join(values.manifests, component, 'candidate-manifest.json')),
    config
  );
  if (manifest.component !== component) throw new Error(`${component} baseline manifest has the wrong component`);
  if (manifest.sourceSha !== pin.sha) throw new Error(`${component} baseline manifest has the wrong source SHA`);
  if (manifest.candidateVersion !== pin.mavenVersion) throw new Error(`${component} baseline manifest has the wrong Maven version`);
}
process.stdout.write('Validated every Maven baseline pin against its immutable candidate manifest.\n');
