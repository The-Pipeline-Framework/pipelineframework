#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { readJson } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    plan: {type: 'string'},
    repository: {type: 'string'}
  },
  strict: true
});
if (values.plan === undefined || values.repository === undefined) throw new Error('--plan and --repository are required');
const plan = await readJson(values.plan);
for (const artifact of plan.artifacts) {
  const directory = join(values.repository, ...artifact.groupId.split('.'), artifact.artifactId, artifact.version);
  for (const file of artifact.files) {
    const bytes = await readFile(join(directory, file.name));
    const actual = createHash('sha256').update(bytes).digest('hex');
    if (actual !== file.sha256) throw new Error(`checksum mismatch for ${artifact.coordinate} ${file.name}`);
  }
}
process.stdout.write(`Verified ${plan.artifacts.length} materialized Maven artifacts.\n`);
