#!/usr/bin/env node
import { parseArgs } from 'node:util';
import {
  readJson,
  sha256File,
  validateCandidateEvent,
  validateCandidateManifest,
  validateComponentsConfig,
  validateEventAgainstManifest,
  validateGitHubProvenance
} from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    event: {type: 'string'},
    manifest: {type: 'string'},
    run: {type: 'string'},
    pullRequest: {type: 'string'}
  },
  strict: true
});

for (const name of ['components', 'event', 'manifest', 'run']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}

const config = validateComponentsConfig(await readJson(values.components));
const event = validateCandidateEvent(await readJson(values.event), config);
const manifest = validateCandidateManifest(await readJson(values.manifest), config);
const manifestSha256 = await sha256File(values.manifest);
validateEventAgainstManifest(event, manifest, manifestSha256);
const run = await readJson(values.run);
const pullRequest = values.pullRequest === undefined ? null : await readJson(values.pullRequest);
validateGitHubProvenance(event, run, pullRequest);
process.stdout.write(`${JSON.stringify({component: event.component, sourceSha: event.source_sha, candidateVersion: event.candidate_version, manifestSha256})}\n`);
