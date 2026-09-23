#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { componentForRepository, readJson, validateCandidateEvent, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    event: {type: 'string'}
  },
  strict: true
});
if (values.components === undefined || values.event === undefined) throw new Error('--components and --event are required');
const config = validateComponentsConfig(await readJson(values.components));
const event = validateCandidateEvent(await readJson(values.event), config);
const repositoryName = event.source_repository.slice(event.source_repository.indexOf('/') + 1);
process.stdout.write(`${JSON.stringify({
  component: componentForRepository(config, event.source_repository),
  repository: event.source_repository,
  repository_name: repositoryName,
  source_sha: event.source_sha,
  pull_request_number: event.pull_request_number,
  publication_run_id: event.publication_run_id,
  candidate_version: event.candidate_version,
  compatibility_set_id: event.compatibility_set_id
})}\n`);
