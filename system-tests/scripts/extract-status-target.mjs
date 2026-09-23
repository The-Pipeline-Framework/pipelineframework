#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { readJson, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    event: {type: 'string'}
  },
  strict: true
});
if (values.components === undefined || values.event === undefined) throw new Error('--components and --event are required');

const config = validateComponentsConfig(await readJson(values.components));
const event = await readJson(values.event);
if (event === null || typeof event !== 'object' || Array.isArray(event)) throw new Error('candidate event must be an object');
const repositories = new Set(Object.values(config.components).map((component) => component.repository));
if (!repositories.has(event.source_repository)) throw new Error('candidate event source_repository is not an allowed repository');
if (typeof event.source_sha !== 'string' || !/^[0-9a-f]{40}$/.test(event.source_sha)) {
  throw new Error('candidate event source_sha must be a lowercase 40-character SHA');
}
const repositoryName = event.source_repository.slice(event.source_repository.indexOf('/') + 1);
process.stdout.write(`${JSON.stringify({
  repository: event.source_repository,
  repository_name: repositoryName,
  source_sha: event.source_sha
})}\n`);
