#!/usr/bin/env node
import {createWriteStream} from 'node:fs';
import {access, mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {pipeline} from 'node:stream/promises';
import {parseArgs} from 'node:util';
import {readJson} from './lib/contracts.mjs';

const {values} = parseArgs({
  options: {plan: {type: 'string'}, repository: {type: 'string'}},
  strict: true
});
if (!values.plan || !values.repository) throw new Error('--plan and --repository are required');
const {GITHUB_ACTOR, PACKAGE_TOKEN} = process.env;
if (!GITHUB_ACTOR || !PACKAGE_TOKEN) throw new Error('GITHUB_ACTOR and PACKAGE_TOKEN are required');

const plan = await readJson(values.plan);
const authorization = 'Basic ' + Buffer.from(`${GITHUB_ACTOR}:${PACKAGE_TOKEN}`).toString('base64');
for (const artifact of plan.artifacts) {
  const {repository, groupId, artifactId, version, files} = artifact;
  const segments = [...groupId.split('.'), artifactId, version];
  if (!/^[a-z\d-]+\/[a-z\d_.-]+$/i.test(repository)
      || !segments.every((part) => /^[a-z\d][a-z\d_.-]*$/i.test(part))
      || /-snapshot$/i.test(version)) {
    throw new Error(`invalid package identity: ${artifact.coordinate}`);
  }
  const directory = join(values.repository, ...segments);
  await mkdir(directory, {recursive: true});
  for (const file of files) {
    if (!['pom', 'jar'].some((type) => file.name === `${artifactId}-${version}.${type}`)) {
      throw new Error(`invalid package filename for ${artifact.coordinate}: ${file.name}`);
    }
    const target = join(directory, file.name);
    try {
      // The caller verifies every file, including entries already present in the cache.
      await access(target);
      continue;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    const url = `https://maven.pkg.github.com/${repository.toLowerCase()}/${segments.join('/')}/${file.name}`;
    const signal = AbortSignal.timeout(60_000);
    let response = await fetch(url, {headers: {authorization}, redirect: 'manual', signal});
    if (response.status === 302) {
      const location = response.headers.get('location');
      await response.body?.cancel();
      const download = location ? new URL(location, url) : undefined;
      if (download?.origin !== 'https://github-registry-files.githubusercontent.com'
          || download.username || download.password) {
        throw new Error(`unexpected package download origin for ${artifact.coordinate}`);
      }
      response = await fetch(download, {redirect: 'error', signal});
    }
    if (!response.ok) {
      await response.body?.cancel();
      throw new Error(`package HTTP ${response.status}: ${artifact.coordinate} ${file.name}`);
    }
    await pipeline(response.body, createWriteStream(target, {flags: 'wx'}));
  }
}
