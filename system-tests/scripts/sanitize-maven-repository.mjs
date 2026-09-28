#!/usr/bin/env node
import {readdir, rm} from 'node:fs/promises';
import {join} from 'node:path';
import {parseArgs} from 'node:util';

const {values} = parseArgs({
  options: {repository: {type: 'string'}},
  strict: true
});
if (values.repository === undefined) throw new Error('--repository is required');

const resolverMetadata = (name) =>
  name === '_remote.repositories'
  || name === 'resolver-status.properties'
  || name.endsWith('.lastUpdated');

async function sanitize(directory) {
  let removedMetadata = 0;
  let removedSnapshots = 0;
  const entries = await readdir(directory, {withFileTypes: true});
  if (directory.endsWith('-SNAPSHOT') && !entries.some((entry) => entry.isDirectory())) {
    await rm(directory, {recursive: true});
    return {removedMetadata, removedSnapshots: 1};
  }
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      const nested = await sanitize(path);
      removedMetadata += nested.removedMetadata;
      removedSnapshots += nested.removedSnapshots;
    } else if (entry.isFile() && resolverMetadata(entry.name)) {
      await rm(path);
      removedMetadata += 1;
    }
  }
  return {removedMetadata, removedSnapshots};
}

const removed = await sanitize(values.repository);
process.stdout.write(`Removed ${removed.removedMetadata} Maven resolver metadata file(s) and ${removed.removedSnapshots} mutable SNAPSHOT version tree(s).\n`);
