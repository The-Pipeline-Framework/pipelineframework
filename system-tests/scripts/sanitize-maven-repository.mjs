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
  for (const entry of await readdir(directory, {withFileTypes: true})) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (entry.name.endsWith('-SNAPSHOT')) {
        await rm(path, {recursive: true});
        removedSnapshots += 1;
      } else {
        const nested = await sanitize(path);
        removedMetadata += nested.removedMetadata;
        removedSnapshots += nested.removedSnapshots;
      }
    } else if (entry.isFile() && resolverMetadata(entry.name)) {
      await rm(path);
      removedMetadata += 1;
    }
  }
  return {removedMetadata, removedSnapshots};
}

const removed = await sanitize(values.repository);
process.stdout.write(`Removed ${removed.removedMetadata} Maven resolver metadata file(s) and ${removed.removedSnapshots} mutable SNAPSHOT version tree(s).\n`);
