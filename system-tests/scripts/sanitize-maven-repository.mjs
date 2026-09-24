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
  let removed = 0;
  for (const entry of await readdir(directory, {withFileTypes: true})) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      removed += await sanitize(path);
    } else if (entry.isFile() && resolverMetadata(entry.name)) {
      await rm(path);
      removed += 1;
    }
  }
  return removed;
}

const removed = await sanitize(values.repository);
process.stdout.write(`Removed ${removed} Maven resolver metadata file(s).\n`);
