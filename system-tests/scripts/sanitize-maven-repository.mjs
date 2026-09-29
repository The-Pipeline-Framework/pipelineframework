#!/usr/bin/env node
import {readdir, rm} from 'node:fs/promises';
import {basename, dirname, join} from 'node:path';
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
  if (isSnapshotVersionDirectory(directory, entries)) {
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

function isSnapshotVersionDirectory(directory, entries) {
  const version = basename(directory);
  if (!version.endsWith('-SNAPSHOT')) return false;
  const artifactId = basename(dirname(directory));
  const snapshotPrefix = `${artifactId}-${version.slice(0, -'SNAPSHOT'.length)}`;
  return entries.some((entry) => entry.isFile() && (
    entry.name.startsWith(`${artifactId}-${version}.`)
    || (entry.name.startsWith(snapshotPrefix)
      && /^\d{8}\.\d{6}-\d+(?:[.-])/.test(entry.name.slice(snapshotPrefix.length)))
  ));
}

const removed = await sanitize(values.repository);
process.stdout.write(`Removed ${removed.removedMetadata} Maven resolver metadata file(s) and ${removed.removedSnapshots} mutable SNAPSHOT version tree(s).\n`);
