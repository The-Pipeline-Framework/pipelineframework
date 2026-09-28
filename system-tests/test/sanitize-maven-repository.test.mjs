import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {mkdir, mkdtemp, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {promisify} from 'node:util';

const execFileAsync = promisify(execFile);
const sanitizer = new URL('../scripts/sanitize-maven-repository.mjs', import.meta.url).pathname;

test('removes resolver provenance and mutable snapshots while preserving immutable artifacts', async () => {
  const repository = await mkdtemp(join(tmpdir(), 'tpf-portable-maven-'));
  const artifact = join(repository, 'org', 'pipelineframework', 'example', '1.0.0');
  await mkdir(artifact, {recursive: true});
  await Promise.all([
    writeFile(join(artifact, 'example-1.0.0.jar'), 'jar'),
    writeFile(join(artifact, 'example-1.0.0.pom'), 'pom'),
    writeFile(join(artifact, 'example-1.0.0.jar.sha1'), 'checksum'),
    writeFile(join(artifact, '_remote.repositories'), 'example-1.0.0.jar>github='),
    writeFile(join(artifact, 'example-1.0.0.jar.lastUpdated'), 'cached failure'),
    writeFile(join(artifact, 'resolver-status.properties'), 'origin=github')
  ]);
  const snapshot = join(repository, 'org', 'pipelineframework', 'example', '1.1.0-SNAPSHOT');
  await mkdir(snapshot, {recursive: true});
  await writeFile(join(snapshot, 'example-1.1.0-SNAPSHOT.jar'), 'mutable');
  const snapshotNamedArtifact = join(repository, 'org', 'pipelineframework', 'example-SNAPSHOT', '1.0.0');
  await mkdir(snapshotNamedArtifact, {recursive: true});
  await writeFile(join(snapshotNamedArtifact, 'example-SNAPSHOT-1.0.0.jar'), 'immutable');
  const nestedSnapshot = join(repository, 'org', 'pipelineframework', 'example-SNAPSHOT', '1.1.0-SNAPSHOT');
  await mkdir(nestedSnapshot, {recursive: true});
  await writeFile(join(nestedSnapshot, 'example-SNAPSHOT-1.1.0-SNAPSHOT.jar'), 'mutable');

  const {stdout} = await execFileAsync(process.execPath, [sanitizer, '--repository', repository]);
  assert.equal(stdout, 'Removed 3 Maven resolver metadata file(s) and 2 mutable SNAPSHOT version tree(s).\n');
  assert.equal(await readFile(join(artifact, 'example-1.0.0.jar'), 'utf8'), 'jar');
  assert.equal(await readFile(join(artifact, 'example-1.0.0.pom'), 'utf8'), 'pom');
  assert.equal(await readFile(join(artifact, 'example-1.0.0.jar.sha1'), 'utf8'), 'checksum');
  assert.equal(await readFile(join(snapshotNamedArtifact, 'example-SNAPSHOT-1.0.0.jar'), 'utf8'), 'immutable');
  await assert.rejects(readFile(join(artifact, '_remote.repositories')));
  await assert.rejects(readFile(join(artifact, 'example-1.0.0.jar.lastUpdated')));
  await assert.rejects(readFile(join(artifact, 'resolver-status.properties')));
  await assert.rejects(readFile(join(snapshot, 'example-1.1.0-SNAPSHOT.jar')));
  await assert.rejects(readFile(join(nestedSnapshot, 'example-SNAPSHOT-1.1.0-SNAPSHOT.jar')));
});
