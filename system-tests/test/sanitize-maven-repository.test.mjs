import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {mkdir, mkdtemp, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {promisify} from 'node:util';

const execFileAsync = promisify(execFile);
const sanitizer = new URL('../scripts/sanitize-maven-repository.mjs', import.meta.url).pathname;

test('removes resolver provenance while preserving materialized Maven artifacts', async () => {
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

  const {stdout} = await execFileAsync(process.execPath, [sanitizer, '--repository', repository]);
  assert.equal(stdout, 'Removed 3 Maven resolver metadata file(s).\n');
  assert.equal(await readFile(join(artifact, 'example-1.0.0.jar'), 'utf8'), 'jar');
  assert.equal(await readFile(join(artifact, 'example-1.0.0.pom'), 'utf8'), 'pom');
  assert.equal(await readFile(join(artifact, 'example-1.0.0.jar.sha1'), 'utf8'), 'checksum');
  await assert.rejects(readFile(join(artifact, '_remote.repositories')));
  await assert.rejects(readFile(join(artifact, 'example-1.0.0.jar.lastUpdated')));
  await assert.rejects(readFile(join(artifact, 'resolver-status.properties')));
});
