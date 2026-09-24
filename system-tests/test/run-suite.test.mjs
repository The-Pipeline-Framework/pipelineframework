import test from 'node:test';
import assert from 'node:assert/strict';
import {chmod, mkdtemp, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';

const runner = new URL('../scripts/run-suite.mjs', import.meta.url).pathname;

async function fixture(command) {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-run-suite-'));
  const capture = join(directory, 'capture.txt');
  await writeFile(join(directory, 'mvnw'), '#!/usr/bin/env bash\nprintf \'%s\\n\' "$@" > "$TPF_CAPTURE"\nprintf \'MAVEN_ARGS=%s\\n\' "${MAVEN_ARGS:-}" >> "$TPF_CAPTURE"\n');
  await chmod(join(directory, 'mvnw'), 0o755);
  await writeFile(join(directory, 'wrapper.sh'), '#!/usr/bin/env bash\nprintf \'%s\\n\' "${MAVEN_ARGS:-}" > "$TPF_CAPTURE"\n');
  await chmod(join(directory, 'wrapper.sh'), 0o755);
  await writeFile(join(directory, 'manifest.json'), JSON.stringify({schemaVersion: 1, suites: {verify: {command, timeoutMinutes: 1}}}));
  await writeFile(join(directory, 'resolved.json'), JSON.stringify({
    components: {contracts: {mavenVersion: '26.9.4-pr.7.abcdef123456'}},
    coordinationBom: {mavenVersion: '26.9.4-system-test.1234567890ab'}
  }));
  return {directory, capture};
}

function run({directory, capture}) {
  return spawnSync(process.execPath, [runner,
    '--manifest', join(directory, 'manifest.json'),
    '--entrypoint', 'verify',
    '--resolvedSet', join(directory, 'resolved.json'),
    '--versionProperties', JSON.stringify({
      contracts: 'pipelineframework.contracts.version',
      coordinationBom: 'pipelineframework.bom.version'
    }),
    '--cwd', directory,
    '--mavenRepository', join(directory, 'm2')
  ], {env: {...process.env, TPF_CAPTURE: capture}, encoding: 'utf8'});
}

test('direct Maven suite commands receive exact versions and the isolated repository as arguments', async () => {
  const value = await fixture(['./mvnw', '-B', 'verify']);
  const result = run(value);
  assert.equal(result.status, 0, result.stderr);
  const captured = await readFile(value.capture, 'utf8');
  assert.match(captured, /-Dpipelineframework\.contracts\.version=26\.9\.4-pr\.7\.abcdef123456/);
  assert.match(captured, /-Dpipelineframework\.bom\.version=26\.9\.4-system-test\.1234567890ab/);
  assert.match(captured, new RegExp(`-Dmaven\\.repo\\.local=${value.directory}/m2`));
});

test('wrapper suite commands receive exact Maven arguments through MAVEN_ARGS', async () => {
  const value = await fixture(['./wrapper.sh']);
  const result = run(value);
  assert.equal(result.status, 0, result.stderr);
  const captured = await readFile(value.capture, 'utf8');
  assert.match(captured, /-Dpipelineframework\.contracts\.version=26\.9\.4-pr\.7\.abcdef123456/);
  assert.match(captured, /-Dpipelineframework\.bom\.version=26\.9\.4-system-test\.1234567890ab/);
  assert.match(captured, new RegExp(`-Dmaven\\.repo\\.local=${value.directory}/m2`));
});
