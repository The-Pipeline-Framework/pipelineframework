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
  await writeFile(join(directory, 'pom.xml'), `<project><properties>
    <pipelineframework.contracts.version>26.10.1-SNAPSHOT</pipelineframework.contracts.version>
    <pipelineframework.bom.version>26.10.1-SNAPSHOT</pipelineframework.bom.version>
  </properties></project>`);
  await writeFile(join(directory, 'manifest.json'), JSON.stringify({schemaVersion: 1, suites: {verify: {command, timeoutMinutes: 1}}}));
  await writeFile(join(directory, 'resolved.json'), JSON.stringify({
    components: {
      contracts: {mavenVersion: '26.9.4-pr.7.abcdef123456'},
      runtime: {mavenVersion: '26.9.4-pr.8.fedcba654321'}
    },
    coordinationBom: {mavenVersion: '26.9.4-system-test.1234567890ab'}
  }));
  return {directory, capture};
}

function run({directory, capture}, versionProperties = {
  contracts: 'pipelineframework.contracts.version',
  coordinationBom: 'pipelineframework.bom.version'
}) {
  return spawnSync(process.execPath, [runner,
    '--manifest', join(directory, 'manifest.json'),
    '--entrypoint', 'verify',
    '--resolvedSet', join(directory, 'resolved.json'),
    '--versionProperties', JSON.stringify(versionProperties),
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
  const pom = await readFile(join(value.directory, 'pom.xml'), 'utf8');
  assert.match(pom, /<pipelineframework\.contracts\.version>26\.9\.4-pr\.7\.abcdef123456<\/pipelineframework\.contracts\.version>/);
  assert.match(pom, /<pipelineframework\.bom\.version>26\.9\.4-system-test\.1234567890ab<\/pipelineframework\.bom\.version>/);
});

test('wrapper suite commands receive exact Maven arguments through MAVEN_ARGS', async () => {
  const value = await fixture(['./wrapper.sh']);
  const result = run(value);
  assert.equal(result.status, 0, result.stderr);
  const captured = await readFile(value.capture, 'utf8');
  assert.match(captured, /-Dpipelineframework\.contracts\.version=26\.9\.4-pr\.7\.abcdef123456/);
  assert.match(captured, /-Dpipelineframework\.bom\.version=26\.9\.4-system-test\.1234567890ab/);
  assert.match(captured, new RegExp(`-Dmaven\\.repo\\.local=${value.directory}/m2`));
  const pom = await readFile(join(value.directory, 'pom.xml'), 'utf8');
  assert.match(pom, /<pipelineframework\.contracts\.version>26\.9\.4-pr\.7\.abcdef123456<\/pipelineframework\.contracts\.version>/);
  assert.match(pom, /<pipelineframework\.bom\.version>26\.9\.4-system-test\.1234567890ab<\/pipelineframework\.bom\.version>/);
});

test('CSV suites select dependencies through one tested BOM and separately pin the Maven build plugin', async () => {
  const config = JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8'));
  const value = await fixture(['./wrapper.sh']);
  await writeFile(join(value.directory, 'pom.xml'), `<project><properties>
    <pipelineframework.bom.version>26.10.1-SNAPSHOT</pipelineframework.bom.version>
    <tpf.release.maven-plugin.version>26.10.1-SNAPSHOT</tpf.release.maven-plugin.version>
  </properties></project>`);
  const result = run(value, config.components.csvPayments.consumerVersionProperties);
  assert.equal(result.status, 0, result.stderr);
  const captured = await readFile(value.capture, 'utf8');
  assert.match(captured, /-Dpipelineframework\.bom\.version=26\.9\.4-system-test\.1234567890ab/);
  assert.match(captured, /-Dtpf\.release\.maven-plugin\.version=26\.9\.4-pr\.8\.fedcba654321/);
  assert.doesNotMatch(captured, /-Dpipelineframework\.(?:contracts|compiler|connectors|runtime)\.version=/);
  const pom = await readFile(join(value.directory, 'pom.xml'), 'utf8');
  assert.match(pom, /<pipelineframework\.bom\.version>26\.9\.4-system-test\.1234567890ab<\/pipelineframework\.bom\.version>/);
  assert.match(pom, /<tpf\.release\.maven-plugin\.version>26\.9\.4-pr\.8\.fedcba654321<\/tpf\.release\.maven-plugin\.version>/);
});
