import assert from 'node:assert/strict';
import {mkdtemp, mkdir, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import test from 'node:test';

const runner = new URL('../scripts/run-shard.mjs', import.meta.url).pathname;
const preparer = new URL('../scripts/prepare-shard-sources.mjs', import.meta.url).pathname;

test('product shard runs owner suites sequentially against one resolved set and Maven repository', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-run-shard-'));
  const coordination = join(directory, 'coordination');
  const owners = join(directory, 'owners');
  const owner = join(owners, 'examples');
  const capture = join(directory, 'capture.txt');
  await mkdir(join(coordination, '.github'), {recursive: true});
  await mkdir(owner, {recursive: true});
  const command = (label) => ['bash', '-c', `printf '${label}:%s\\n' "$MAVEN_ARGS" >> "$TPF_CAPTURE"`];
  await writeFile(join(coordination, '.github', 'tpf-system-tests.json'), JSON.stringify({
    schemaVersion: 1,
    suites: {compatibility: {command: command('coordination'), timeoutMinutes: 1}}
  }));
  await writeFile(join(owner, 'suite.json'), JSON.stringify({
    schemaVersion: 1,
    suites: {verify: {command: command('examples'), timeoutMinutes: 1}}
  }));
  await writeFile(join(directory, 'resolved.json'), JSON.stringify({components: {}, coordinationBom: {mavenVersion: '26.9.4-system-test.1234567890ab'}}));
  await writeFile(join(directory, 'shard.json'), JSON.stringify({
    shard: 'consumers',
    suites: [
      {suite: 'coordination-compatibility', owner: 'coordination', manifest: null, entrypoint: 'compatibility', versionProperties: {}},
      {suite: 'examples-verify', owner: 'examples', manifest: 'suite.json', entrypoint: 'verify', versionProperties: {}}
    ]
  }));

  const result = spawnSync(process.execPath, [runner,
    '--shard', join(directory, 'shard.json'),
    '--coordination', coordination,
    '--owners', owners,
    '--resolvedSet', join(directory, 'resolved.json'),
    '--mavenRepository', join(directory, 'm2')
  ], {env: {...process.env, TPF_CAPTURE: capture}, encoding: 'utf8'});

  assert.equal(result.status, 0, result.stderr);
  const captured = await readFile(capture, 'utf8');
  assert.deepEqual(captured.trim().split('\n').map((line) => line.split(':', 1)[0]), ['coordination', 'examples']);
});

test('shard source preparation rejects conflicting immutable pins before checkout', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-prepare-shard-'));
  const shard = join(directory, 'shard.json');
  await writeFile(shard, JSON.stringify({suites: [
    {owner: 'examples', repository: 'The-Pipeline-Framework/pipelineframework-examples', sha: 'a'.repeat(40)},
    {owner: 'examples', repository: 'The-Pipeline-Framework/pipelineframework-examples', sha: 'b'.repeat(40)}
  ]}));
  const result = spawnSync(process.execPath, [preparer, '--shard', shard, '--output', join(directory, 'owners')], {encoding: 'utf8'});
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /conflicting pins/);
});

test('product shard runs remaining suites after one suite fails', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-run-shard-failure-'));
  const coordination = join(directory, 'coordination');
  const owner = join(directory, 'owners', 'examples');
  const capture = join(directory, 'capture.txt');
  await mkdir(coordination, {recursive: true});
  await mkdir(owner, {recursive: true});
  await writeFile(join(owner, 'suite.json'), JSON.stringify({schemaVersion: 1, suites: {
    fail: {command: ['bash', '-c', 'exit 7'], timeoutMinutes: 1},
    after: {command: ['bash', '-c', 'printf after > "$TPF_CAPTURE"'], timeoutMinutes: 1}
  }}));
  await writeFile(join(directory, 'resolved.json'), JSON.stringify({components: {}}));
  await writeFile(join(directory, 'shard.json'), JSON.stringify({suites: [
    {suite: 'first', owner: 'examples', manifest: 'suite.json', entrypoint: 'fail', versionProperties: {}},
    {suite: 'second', owner: 'examples', manifest: 'suite.json', entrypoint: 'after', versionProperties: {}}
  ]}));

  const result = spawnSync(process.execPath, [runner,
    '--shard', join(directory, 'shard.json'),
    '--coordination', coordination,
    '--owners', join(directory, 'owners'),
    '--resolvedSet', join(directory, 'resolved.json'),
    '--mavenRepository', join(directory, 'm2')
  ], {env: {...process.env, TPF_CAPTURE: capture}, encoding: 'utf8'});

  assert.notEqual(result.status, 0);
  assert.equal(await readFile(capture, 'utf8'), 'after');
  assert.match(result.stderr, /product shard failed: first/);
});
