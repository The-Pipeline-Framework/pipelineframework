import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {promisify} from 'node:util';

const execFileAsync = promisify(execFile);
const script = new URL('../scripts/select-set-suites.mjs', import.meta.url).pathname;
const packageScript = new URL('../scripts/package-native-host-workers.sh', import.meta.url).pathname;
const components = new URL('../components.yml', import.meta.url).pathname;
const policy = new URL('../policy.yml', import.meta.url).pathname;
const sha = 'a'.repeat(40);

async function select(requestedSuites, includeCsv = true, options = {}) {
  const root = await mkdtemp(join(tmpdir(), 'tpf-set-suites-'));
  const resolvedSet = join(root, 'resolved.json');
  const compatibilitySet = join(root, 'request.json');
  const output = join(root, 'matrix.json');
  const requested = options.components ?? (includeCsv ? ['csvPayments', 'ragTurnkey'] : ['examples', 'ragTurnkey']);
  const requests = requested.map(component => ({component}));
  try {
    const pins = Object.fromEntries(Object.keys(JSON.parse(await readFile(components, 'utf8')).components)
      .map(component => [component, {sha}]));
    await writeFile(resolvedSet, JSON.stringify({
      candidates: requested.map(component => ({component, suiteHints: []})),
      components: pins,
      testHarnesses: pins
    }));
    await writeFile(compatibilitySet, JSON.stringify({schemaVersion: 1, requests, requestedSuites}));
    await execFileAsync(process.execPath, [script, '--components', components, '--policy', policy,
      '--resolvedSet', resolvedSet, '--compatibilitySet', compatibilitySet, '--output', output,
      ...(options.nativeHostedWorker ? ['--nativeHostedWorker'] : [])]);
    return JSON.parse(await readFile(output, 'utf8'));
  } finally {
    await rm(root, {recursive: true, force: true});
  }
}

test('manual compatibility set can add exact-head CSV HA scale without changing default coverage', async () => {
  const ordinary = await select([]);
  assert.ok(!ordinary.include.some(({shard}) => shard === 'scale-native'));
  const scale = await select(['csv-ha-scale']);
  assert.deepEqual(scale.include.find(({shard}) => shard === 'scale-native').suites
    .map(({suite}) => suite), ['csv-ha-scale']);
  assert.ok(scale.include.flatMap(({suites}) => suites.map(({suite}) => suite)).includes('csv-smoke'));
});

test('CSV HA scale request is rejected without a CSV Payments candidate', async () => {
  await assert.rejects(select(['csv-ha-scale'], false), /extra CSV suites require a CSV Payments candidate/);
  await assert.rejects(select(['unknown-suite']), /unsupported extra suite/);
});

test('manual compatibility set can add the provider-reject lane for a CSV candidate', async () => {
  const matrix = await select(['csv-provider-reject']);
  assert.deepEqual(matrix.include.find(({shard}) => shard === 'csv-provider-reject').suites
    .map(({suite}) => suite), ['csv-provider-reject']);
  await assert.rejects(select(['csv-provider-reject'], false),
    /extra CSV suites require a CSV Payments candidate/);
});

test('the native hosted proof requires explicit opt-in and both owner candidates', async () => {
  const components = ['runtime', 'examples'];
  const ordinary = await select([], false, {components});
  assert.deepEqual(ordinary.include.find(({shard}) => shard === 'examples').suites
    .map(({suite}) => suite), ['examples-verify']);
  const hosted = await select([], false, {components, nativeHostedWorker: true});
  assert.deepEqual(hosted.include.find(({shard}) => shard === 'examples').suites
    .map(({suite}) => suite), ['examples-native-hosted-worker', 'examples-verify']);
  await assert.rejects(select([], false, {nativeHostedWorker: true}),
    /requires coordinated runtime and examples candidates/);
  const commonPolicy = JSON.parse(await readFile(policy, 'utf8'));
  assert.ok(!Object.hasOwn(commonPolicy.suites, 'examples-native-hosted-worker'),
    'Singleton and nightly policy must not select the archive-dependent suite');
});

test('native archive staging rejects an untested runtime descendant before packaging', async () => {
  const root = await mkdtemp(join(tmpdir(), 'tpf-native-package-pin-'));
  try {
    const git = (...args) => execFileAsync('git', ['-C', root, ...args]);
    await git('init', '--quiet');
    const commit = message => git('-c', 'user.name=Test', '-c', 'user.email=test@example.invalid',
      'commit', '--quiet', '--allow-empty', '-m', message);
    await commit('Tested runtime');
    const testedSha = (await git('rev-parse', 'HEAD')).stdout.trim();
    await commit('Untested descendant');
    const manifest = join(root, 'manifest.json');
    const resolved = join(root, 'resolved.json');
    const bytes = JSON.stringify({component: 'runtime', sourceSha: testedSha, testedSha});
    const digest = 'sha256:' + createHash('sha256').update(bytes).digest('hex');
    await writeFile(manifest, bytes);
    await writeFile(resolved, JSON.stringify({components: {runtime: {sha: testedSha, manifestDigest: digest}}}));
    const packageWorkers = () => execFileAsync('bash', [packageScript, root, resolved, manifest, join(root, 'out')]);
    await assert.rejects(packageWorkers(), /Runtime checkout differs from the exact tested Maven revision/);
    await writeFile(manifest, bytes + '\n');
    await assert.rejects(packageWorkers(), /Runtime Maven manifest differs from the selected compatibility set/);
  } finally {
    await rm(root, {recursive: true, force: true});
  }
});
