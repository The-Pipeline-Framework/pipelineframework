import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {mkdtemp, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {promisify} from 'node:util';

const execFileAsync = promisify(execFile);
const directory = new URL('../', import.meta.url);
const script = new URL('../scripts/select-set-suites.mjs', import.meta.url).pathname;
const components = new URL('../components.yml', import.meta.url).pathname;
const policy = new URL('../policy.yml', import.meta.url).pathname;
const sha = 'a'.repeat(40);

async function select(requestedSuites, includeCsv = true) {
  const root = await mkdtemp(join(tmpdir(), 'tpf-set-suites-'));
  const resolvedSet = join(root, 'resolved.json');
  const compatibilitySet = join(root, 'request.json');
  const output = join(root, 'matrix.json');
  const requests = includeCsv
    ? [{component: 'csvPayments'}, {component: 'ragTurnkey'}]
    : [{component: 'examples'}, {component: 'ragTurnkey'}];
  await writeFile(resolvedSet, JSON.stringify({
    candidates: requests.map(({component}) => ({component, suiteHints: []})),
    testHarnesses: Object.fromEntries(requests.map(({component}) => [component, {sha}]))
  }));
  await writeFile(compatibilitySet, JSON.stringify({schemaVersion: 1, requests, requestedSuites}));
  await execFileAsync(process.execPath, [script, '--components', components, '--policy', policy,
    '--resolvedSet', resolvedSet, '--compatibilitySet', compatibilitySet, '--output', output]);
  return JSON.parse(await readFile(output, 'utf8'));
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
