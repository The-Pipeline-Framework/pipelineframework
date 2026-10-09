import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';

test('existing Connector service integrations remain in the train without an invented live-account gate', async () => {
  const policy = JSON.parse(await readFile(new URL('../policy.yml', import.meta.url), 'utf8'));
  assert.deepEqual(policy.suites['connectors-verify'], {
    owner: 'connectors', entrypoint: 'verify', tier: 'pr', shard: 'ecosystem'
  });
  assert.equal(policy.fullTrain.filter(suite => suite === 'connectors-verify').length, 1);
  assert.equal(policy.suites['live-providers'], undefined);
  assert.ok(!JSON.stringify(policy).includes('live-providers'));
  assert.ok(policy.fullTrain.includes('cloud-deployments'), 'real cloud coverage must remain required');
  for (const component of ['contracts', 'runtime']) {
    assert.ok(policy.componentPolicy[component].required.includes('connectors-verify'));
  }
});
