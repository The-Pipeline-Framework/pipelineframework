import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {discoverCompatibilitySet} from '../scripts/lib/compatibility-discovery.mjs';
import {validateComponentsConfig} from '../scripts/lib/contracts.mjs';

const config = validateComponentsConfig(JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8')));
const contracts = config.components.contracts.repository;
const runtime = config.components.runtime.repository;
const sourceSha = 'a'.repeat(40);

function pull(repository, number, sha, ref = 'feature/release-closure', owner = 'The-Pipeline-Framework') {
  return {
    number,
    state: 'open',
    base: {ref: 'main', repo: {full_name: repository}},
    head: {ref, sha, repo: {owner: {login: owner}}}
  };
}

test('same-owner and same-branch component PRs form one deterministic compatibility set', () => {
  const result = discoverCompatibilitySet(config, [
    pull(runtime, 16, sourceSha),
    pull(contracts, 35, 'b'.repeat(40)),
    pull(config.components.blocks.repository, 9, 'c'.repeat(40), 'unrelated')
  ], {sourceRepository: runtime, sourcePullRequest: 16, sourceSha});

  assert.equal(result.coalesced, true);
  assert.match(result.setId, /^auto-[0-9a-f]{20}$/);
  assert.deepEqual(result.pullRequests, [
    'https://github.com/The-Pipeline-Framework/pipelineframework-contracts/pull/35',
    'https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/16'
  ]);
});

test('a candidate without a companion remains a singleton', () => {
  assert.deepEqual(
    discoverCompatibilitySet(config, [pull(runtime, 16, sourceSha)], {
      sourceRepository: runtime,
      sourcePullRequest: 16,
      sourceSha
    }),
    {coalesced: false, setId: null, pullRequests: ['https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/16']}
  );
});

test('candidate discovery rejects a stale source head', () => {
  assert.throws(() => discoverCompatibilitySet(config, [pull(runtime, 16, 'b'.repeat(40))], {
    sourceRepository: runtime,
    sourcePullRequest: 16,
    sourceSha
  }), /head moved/);
});

test('candidate discovery rejects ambiguous same-repository branch matches', () => {
  assert.throws(() => discoverCompatibilitySet(config, [
    pull(runtime, 16, sourceSha),
    pull(runtime, 17, 'b'.repeat(40)),
    pull(contracts, 35, 'c'.repeat(40))
  ], {sourceRepository: runtime, sourcePullRequest: 16, sourceSha}), /multiple open/);
});
