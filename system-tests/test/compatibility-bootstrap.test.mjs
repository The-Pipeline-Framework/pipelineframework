import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {candidateFromOutput, expectedCandidateVersion, orderedMavenTargets} from '../scripts/lib/compatibility-bootstrap.mjs';

const config = JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8'));
const sha = 'abcdef1234567890abcdef1234567890abcdef12';
const target = (component, pullRequestNumber) => ({component, pullRequestNumber, sourceSha: sha, baseSha: sha, testedSha: sha});

test('compatibility Maven candidates build in dependency order', () => {
  const targets = new Map([
    ['connectors', target('connectors', 17)],
    ['runtime', target('runtime', 7)],
    ['compiler', target('compiler', 7)],
    ['contracts', target('contracts', 28)],
    ['csvPayments', target('csvPayments', 5)]
  ]);
  assert.deepEqual(
    orderedMavenTargets(config, targets).map(({component}) => component),
    ['contracts', 'compiler', 'runtime', 'connectors']
  );
});

test('compatibility candidate identity is derived from the exact PR head', () => {
  const resolvedSet = {components: {contracts: {mavenVersion: '26.9.4-main.111111111111'}}};
  assert.equal(expectedCandidateVersion(resolvedSet, target('runtime', 7)), '26.9.4-pr.7.abcdef123456');
  assert.equal(candidateFromOutput('candidate=26.9.4-pr.7.abcdef123456\n'), '26.9.4-pr.7.abcdef123456');
  assert.throws(() => candidateFromOutput('candidate=26.9.4-SNAPSHOT\n'), /exactly one valid version/);
});

test('compatibility candidate dependency cycles are rejected', () => {
  const cyclic = structuredClone(config);
  cyclic.components.contracts.consumerVersionProperties = {runtime: 'tpf.runtime.version'};
  const targets = new Map([
    ['contracts', target('contracts', 28)],
    ['runtime', target('runtime', 7)]
  ]);
  assert.throws(() => orderedMavenTargets(cyclic, targets), /dependency cycle/);
});
