import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {candidateBuildArguments, candidateFromOutput, expectedCandidateVersion, orderedMavenTargets} from '../scripts/lib/compatibility-bootstrap.mjs';

const config = JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8'));
const workflow = await readFile(new URL('../../.github/workflows/system-test-compatibility-set.yml', import.meta.url), 'utf8');
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

test('compatibility bootstrap installs candidates without repeating owner test suites', () => {
  const arguments_ = candidateBuildArguments('/tmp/tpf-m2', ['-Dtpf.contracts.version=26.9.4-pr.28.abcdef123456']);
  assert.equal(arguments_[0], '-B');
  assert.ok(arguments_.includes('install'));
  assert.ok(!arguments_.includes('clean'));
  assert.ok(arguments_.includes('-DskipTests=true'));
  assert.ok(arguments_.includes('-DskipITs=true'));
  assert.ok(arguments_.includes('-DskipUnitTests=true'));
  assert.ok(arguments_.includes('-Dmaven.repo.local=/tmp/tpf-m2'));
});

test('compatibility baseline is portable and cached by immutable digest before bootstrap', () => {
  const cache = workflow.indexOf('key: tpf-system-test-baseline-${{ steps.baseline.outputs.cache_key }}');
  const sanitize = workflow.indexOf('node system-tests/scripts/sanitize-maven-repository.mjs');
  const archive = workflow.indexOf('tar -C "$local_repo" -czf baseline/tpf-system-test-m2.tar.gz .');
  assert.ok(cache >= 0, 'baseline cache key is missing');
  assert.ok(sanitize >= 0, 'baseline sanitation is missing');
  assert.ok(archive > sanitize, 'baseline must be sanitized before it crosses the credential boundary');
});
