import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {augmentCompatibilityTargets, candidateBuildArguments, candidateFromOutput, expectedCandidateVersion, orderedMavenTargets, pinCandidateDependencyProperties} from '../scripts/lib/compatibility-bootstrap.mjs';

const config = JSON.parse(await readFile(new URL('../components.yml', import.meta.url), 'utf8'));
const workflow = await readFile(new URL('../../.github/workflows/system-test-compatibility-set.yml', import.meta.url), 'utf8');
const bootstrap = await readFile(new URL('../scripts/bootstrap-compatibility-set.mjs', import.meta.url), 'utf8');
const sha = 'abcdef1234567890abcdef1234567890abcdef12';
const target = (component, pullRequestNumber) => ({component, pullRequestNumber, sourceSha: sha, baseSha: sha, baseRef: 'main', merged: false});
const currentHead = (value) => ({baseRef: 'main', sha: value});

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
  assert.equal(expectedCandidateVersion(resolvedSet, {...target('runtime', null), merged: true}), '26.9.4-main.abcdef123456');
  assert.equal(candidateFromOutput('candidate=26.9.4-pr.7.abcdef123456\n'), '26.9.4-pr.7.abcdef123456');
  assert.equal(candidateFromOutput('candidate=26.9.4-main.abcdef123456\n'), '26.9.4-main.abcdef123456');
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
  assert.ok(arguments_.includes('-Dinvoker.skip=true'));
  assert.ok(arguments_.includes('-Dmaven.repo.local=/tmp/tpf-m2'));
});

test('candidate POMs persist exact predecessor versions for downstream consumers', () => {
  const source = '<properties>\n<tpf.contracts.version>26.9.4-SNAPSHOT</tpf.contracts.version>\n</properties>\n';
  assert.equal(
    pinCandidateDependencyProperties(source, {'tpf.contracts.version': '26.9.4-pr.35.abcdef123456'}),
    '<properties>\n<tpf.contracts.version>26.9.4-pr.35.abcdef123456</tpf.contracts.version>\n</properties>\n'
  );
  assert.throws(
    () => pinCandidateDependencyProperties(source, {'missing.version': '26.9.4-main.abcdef123456'}),
    /must declare missing\.version exactly once/
  );
  assert.throws(
    () => pinCandidateDependencyProperties(source, {'tpf.contracts.version': '26.9.4-SNAPSHOT'}),
    /must use an immutable version/
  );
});

test('compatibility targets include changed main and the complete downstream Maven closure', () => {
  const baselineSha = '1'.repeat(40);
  const connectorSha = '2'.repeat(40);
  const heads = Object.fromEntries(Object.keys(config.components).map((component) => [component, currentHead(baselineSha)]));
  heads.connectors = currentHead(connectorSha);
  const baseline = {
    components: Object.fromEntries(Object.entries(config.components).filter(([, value]) => value.kind === 'maven')
      .map(([component, value]) => [component, {repository: value.repository, sha: baselineSha}])),
    testHarnesses: Object.fromEntries(Object.entries(config.components).filter(([, value]) => value.kind === 'source')
      .map(([component, value]) => [component, {repository: value.repository, sha: baselineSha}]))
  };
  const augmented = augmentCompatibilityTargets(config, baseline, {schemaVersion: 1, targets: [target('runtime', 7)]}, heads);
  assert.deepEqual(
    augmented.targets.filter(({component}) => config.components[component].kind === 'maven').map(({component}) => component),
    ['blocks', 'connectors', 'expansions', 'runtime']
  );
  assert.equal(augmented.targets.find(({component}) => component === 'connectors').sourceSha, connectorSha);
  assert.equal(augmented.targets.find(({component}) => component === 'blocks').pullRequestNumber, null);
});

test('compatibility baseline is portable and cached by immutable digest before bootstrap', () => {
  const cache = workflow.indexOf('key: tpf-system-test-baseline-${{ steps.baseline.outputs.cache_key }}');
  const sanitize = workflow.indexOf('node system-tests/scripts/sanitize-maven-repository.mjs');
  const archive = workflow.indexOf('tar -C "$local_repo" -czf baseline/tpf-system-test-m2.tar.gz .');
  assert.ok(cache >= 0, 'baseline cache key is missing');
  assert.ok(sanitize >= 0, 'baseline sanitation is missing');
  assert.ok(archive > sanitize, 'baseline must be sanitized before it crosses the credential boundary');
  assert.match(workflow, /Resolve immutable baseline metadata[\s\S]*?PACKAGE_TOKEN: \$\{\{ github\.token \}\}[\s\S]*?write-maven-settings\.mjs/);
  assert.match(workflow, /augment-compatibility-targets\.mjs[\s\S]*?--output baseline\/bootstrap-targets\.json/);
  assert.match(workflow, /incomplete_args[\s\S]*?--allowMissingCoordinatesFor[\s\S]*?baseline\/bootstrap-targets\.json/);
  assert.match(workflow, /bootstrap-compatibility-set\.mjs[\s\S]*?--targets baseline\/bootstrap-targets\.json/);
});

test('compatibility targets are merged locally from the exact current base and PR head', () => {
  assert.match(workflow, /permission-contents: read/);
  assert.match(workflow, /repos\/\$repository\/commits\/\$base_ref/);
  assert.match(workflow, /baseRef: \$baseRef/);
  assert.doesNotMatch(workflow, /\.mergeable/);
  assert.match(bootstrap, /refs\/pull\/\$\{target\.pullRequestNumber\}\/head/);
  assert.match(bootstrap, /'commit-tree', tree, '-p', target\.baseSha, '-p', target\.sourceSha/);
  assert.match(bootstrap, /GIT_COMMITTER_DATE: '2000-01-01T00:00:00Z'/);
});
