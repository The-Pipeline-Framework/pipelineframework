import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { promisify } from 'node:util';
import {
  canonicalJson,
  nextBaseline,
  overlayBaseline,
  parseCandidateVersion,
  readJson,
  selectPostMergeSuites,
  selectSuites,
  sha256File,
  shardMatrix,
  suiteMatrix,
  validateBaseline,
  validateCandidateEvent,
  validateCandidateManifest,
  validateComponentsConfig,
  validateEventAgainstManifest,
  validateGitHubProvenance,
  validatePolicy
} from '../scripts/lib/contracts.mjs';

const execFileAsync = promisify(execFile);

const root = new URL('../', import.meta.url);
const config = validateComponentsConfig(await readJson(new URL('components.yml', root)));
const policy = validatePolicy(await readJson(new URL('policy.yml', root)), config);
const sha = 'abcdef1234567890abcdef1234567890abcdef12';
const otherSha = '1234567890abcdef1234567890abcdef12345678';
const digest = `sha256:${'a'.repeat(64)}`;

function manifest(overrides = {}) {
  const candidateVersion = '26.9.4-pr.42.abcdef123456';
  return {
    schemaVersion: 1,
    repository: 'The-Pipeline-Framework/pipelineframework-blocks',
    component: 'blocks',
    sourceSha: sha,
    pullRequestNumber: 42,
    candidateVersion,
    provenance: {
      build: {
        repository: 'The-Pipeline-Framework/pipelineframework-blocks',
        runId: 122,
        runAttempt: 1,
        workflowPath: '.github/workflows/tpf-candidate-build.yml',
        event: 'pull_request'
      },
      publication: {
        repository: 'The-Pipeline-Framework/pipelineframework-blocks',
        runId: 123,
        runAttempt: 1,
        workflowPath: '.github/workflows/tpf-candidate-publish.yml',
        event: 'workflow_run'
      }
    },
    mavenArtifacts: config.components.blocks.allowedCoordinates.map((coordinate) => {
      const [groupId, artifactId, packaging] = coordinate.split(':');
      const extension = packaging === 'maven-plugin' ? 'jar' : packaging;
      const primary = `${artifactId}-${candidateVersion}.${extension}`;
      const pom = `${artifactId}-${candidateVersion}.pom`;
      return {
        groupId,
        artifactId,
        version: candidateVersion,
        packaging,
        files: [...new Set([primary, pom])].map((name) => ({name, sha256: 'b'.repeat(64)}))
      };
    }),
    images: [],
    ...overrides
  };
}

function event(overrides = {}) {
  return {
    schema_version: 1,
    source_repository: 'The-Pipeline-Framework/pipelineframework-blocks',
    source_sha: sha,
    pull_request_number: 42,
    component: 'blocks',
    candidate_version: '26.9.4-pr.42.abcdef123456',
    publication_run_id: 123,
    manifest_sha256: 'c'.repeat(64),
    compatibility_set_id: null,
    ...overrides
  };
}

function baseline() {
  const components = Object.fromEntries(
    ['contracts', 'compiler', 'runtime', 'connectors', 'blocks', 'expansions'].map((name, index) => [name, {
      repository: config.components[name].repository,
      sha: index % 2 === 0 ? sha : otherSha,
      mavenVersion: '26.9.3',
      manifestDigest: digest
    }])
  );
  const testHarnesses = Object.fromEntries(
    ['examples', 'references', 'csvPayments', 'ragTurnkey'].map((name, index) => [name, {
      repository: config.components[name].repository,
      sha: index % 2 === 0 ? otherSha : sha
    }])
  );
  return {schemaVersion: 1, revision: 7, generatedAt: '2026-09-23T00:00:00.000Z', components, testHarnesses, images: {}};
}

test('configuration covers exactly the ten extracted repositories', () => {
  assert.equal(Object.keys(config.components).length, 10);
  assert.equal(new Set(Object.values(config.components).map((component) => component.repository)).size, 10);
  assert.deepEqual(config.components.runtime.allowedCoordinates, [
    'org.pipelineframework:cache-plugin:jar',
    'org.pipelineframework:persistence-plugin:jar',
    'org.pipelineframework:pipelineframework-deployment:jar',
    'org.pipelineframework:pipelineframework-runtime-parent:pom',
    'org.pipelineframework:pipelineframework-runtime-spring:jar',
    'org.pipelineframework:pipelineframework:jar',
    'org.pipelineframework:repository-plugin:jar'
  ]);
  assert.deepEqual(
    config.components.connectors.allowedCoordinates.filter((coordinate) => coordinate.includes(':decision-query-')),
    [
      'org.pipelineframework:decision-query-connector:jar',
      'org.pipelineframework:decision-query-jev-connector:jar'
    ]
  );
});

test('candidate identity binds PR number and full source SHA', () => {
  assert.deepEqual(parseCandidateVersion('26.9.4-pr.42.abcdef123456'), {
    baseVersion: '26.9.4', kind: 'pr', pullRequestNumber: 42, shortSha: 'abcdef123456'
  });
  assert.equal(validateCandidateEvent(event(), config).component, 'blocks');
  assert.throws(() => validateCandidateEvent(event({source_sha: otherSha}), config), /short SHA/);
  assert.throws(() => validateCandidateEvent(event({candidate_version: '26.9.4-SNAPSHOT'}), config), /does not match/);
});

test('candidate event rejects unknown repositories and extra properties', () => {
  assert.throws(() => validateCandidateEvent(event({source_repository: 'attacker/repository'}), config), /not an allowed/);
  assert.throws(() => validateCandidateEvent({...event(), suite_hints: []}, config), /unsupported properties/);
});

test('status target survives a malformed event only for an allowlisted repository and SHA', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-status-target-'));
  const path = join(directory, 'event.json');
  await writeFile(path, `${JSON.stringify(event({candidate_version: 'not-a-candidate'}))}\n`);
  const script = new URL('../scripts/extract-status-target.mjs', import.meta.url);
  const components = new URL('../components.yml', import.meta.url);
  const {stdout} = await execFileAsync(process.execPath, [script.pathname, '--components', components.pathname, '--event', path]);
  assert.deepEqual(JSON.parse(stdout), {
    repository: event().source_repository,
    repository_name: 'pipelineframework-blocks',
    source_sha: sha
  });

  await writeFile(path, `${JSON.stringify(event({source_repository: 'attacker/repository'}))}\n`);
  await assert.rejects(
    execFileAsync(process.execPath, [script.pathname, '--components', components.pathname, '--event', path]),
    /not an allowed repository/
  );
});

test('candidate manifest contains only owned immutable Maven coordinates', () => {
  assert.equal(validateCandidateManifest(manifest(), config).mavenArtifacts.length, config.components.blocks.allowedCoordinates.length);
  const foreign = manifest();
  foreign.mavenArtifacts[0].groupId = 'com.example';
  assert.throws(() => validateCandidateManifest(foreign, config), /not owned/);
  const floating = manifest();
  floating.mavenArtifacts[0].version = '26.9.4-SNAPSHOT';
  assert.throws(() => validateCandidateManifest(floating, config), /must equal candidateVersion/);
});

test('source-only candidate manifests carry identity and provenance without Maven artifacts', () => {
  const sourceManifest = manifest({
    repository: config.components.examples.repository,
    component: 'examples',
    mavenArtifacts: []
  });
  sourceManifest.provenance.build.repository = sourceManifest.repository;
  sourceManifest.provenance.publication.repository = sourceManifest.repository;
  assert.equal(validateCandidateManifest(sourceManifest, config).component, 'examples');
  sourceManifest.mavenArtifacts = manifest().mavenArtifacts;
  assert.throws(() => validateCandidateManifest(sourceManifest, config), /source-only candidate manifest/);
});

test('raw manifest checksum and dispatch values must agree', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-contracts-'));
  const path = join(directory, 'candidate.json');
  await writeFile(path, `${JSON.stringify(manifest(), null, 2)}\n`);
  const checksum = await sha256File(path);
  const matching = event({manifest_sha256: checksum});
  validateEventAgainstManifest(matching, manifest(), checksum);
  assert.throws(() => validateEventAgainstManifest(event(), manifest(), checksum), /checksum/);
});

test('GitHub provenance binds the trusted build and publisher runs', () => {
  const sourceRepository = {full_name: event().source_repository, default_branch: 'main'};
  const publicationRun = {
    id: 123,
    run_attempt: 1,
    status: 'completed',
    conclusion: 'success',
    path: '.github/workflows/tpf-candidate-publish.yml',
    event: 'workflow_run',
    head_branch: 'main',
    repository: {full_name: event().source_repository}
  };
  const buildRun = {
    id: 122,
    run_attempt: 1,
    status: 'completed',
    conclusion: 'success',
    path: '.github/workflows/tpf-candidate-build.yml',
    event: 'pull_request',
    repository: {full_name: event().source_repository},
    pull_requests: [{number: 42, head: {sha}}]
  };
  const pullRequest = {
    number: 42,
    base: {repo: {full_name: event().source_repository}},
    head: {sha, repo: {full_name: 'contributor/pipelineframework-blocks'}}
  };
  validateGitHubProvenance(event(), manifest(), sourceRepository, publicationRun, buildRun, pullRequest);
  assert.throws(() => validateGitHubProvenance(event(), manifest(), sourceRepository, publicationRun, buildRun, {...pullRequest, head: {...pullRequest.head, sha: otherSha}}), /stale/);
  assert.throws(() => validateGitHubProvenance(event(), manifest(), sourceRepository, {...publicationRun, path: '.github/workflows/other.yml'}, buildRun, pullRequest), /workflow path/);
  assert.throws(() => validateGitHubProvenance(event(), manifest(), sourceRepository, publicationRun, {...buildRun, pull_requests: []}, pullRequest), /not associated/);
});

test('main provenance binds the candidate SHA to the default-branch build, not the publisher SHA', () => {
  const candidateManifest = manifest({
    pullRequestNumber: null,
    candidateVersion: '26.9.4-main.abcdef123456'
  });
  candidateManifest.provenance.build.event = 'push';
  const candidateEvent = event({
    pull_request_number: null,
    candidate_version: '26.9.4-main.abcdef123456'
  });
  const sourceRepository = {full_name: candidateEvent.source_repository, default_branch: 'main'};
  const publicationRun = {
    id: 123,
    run_attempt: 1,
    head_sha: otherSha,
    head_branch: 'main',
    status: 'completed',
    conclusion: 'success',
    path: '.github/workflows/tpf-candidate-publish.yml',
    event: 'workflow_run',
    repository: {full_name: candidateEvent.source_repository}
  };
  const buildRun = {
    id: 122,
    run_attempt: 1,
    head_sha: sha,
    head_branch: 'main',
    status: 'completed',
    conclusion: 'success',
    path: '.github/workflows/tpf-candidate-build.yml',
    event: 'push',
    repository: {full_name: candidateEvent.source_repository}
  };
  validateGitHubProvenance(candidateEvent, candidateManifest, sourceRepository, publicationRun, buildRun, null);
  assert.throws(
    () => validateGitHubProvenance(candidateEvent, candidateManifest, sourceRepository, publicationRun, {...buildRun, head_sha: otherSha}, null),
    /build head SHA/
  );
  assert.throws(
    () => validateGitHubProvenance(candidateEvent, candidateManifest, {...sourceRepository, default_branch: 'trunk'}, publicationRun, buildRun, null),
    /publisher did not run/
  );
});

test('baseline forbids floating Maven versions and container tags', () => {
  assert.equal(validateBaseline(baseline(), config).revision, 7);
  const floating = baseline();
  floating.components.runtime.mavenVersion = '26.9.4-SNAPSHOT';
  assert.throws(() => validateBaseline(floating, config), /floating/);
  const taggedImage = baseline();
  taggedImage.images['ghcr.io/the-pipeline-framework/csv'] = 'latest';
  assert.throws(() => validateBaseline(taggedImage, config), /immutable sha256 digest/);
});

test('candidate overlays are deterministic and component-unique', () => {
  const first = overlayBaseline(baseline(), digest, [manifest()], ['c'.repeat(64)], config);
  const second = overlayBaseline(baseline(), digest, [manifest()], ['c'.repeat(64)], config);
  assert.equal(canonicalJson(first), canonicalJson(second));
  assert.equal(first.components.blocks.mavenVersion, manifest().candidateVersion);
  const hinted = overlayBaseline(baseline(), digest, [manifest({suiteHints: ['coordination-compatibility']})], ['c'.repeat(64)], config);
  assert.deepEqual(hinted.candidates[0].suiteHints, ['coordination-compatibility']);
  assert.throws(() => validateCandidateManifest(manifest({suiteHints: ['examples-verify', 'examples-verify']}), config), /contains duplicates/);
  assert.throws(() => overlayBaseline(baseline(), digest, [manifest(), manifest()], ['c'.repeat(64), 'd'.repeat(64)], config), /more than one blocks/);
});

test('publisher hints may widen but cannot reduce central coverage', () => {
  const base = selectSuites('blocks', policy);
  const widened = selectSuites('blocks', policy, [], ['coordination-compatibility']);
  assert.deepEqual(base, ['examples-verify', 'expansions-resolution', 'rag-system', 'references-core']);
  assert.deepEqual(widened, ['coordination-compatibility', ...base]);
});

test('non-semantic path rules suppress product tests while mixed changes fall back to full coverage', () => {
  assert.deepEqual(selectSuites('runtime', policy, ['README.md', 'docs/architecture/runtime.md']), []);
  assert.deepEqual(
    selectSuites('runtime', policy, ['README.md', 'runtime/src/main/java/Runtime.java']),
    policy.componentPolicy.runtime.required.slice().sort()
  );
  assert.deepEqual(selectPostMergeSuites('runtime', policy), ['csv-ha']);
});

test('suite matrix pins owner source SHAs from the resolved set', () => {
  const resolved = overlayBaseline(baseline(), digest, [manifest()], ['c'.repeat(64)], config);
  const [suite] = suiteMatrix(['expansions-resolution'], policy, resolved, config);
  assert.equal(suite.repository, config.components.expansions.repository);
  assert.equal(suite.sha, resolved.components.expansions.sha);
  assert.deepEqual(suite.versionProperties, config.components.expansions.consumerVersionProperties);
});

test('product matrix groups suites into a bounded set of coarse shards', () => {
  const resolved = overlayBaseline(baseline(), digest, [manifest()], ['c'.repeat(64)], config);
  const selected = selectSuites('blocks', policy);
  const shards = shardMatrix(selected, policy, resolved, config);
  assert.deepEqual(shards.map(({shard}) => shard), ['applications', 'consumers', 'ecosystem']);
  assert.deepEqual(shards.flatMap(({suites}) => suites.map(({suite}) => suite)).sort(), selected);
  assert.ok(shards.every(({suites}) => suites.length > 0));
});

test('promotion increments the baseline and rejects a stale tested revision', () => {
  const current = baseline();
  const resolved = overlayBaseline(current, digest, [manifest()], ['c'.repeat(64)], config);
  const promoted = nextBaseline(current, resolved, '2026-09-23T01:00:00.000Z');
  assert.equal(promoted.revision, 8);
  assert.equal(promoted.components.blocks.mavenVersion, manifest().candidateVersion);
  assert.throws(() => nextBaseline({...current, revision: 8}, resolved, '2026-09-23T01:00:00.000Z'), /not tested against/);
});

test('checked-in schemas are valid JSON and do not allow candidate-event extras', async () => {
  const schema = JSON.parse(await readFile(new URL('schemas/candidate-event.schema.json', root), 'utf8'));
  assert.equal(schema.additionalProperties, false);
  assert.equal(schema.required.length, 9);
  const candidateSchema = JSON.parse(await readFile(new URL('schemas/candidate-manifest.schema.json', root), 'utf8'));
  assert.equal(candidateSchema.additionalProperties, false);
  assert.deepEqual(candidateSchema.properties.provenance.required, ['build', 'publication']);
  assert.equal(candidateSchema.$defs.workflowRun.additionalProperties, false);
  const baselineSchema = JSON.parse(await readFile(new URL('schemas/baseline-manifest.schema.json', root), 'utf8'));
  assert.equal(baselineSchema.properties.components.additionalProperties, false);
  assert.equal(baselineSchema.properties.testHarnesses.additionalProperties, false);
  assert.equal(baselineSchema.$defs.mavenComponent.additionalProperties, false);
});

test('compatibility-set PR URLs resolve to unique allowed components', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-compatibility-set-'));
  const input = join(directory, 'pull-requests.txt');
  const output = join(directory, 'request.json');
  await writeFile(input, [
    'https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/12',
    'https://github.com/The-Pipeline-Framework/pipelineframework-blocks/pull/34'
  ].join('\n'));
  await execFileAsync(process.execPath, [
    new URL('../scripts/parse-compatibility-prs.mjs', import.meta.url).pathname,
    '--components', new URL('../components.yml', import.meta.url).pathname,
    '--input', input,
    '--setId', 'manual-123',
    '--output', output
  ]);
  const parsed = JSON.parse(await readFile(output, 'utf8'));
  assert.deepEqual(parsed.requests.map(({component}) => component), ['blocks', 'runtime']);

  await writeFile(input, [
    'https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/12',
    'https://github.com/The-Pipeline-Framework/pipelineframework-runtime/pull/13'
  ].join('\n'));
  await assert.rejects(
    execFileAsync(process.execPath, [
      new URL('../scripts/parse-compatibility-prs.mjs', import.meta.url).pathname,
      '--components', new URL('../components.yml', import.meta.url).pathname,
      '--input', input,
      '--setId', 'manual-123',
      '--output', output
    ]),
    /multiple pull requests/
  );
});

test('candidate event CLI emits the workflow output names', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-event-output-'));
  const eventPath = join(directory, 'event.json');
  await writeFile(eventPath, `${JSON.stringify(event())}\n`);
  const {stdout} = await execFileAsync(process.execPath, [
    new URL('../scripts/validate-event.mjs', import.meta.url).pathname,
    '--components', new URL('../components.yml', import.meta.url).pathname,
    '--event', eventPath
  ]);
  const summary = JSON.parse(stdout);
  assert.deepEqual(Object.keys(summary).sort(), [
    'candidate_version', 'compatibility_set_id', 'component', 'publication_run_id',
    'pull_request_number', 'repository', 'repository_name', 'source_sha'
  ]);
});
