import assert from 'node:assert/strict';
import {mkdtemp, mkdir, writeFile, readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import test from 'node:test';

const runner = new URL('../scripts/run-search-cloud.mjs', import.meta.url).pathname;
const sha = 'a'.repeat(40);
const repository = 'The-Pipeline-Framework/pipelineframework-reference-implementations';
const version = '26.10.1-main.aaaaaaaaaaaa';
async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'tpf-cloud-'));
  await mkdir(join(root, 'search'));
  const reports = join(root, 'search/orchestrator-svc/target/failsafe-reports');
  await mkdir(reports, {recursive: true});
  await writeFile(join(reports, 'TEST-org.pipelineframework.search.orchestrator.service.AwsLambdaModularEndToEndIT.xml'),
    '<testsuite tests="3" skipped="0" failures="0" errors="0"/>');
  await writeFile(join(root, 'pom.xml'), '<project><properties>' +
    ['bom', 'compiler', 'connectors', 'runtime'].map((key) =>
      `<pipelineframework.${key}.version>26.10.1-SNAPSHOT</pipelineframework.${key}.version>`).join('') +
    '</properties></project>');
  const capture = join(root, 'capture');
  const script = '#!/bin/sh\nprintf "%s\\n" "$MAVEN_ARGS" "$@" > "$TPF_CAPTURE"\n';
  await writeFile(join(root, 'mvnw'), script, {mode: 0o755});
  await writeFile(join(root, 'search/build-lambda-modular.sh'), script, {mode: 0o755});
  const set = {testHarnesses: {references: {repository, sha}},
    coordinationBom: {mavenVersion: version}, components: Object.fromEntries(
      ['compiler', 'connectors', 'runtime'].map((component) => [component, {mavenVersion: version}]))};
  return {root, capture, set};
}
async function run(f, mode = 'build', url = '') {
  await writeFile(join(f.root, 'set.json'), JSON.stringify(f.set));
  return spawnSync(process.execPath, [runner, '--mode', mode, '--source', f.root,
    '--sourceSha', sha, '--resolvedSet', join(f.root, 'set.json'),
    '--mavenRepository', join(f.root, 'm2')], {encoding: 'utf8', env: {
      ...process.env, TPF_CAPTURE: f.capture, AWS_LAMBDA_ORCHESTRATOR_URL: url
    }});
}
test('AWS cloud build pins all reference dependencies to the same resolved train', async () => {
  const f = await fixture(); const result = await run(f);
  assert.equal(result.status, 0, result.stderr);
  const capture = await readFile(f.capture, 'utf8');
  for (const property of ['bom', 'compiler', 'connectors', 'runtime']) {
    assert.ok(capture.includes(`-Dpipelineframework.${property}.version=${version}`));
  }
  assert.ok(capture.includes(`-Dmaven.repo.local=${join(f.root, 'm2')}`));
  assert.doesNotMatch(await readFile(join(f.root, 'pom.xml'), 'utf8'), /SNAPSHOT/);
});
test('AWS cloud test invokes owner integration test, not a module-only smoke', async () => {
  const f = await fixture(); const result = await run(f, 'test', 'https://abc.lambda-url.us-east-2.on.aws/');
  assert.equal(result.status, 0, result.stderr);
  assert.match(await readFile(f.capture, 'utf8'), /-Dit.test=AwsLambdaModularEndToEndIT/);
});
test('a skipped AWS test suite cannot produce green cloud evidence', async () => {
  const f = await fixture();
  await writeFile(join(f.root, 'search/orchestrator-svc/target/failsafe-reports',
    'TEST-org.pipelineframework.search.orchestrator.service.AwsLambdaModularEndToEndIT.xml'),
  '<testsuite tests="3" skipped="3" failures="0" errors="0"/>');
  const result = await run(f, 'test', 'https://abc.lambda-url.us-east-2.on.aws/');
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /unskipped tests/);
});
for (const defect of ['sha', 'repository', 'snapshot', 'missing-pin', 'missing-url', 'foreign-url']) {
  test(`cloud preparation fails closed on ${defect}`, async () => {
    const f = await fixture(); let mode = 'build'; let url = '';
    if (defect === 'sha') f.set.testHarnesses.references.sha = 'b'.repeat(40);
    if (defect === 'repository') f.set.testHarnesses.references.repository = 'foreign/repo';
    if (defect === 'snapshot') f.set.components.compiler.mavenVersion = '26.10.1-SNAPSHOT';
    if (defect === 'missing-pin') delete f.set.components.runtime;
    if (defect.endsWith('url')) {mode = 'test'; url = defect === 'foreign-url' ? 'https://example.com' : '';}
    const result = await run(f, mode, url);
    assert.notEqual(result.status, 0);
    await assert.rejects(readFile(f.capture), {code: 'ENOENT'});
  });
}
test('cloud jobs isolate credentials and baseline promotion requires the relay', async () => {
  const workflow = await readFile(new URL('../../.github/workflows/system-test-search-aws.yml', import.meta.url), 'utf8');
  for (const job of ['build', 'test']) {
    const body = workflow.split(`\n  ${job}:\n`)[1].split(/\n  [a-z]+:\n/)[0];
    assert.doesNotMatch(body, /id-token: write|secrets\.|configure-aws-credentials|packages:|statuses:/);
  }
  assert.match(workflow, /cleanup:\n\s+if: always\(\)/);
  assert.match(workflow, /role\/search-modular-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}-lambda-exec/);
  assert.doesNotMatch(workflow, /cancel-in-progress: true/);
  const full = await readFile(new URL('../../.github/workflows/system-test-full-train.yml', import.meta.url), 'utf8');
  assert.match(full, /uses: \.\/\.github\/workflows\/system-test-search-aws.yml/);
  assert.match(full, /needs\.cloud-deployments.result == 'success'/);
  assert.match(full, /select\(\.suite != "cloud-deployments"\)/);
});
