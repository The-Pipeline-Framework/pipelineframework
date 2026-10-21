import assert from 'node:assert/strict';
import {readFile, writeFile, mkdtemp, chmod} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawnSync} from 'node:child_process';
import test from 'node:test';
import {incident, bootstrapPaths, publicationDecision, validateArchive, validateReplay, validateRequest}
  from '../scripts/formatting-outage-incident.mjs';

const now = new Date('2026-10-21T16:30:00.000Z');
const resolved = await readFile(new URL('./fixtures/eclipse-outage-20261021/resolved-set.json', import.meta.url));
const matrix = await readFile(new URL('./fixtures/eclipse-outage-20261021/suite-matrix.json', import.meta.url));
const workflow = await readFile(new URL('../../.github/workflows/system-test-candidate.yml', import.meta.url), 'utf8');
const publisher = await readFile(new URL('../../.github/workflows/publish-snapshots.yml', import.meta.url), 'utf8');
function fixture(enabled = false) {
  return {
    context: {repository: incident.repository, ref: 'refs/heads/main', eventName: 'workflow_dispatch',
      inputs: {source_run_id: String(incident.sourceRunId), source_sha: incident.sourceSha,
        formatting_outage_exception: enabled}},
    run: {id: incident.sourceRunId, repository: {full_name: incident.repository},
      path: '.github/workflows/system-test-candidate.yml', event: 'repository_dispatch',
      head_branch: 'main', head_sha: incident.coordinatorSha, status: 'completed', conclusion: 'failure'},
    artifacts: {artifacts: Object.values(incident.artifacts).map(a => ({id: a.id, name: a.name,
      expired: false, size_in_bytes: a.bytes, digest: `sha256:${a.sha256}`, workflow_run: {id: incident.sourceRunId}}))},
    pull: {number: incident.sourcePullRequest, state: 'open', head: {sha: incident.sourceSha,
      repo: {full_name: incident.sourceRepository}}, base: {ref: 'main', repo: {full_name: incident.sourceRepository}}}
  };
}
function validate(value, resolvedBytes = resolved, matrixBytes = matrix, time = now) {
  return validateReplay(value.context, value.run, value.artifacts, value.pull, resolvedBytes, matrixBytes, time);
}

test('incident default keeps formatting; explicit boolean allows only exact Spotless flag', () => {
  const ordinary = validate(fixture());
  assert.equal(ordinary.mavenArgs, '');
  assert.equal(ordinary.formattingException, false);
  const exception = validate(fixture(true));
  assert.equal(exception.mavenArgs, '-Dspotless.skip=true');
  assert.equal(exception.baselinePromotion, false);
  assert.equal(exception.matrix.include.length, 6);
  assert.deepEqual(exception.matrix, JSON.parse(matrix));
  assert.equal(exception.resolvedSetSha256, incident.resolvedSetSha256);
});

for (const [name, mutate] of [
  ['branch workflow', f => f.context.ref = 'refs/heads/feature'],
  ['foreign coordinator', f => f.context.repository = 'attacker/pipelineframework'],
  ['repository dispatch cannot request exception', f => f.context.eventName = 'repository_dispatch'],
  ['different original run', f => f.context.inputs.source_run_id = '39623835076'],
  ['different expected head', f => f.context.inputs.source_sha = 'a'.repeat(40)],
  ['string boolean', f => f.context.inputs.formatting_outage_exception = 'true'],
  ['arbitrary Maven flag', f => f.context.inputs.MAVEN_ARGS = '-DskipTests'],
  ['untrusted original workflow', f => f.run.path = '.github/workflows/evil.yml'],
  ['original PR workflow', f => f.run.event = 'pull_request'],
  ['different original coordinator revision', f => f.run.head_sha = 'b'.repeat(40)],
  ['original run incomplete', f => f.run.status = 'in_progress'],
  ['different repository artifact', f => f.artifacts.artifacts[0].workflow_run.id++],
  ['artifact ID replacement', f => f.artifacts.artifacts[0].id++],
  ['artifact digest replacement', f => f.artifacts.artifacts[0].digest = 'sha256:' + '0'.repeat(64)],
  ['expired artifact', f => f.artifacts.artifacts[1].expired = true],
  ['current source head moved', f => f.pull.head.sha = 'c'.repeat(40)],
  ['closed source PR', f => f.pull.state = 'closed'],
  ['foreign source repository', f => f.pull.head.repo.full_name = 'attacker/runtime'],
  ['changed source base', f => f.pull.base.ref = 'feature']
]) test(`replay rejects ${name}`, () => {
  const value = fixture(true); mutate(value); assert.throws(() => validate(value));
});

test('expired incident cannot authorize a replay', () => {
  assert.throws(() => validate(fixture(true), resolved, matrix, new Date(incident.expiresAt)), /expired/);
  assert.throws(() => validateRequest(fixture().context, new Date('invalid')), /expired/);
});

test('exact original closure and six-group matrix cannot be substituted or filtered', () => {
  assert.throws(() => validate(fixture(), Buffer.from('{}')), /closure changed/);
  const filtered = JSON.parse(matrix); filtered.include.pop();
  assert.throws(() => validate(fixture(), resolved, Buffer.from(JSON.stringify(filtered))), /matrix changed/);
  const changed = Buffer.from(matrix); changed[10] ^= 1;
  assert.throws(() => validate(fixture(), resolved, changed), /matrix changed/);
  assert.throws(() => validateArchive(Buffer.from('tampered'), 'materialized'), /digest mismatch/);
  assert.throws(() => validateArchive(Buffer.from('tampered'), 'unknown'), /digest mismatch/);
});

const publicationContext = {repository: incident.repository, ref: 'refs/heads/main',
  eventName: 'push', message: incident.mergeMarker + '\n\nReviewed outage-only bootstrap'};
test('only exact incident marker, UTC day and non-Maven allowlist suppress bootstrap publication', () => {
  assert.equal(publicationDecision(publicationContext, [...bootstrapPaths], now).publish, false);
  assert.throws(() => publicationDecision(publicationContext, ['framework/bom/pom.xml'], now), /non-allowlisted/);
  assert.throws(() => publicationDecision(publicationContext, [], now), /non-allowlisted/);
  assert.throws(() => publicationDecision(publicationContext, [...bootstrapPaths], new Date(incident.expiresAt)), /window/);
  assert.throws(() => publicationDecision({...publicationContext, ref: 'refs/heads/feature'}, [...bootstrapPaths], now), /window/);
});
test('ordinary pushes and explicit manual recovery retain existing publication behavior', () => {
  assert.equal(publicationDecision({...publicationContext, message: 'ordinary merge'}, ['framework/pom.xml'], now).publish, true);
  assert.equal(publicationDecision({...publicationContext, eventName: 'workflow_dispatch'}, [], now).publish, true);
  assert.equal(publicationDecision({...publicationContext, message: incident.mergeMarker + ' suffix'}, [], now).publish, true);
});

test('workflow keeps automatic intake and full unprivileged runner; manual exception cannot promote baseline', () => {
  assert.match(workflow, /intake:\n    if: github.event_name == 'repository_dispatch'/);
  const manual = workflow.split('\n  formatting-replay-intake:')[1];
  assert.match(manual, /github.event_name == 'workflow_dispatch' && github.ref == 'refs\/heads\/main'/);
  assert.match(workflow, /formatting_outage_exception:[\s\S]*?default: false/);
  assert.match(manual, /--archive inputs.zip --kind materialized/);
  assert.match(manual, /MAVEN_ARGS: \$\{\{ needs.formatting-replay-intake.outputs.maven_args \}\}/);
  assert.match(manual, /node coordination\/system-tests\/scripts\/run-shard.mjs/);
  assert.doesNotMatch(manual, /skipTests|maven.test.skip|permission-id-token|packages: write|promote-baseline/);
  const execution = manual.split('      - name: Run original full product shard without privileged credentials')[1]
    .split('\n  formatting-replay-report:')[0];
  assert.doesNotMatch(execution, /GH_TOKEN|secrets\./);
  assert.match(manual, /TEST_RESULT.*success[\s\S]*All six full groups passed/);
  assert.match(manual, /test "\$state" = success/);
  assert.match(publisher, /needs.publication-policy.outputs.publish == 'true'/);
});


test('fixed exception reaches ordinary verify while non-format test failures remain failures', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-formatting-incident-'));
  const capture = join(directory, 'capture.txt');
  await writeFile(join(directory, 'mvnw'), '#!/usr/bin/env bash\nprintf \'%s\\n\' "$@" "$MAVEN_ARGS" > "$CAPTURE"\nexit 37\n');
  await chmod(join(directory, 'mvnw'), 0o755);
  await writeFile(join(directory, 'pom.xml'), '<project/>');
  await writeFile(join(directory, 'manifest.json'), JSON.stringify({schemaVersion: 1,
    suites: {verify: {command: ['./mvnw', '-B', 'clean', 'verify'], timeoutMinutes: 1}}}));
  await writeFile(join(directory, 'resolved.json'), '{}');
  const result = spawnSync(process.execPath, [new URL('../scripts/run-suite.mjs', import.meta.url).pathname,
    '--manifest', join(directory, 'manifest.json'), '--entrypoint', 'verify',
    '--resolvedSet', join(directory, 'resolved.json'), '--versionProperties', '{}',
    '--cwd', directory, '--mavenRepository', join(directory, 'm2')],
  {env: {...process.env, CAPTURE: capture, MAVEN_ARGS: validate(fixture(true)).mavenArgs}, encoding: 'utf8'});
  assert.equal(result.status, 37, result.stderr);
  const arguments_ = await readFile(capture, 'utf8');
  assert.match(arguments_, /^verify$/m);
  assert.match(arguments_, /-Dspotless.skip=true/);
  assert.match(arguments_, /-Dmaven.repo.local=/);
  assert.doesNotMatch(arguments_, /skipTests|maven.test.skip|verify.skip/);
});


test('ordinary multi-commit push does not require unavailable BEFORE history', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-publication-incident-'));
  const policy = publisher.split('      - name: Preserve ordinary publication; suppress only reviewed incident bootstrap')[1]
    .split('  publish:')[0];
  const run = policy.split('        run: |\n')[1].split('\n').map(line => line.slice(10)).join('\n')
    .replaceAll('${{ github.event_name }}', 'push')
    .replace('node system-tests/scripts/formatting-outage-incident.mjs',
      `node ${new URL('../scripts/formatting-outage-incident.mjs', import.meta.url).pathname}`);
  const output = join(directory, 'output.txt');
  const result = spawnSync('bash', ['-c', run], {cwd: directory, encoding: 'utf8', env: {...process.env,
    POLICY_CONTEXT: JSON.stringify({...publicationContext, message: 'ordinary multi-commit push'}),
    BEFORE_SHA: 'unavailable-before-sha', AFTER_SHA: 'unavailable-after-sha',
    GITHUB_OUTPUT: output, GITHUB_STEP_SUMMARY: join(directory, 'summary.txt')}});
  assert.equal(result.status, 0, result.stderr);
  assert.equal(await readFile(output, 'utf8'), 'publish=true\n');
});

test('automatic intake skips every incident job before empty matrix evaluation', () => {
  assert.match(workflow, /formatting-replay-shard:[\s\S]*?if: github.event_name == 'workflow_dispatch' && needs.formatting-replay-intake.result == 'success'\n    needs:/);
  assert.match(workflow, /formatting-replay-report:[\s\S]*?if: always\(\) && needs.formatting-replay-intake.result == 'success'/);
  const automatic = {eventName: 'repository_dispatch', intake: 'skipped'};
  assert.equal(automatic.eventName === 'workflow_dispatch' && automatic.intake === 'success', false);
});
