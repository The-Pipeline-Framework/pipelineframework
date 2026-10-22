#!/usr/bin/env node
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {parseArgs} from 'node:util';

// Two explicitly pinned outage recoveries, never a general Maven-argument or test-skip interface.
export const incident = Object.freeze({
  reason: 'ECLIPSE_4_34_SHUTDOWN_20261021',
  expiresAt: '2026-10-22T00:00:00.000Z',
  repository: 'The-Pipeline-Framework/pipelineframework',
  sourceRepository: 'The-Pipeline-Framework/pipelineframework-runtime',
  sourceSha: '44e412812ed1bda3eb23ab07859864d1c5c89754',
  sourcePullRequest: 68,
  sourceVersion: '26.10.1-pr.68.44e412812ed1',
  day: '2026-10-21',
  matrixSha256: '62c88cbf034d50759e4b077a924e2db5f130a67cd9c5c8c76d77215c48bec73c',
  sourceRunId: 39623835075,
  coordinatorSha: '8aab0a1ba127428a6a2afcd33e7cd87489f7f463',
  resolvedSetSha256: '826d84d3a6ad732d3c2883d27e8f1d8610193b66b1a96f2bddb6dba903f75d35',
  mergeMarker: 'ci(system-tests): replay Eclipse formatting outage [incident-20261021]',
  artifacts: Object.freeze({
    resolved: Object.freeze({id: 12356422381, name: 'tpf-resolved-system-test-set',
      sha256: '091dfa6cb79cc3059785cb89c62285996fc6bcaf173b9c1a3e3f5ac0eaab9078', bytes: 47614}),
    materialized: Object.freeze({id: 12358000239, name: 'tpf-materialized-system-test-inputs',
      sha256: 'fd59a08cbdd3630644eb62a75f52afc91b163a4168d9a3a5dc90abd4941ef217', bytes: 360046484})
  })
});

export const mainIncident = Object.freeze({
  reason: 'ECLIPSE_4_34_SHUTDOWN_20261022',
  expiresAt: '2026-10-23T00:00:00.000Z',
  day: '2026-10-22',
  repository: 'The-Pipeline-Framework/pipelineframework',
  sourceRepository: 'The-Pipeline-Framework/pipelineframework-runtime',
  sourceSha: '4c0de12a53710a3599a35dd851a1dbfca7e0df1a',
  sourcePullRequest: null,
  sourceVersion: '26.10.1-main.4c0de12a5371',
  sourceRunId: 39705054075,
  coordinatorSha: '341d991e61e71c03761073788b932aa5445e3b52',
  resolvedSetSha256: '529c1d2eb7d8585f340316d54d343dd91bafb6fd62a405cd376f508dff59e838',
  matrixSha256: '46b96662593807728fcb6d9486fd1ac7a0d9736eefa7ced6a291f2a28554a728',
  mergeMarker: 'ci(system-tests): replay Eclipse formatting outage [incident-20261022]',
  artifacts: Object.freeze({
    resolved: Object.freeze({id: 12394507647, name: 'tpf-resolved-system-test-set',
      sha256: 'f9826dbbd456bd09886c239520c44a205bdd9c7aaf8efeaf462051407d98d307', bytes: 47741}),
    materialized: Object.freeze({id: 12395262575, name: 'tpf-materialized-system-test-inputs',
      sha256: '3f649ffcc4ee3b22eb571e7374dc175e8148de9bcd716f32439e6e0600f21113', bytes: 360046049})
  })
});
const incidents = [incident, mainIncident];

export const bootstrapPaths = Object.freeze([
  '.github/workflows/publish-snapshots.yml',
  '.github/workflows/system-test-candidate.yml',
  '.github/workflows/system-test-plumbing.yml',
  'system-tests/scripts/formatting-outage-incident.mjs',
  'system-tests/test/formatting-outage-incident.test.mjs',
  'system-tests/test/fixtures/eclipse-outage-20261021/resolved-set.json',
  'system-tests/test/fixtures/eclipse-outage-20261021/suite-matrix.json',
  'docs/evolve/cross-repository-system-tests.md',
  'docs/deploy/orchestrator-runtime/aws-durable.md'
]);
export const mainBootstrapPaths = Object.freeze([
  '.github/workflows/publish-snapshots.yml',
  '.github/workflows/system-test-candidate.yml',
  'system-tests/scripts/formatting-outage-incident.mjs',
  'system-tests/test/formatting-outage-incident.test.mjs',
  'system-tests/test/fixtures/eclipse-outage-20261022/resolved-set.json',
  'system-tests/test/fixtures/eclipse-outage-20261022/suite-matrix.json',
  'docs/evolve/cross-repository-system-tests.md'
]);

const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const requireValue = (condition, message) => { if (!condition) throw new Error(message); };

export function validateRequest(context, now = new Date()) {
  const selected = incidents.find(entry => context.inputs?.source_run_id === String(entry.sourceRunId) &&
    context.inputs?.source_sha === entry.sourceSha);
  requireValue(selected, 'replay must name the reviewed original run and runtime head');
  requireValue(context.repository === selected.repository && context.ref === 'refs/heads/main' &&
    context.eventName === 'workflow_dispatch', 'manual replay requires the trusted main workflow');
  requireValue(Number.isFinite(now.getTime()) && now.toISOString().startsWith(`${selected.day}T`),
    'formatting incident replay has expired');
  const inputs = context.inputs;
  requireValue(inputs && Object.keys(inputs).sort().join(',') ===
    'formatting_outage_exception,source_run_id,source_sha', 'unsupported replay inputs');
  requireValue(inputs.source_run_id === String(selected.sourceRunId) && inputs.source_sha === selected.sourceSha,
    'replay must name the reviewed original run and runtime head');
  requireValue(typeof inputs.formatting_outage_exception === 'boolean', 'exception must be an explicit boolean');
  return {schemaVersion: 1, reason: selected.reason, expiresAt: selected.expiresAt,
    originalRunId: selected.sourceRunId, sourceRepository: selected.sourceRepository,
    sourceSha: selected.sourceSha, sourcePullRequest: selected.sourcePullRequest,
    sourcePath: selected.sourcePullRequest ? `pulls/${selected.sourcePullRequest}` : 'git/ref/heads/main',
    formattingException: inputs.formatting_outage_exception,
    mavenArgs: inputs.formatting_outage_exception ? '-Dspotless.skip=true' : '',
    baselinePromotion: false, artifacts: selected.artifacts};
}

export function validateSource(context, source, now = new Date()) {
  const receipt = validateRequest(context, now);
  if (receipt.sourcePullRequest) {
    requireValue(source.number === receipt.sourcePullRequest && source.state === 'open' &&
      source.head?.sha === receipt.sourceSha && source.head?.repo?.full_name === receipt.sourceRepository &&
      source.base?.ref === 'main' && source.base?.repo?.full_name === receipt.sourceRepository,
      'runtime pull request is no longer the reviewed current head');
  } else {
    requireValue(source.ref === 'refs/heads/main' && source.object?.type === 'commit' &&
      source.object?.sha === receipt.sourceSha, 'runtime main is no longer the reviewed current head');
  }
  return receipt;
}

export function validateReplay(context, run, artifacts, pull, resolvedBytes, matrixBytes, now = new Date()) {
  const receipt = validateSource(context, pull, now);
  const selected = incidents.find(entry => entry.sourceRunId === receipt.originalRunId);
  requireValue(run.id === selected.sourceRunId && run.repository?.full_name === selected.repository &&
    run.path === '.github/workflows/system-test-candidate.yml' && run.event === 'repository_dispatch' &&
    run.head_branch === 'main' && run.head_sha === selected.coordinatorSha &&
    run.status === 'completed' && run.conclusion === 'failure', 'original workflow provenance mismatch');
  for (const expected of Object.values(selected.artifacts)) {
    const matches = artifacts.artifacts?.filter(a => a.id === expected.id) ?? [];
    requireValue(matches.length === 1 && matches[0].name === expected.name && matches[0].expired === false &&
      matches[0].size_in_bytes === expected.bytes && matches[0].digest === `sha256:${expected.sha256}` &&
      matches[0].workflow_run?.id === selected.sourceRunId, 'original artifact identity mismatch');
  }
  requireValue(sha256(resolvedBytes) === selected.resolvedSetSha256, 'resolved closure changed');
  const resolved = JSON.parse(resolvedBytes);
  requireValue(resolved.components?.runtime?.sha === selected.sourceSha &&
    resolved.components.runtime.mavenVersion === selected.sourceVersion, 'runtime candidate changed');
  requireValue(sha256(matrixBytes) === selected.matrixSha256, 'mandatory suite matrix changed');
  const matrix = JSON.parse(matrixBytes);
  requireValue(matrix.include?.length === 6 && new Set(matrix.include.map(s => s.shard)).size === 6,
    'all six original mandatory groups are required');
  return {...receipt, resolvedSetSha256: sha256(resolvedBytes), matrixSha256: sha256(matrixBytes), matrix};
}

export function validateArchive(bytes, kind, context, now = new Date()) {
  const selected = context ? incidents.find(entry => entry.sourceRunId === validateRequest(context, now).originalRunId) : incident;
  const expected = selected.artifacts[kind];
  requireValue(expected && bytes.length === expected.bytes && sha256(bytes) === expected.sha256,
    'original artifact archive digest mismatch');
  return {artifactId: expected.id, sha256: expected.sha256, bytes: expected.bytes};
}

export function publicationDecision(context, paths, now = new Date()) {
  const selected = incidents.find(entry => context.message?.split('\n')[0] === entry.mergeMarker);
  if (context.eventName !== 'push' || !selected) {
    return {publish: true, reason: 'ordinary publication policy'};
  }
  requireValue(context.repository === selected.repository && context.ref === 'refs/heads/main' &&
    Number.isFinite(now.getTime()) && now.toISOString().startsWith(`${selected.day}T`),
    'incident bootstrap publication suppression is outside its reviewed window');
  requireValue(Array.isArray(paths) && paths.length > 0 &&
    paths.every(path => (selected === incident ? bootstrapPaths : mainBootstrapPaths).includes(path)), 'bootstrap contains a non-allowlisted publication change');
  return {publish: false, reason: selected.reason, paths};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const {values} = parseArgs({options: Object.fromEntries(
    ['context', 'run', 'artifacts', 'pull', 'resolved', 'matrix', 'archive', 'kind', 'paths']
      .map(name => [name, {type: 'string'}])), strict: true});
  const json = async path => JSON.parse(await readFile(path, 'utf8'));
  let result;
  if (values.archive) result = validateArchive(await readFile(values.archive), values.kind, await json(values.context));
  else if (values.paths) result = publicationDecision(await json(values.context), await json(values.paths));
  else if (values.run) result = validateReplay(await json(values.context), await json(values.run),
    await json(values.artifacts), await json(values.pull), await readFile(values.resolved), await readFile(values.matrix));
  else if (values.pull) result = validateSource(await json(values.context), await json(values.pull));
  else result = validateRequest(await json(values.context));
  process.stdout.write(`${JSON.stringify(result)}\n`);
}
