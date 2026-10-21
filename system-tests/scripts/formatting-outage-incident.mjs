#!/usr/bin/env node
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {pathToFileURL} from 'node:url';
import {parseArgs} from 'node:util';

// One supervised outage recovery, not a general Maven-argument or test-skip interface.
export const incident = Object.freeze({
  reason: 'ECLIPSE_4_34_SHUTDOWN_20261021',
  expiresAt: '2026-10-22T00:00:00.000Z',
  repository: 'The-Pipeline-Framework/pipelineframework',
  sourceRepository: 'The-Pipeline-Framework/pipelineframework-runtime',
  sourceSha: '44e412812ed1bda3eb23ab07859864d1c5c89754',
  sourcePullRequest: 68,
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
const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
const requireValue = (condition, message) => { if (!condition) throw new Error(message); };

export function validateRequest(context, now = new Date()) {
  requireValue(context.repository === incident.repository && context.ref === 'refs/heads/main' &&
    context.eventName === 'workflow_dispatch', 'manual replay requires the trusted main workflow');
  requireValue(Number.isFinite(now.getTime()) && now.toISOString().startsWith('2026-10-21T'),
    'formatting incident replay has expired');
  const inputs = context.inputs;
  requireValue(inputs && Object.keys(inputs).sort().join(',') ===
    'formatting_outage_exception,source_run_id,source_sha', 'unsupported replay inputs');
  requireValue(inputs.source_run_id === String(incident.sourceRunId) && inputs.source_sha === incident.sourceSha,
    'replay must name the reviewed original run and runtime head');
  requireValue(typeof inputs.formatting_outage_exception === 'boolean', 'exception must be an explicit boolean');
  return {schemaVersion: 1, reason: incident.reason, expiresAt: incident.expiresAt,
    originalRunId: incident.sourceRunId, sourceRepository: incident.sourceRepository,
    sourceSha: incident.sourceSha, formattingException: inputs.formatting_outage_exception,
    mavenArgs: inputs.formatting_outage_exception ? '-Dspotless.skip=true' : '',
    baselinePromotion: false, artifacts: incident.artifacts};
}

export function validateReplay(context, run, artifacts, pull, resolvedBytes, matrixBytes, now = new Date()) {
  const receipt = validateRequest(context, now);
  requireValue(run.id === incident.sourceRunId && run.repository?.full_name === incident.repository &&
    run.path === '.github/workflows/system-test-candidate.yml' && run.event === 'repository_dispatch' &&
    run.head_branch === 'main' && run.head_sha === incident.coordinatorSha &&
    run.status === 'completed' && run.conclusion === 'failure', 'original workflow provenance mismatch');
  requireValue(pull.number === incident.sourcePullRequest && pull.state === 'open' &&
    pull.head?.sha === incident.sourceSha && pull.head?.repo?.full_name === incident.sourceRepository &&
    pull.base?.ref === 'main' && pull.base?.repo?.full_name === incident.sourceRepository,
    'runtime pull request is no longer the reviewed current head');
  for (const expected of Object.values(incident.artifacts)) {
    const matches = artifacts.artifacts?.filter(a => a.id === expected.id) ?? [];
    requireValue(matches.length === 1 && matches[0].name === expected.name && matches[0].expired === false &&
      matches[0].size_in_bytes === expected.bytes && matches[0].digest === `sha256:${expected.sha256}` &&
      matches[0].workflow_run?.id === incident.sourceRunId, 'original artifact identity mismatch');
  }
  requireValue(sha256(resolvedBytes) === incident.resolvedSetSha256, 'resolved closure changed');
  const resolved = JSON.parse(resolvedBytes);
  requireValue(resolved.components?.runtime?.sha === incident.sourceSha &&
    resolved.components.runtime.mavenVersion === '26.10.1-pr.68.44e412812ed1', 'runtime candidate changed');
  requireValue(sha256(matrixBytes) === '62c88cbf034d50759e4b077a924e2db5f130a67cd9c5c8c76d77215c48bec73c', 'mandatory suite matrix changed');
  const matrix = JSON.parse(matrixBytes);
  requireValue(matrix.include?.length === 6 && new Set(matrix.include.map(s => s.shard)).size === 6,
    'all six original mandatory groups are required');
  return {...receipt, resolvedSetSha256: sha256(resolvedBytes), matrixSha256: sha256(matrixBytes), matrix};
}

export function validateArchive(bytes, kind) {
  const expected = incident.artifacts[kind];
  requireValue(expected && bytes.length === expected.bytes && sha256(bytes) === expected.sha256,
    'original artifact archive digest mismatch');
  return {artifactId: expected.id, sha256: expected.sha256, bytes: expected.bytes};
}

export function publicationDecision(context, paths, now = new Date()) {
  if (context.eventName !== 'push' || context.message?.split('\n')[0] !== incident.mergeMarker) {
    return {publish: true, reason: 'ordinary publication policy'};
  }
  requireValue(context.repository === incident.repository && context.ref === 'refs/heads/main' &&
    Number.isFinite(now.getTime()) && now.toISOString().startsWith('2026-10-21T'),
    'incident bootstrap publication suppression is outside its reviewed window');
  requireValue(Array.isArray(paths) && paths.length > 0 &&
    paths.every(path => bootstrapPaths.includes(path)), 'bootstrap contains a non-allowlisted publication change');
  return {publish: false, reason: incident.reason, paths};
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const {values} = parseArgs({options: Object.fromEntries(
    ['context', 'run', 'artifacts', 'pull', 'resolved', 'matrix', 'archive', 'kind', 'paths']
      .map(name => [name, {type: 'string'}])), strict: true});
  const json = async path => JSON.parse(await readFile(path, 'utf8'));
  let result;
  if (values.archive) result = validateArchive(await readFile(values.archive), values.kind);
  else if (values.paths) result = publicationDecision(await json(values.context), await json(values.paths));
  else if (values.run) result = validateReplay(await json(values.context), await json(values.run),
    await json(values.artifacts), await json(values.pull), await readFile(values.resolved), await readFile(values.matrix));
  else result = validateRequest(await json(values.context));
  process.stdout.write(`${JSON.stringify(result)}\n`);
}
