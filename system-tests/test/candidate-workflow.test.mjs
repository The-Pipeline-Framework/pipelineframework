import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {runInNewContext} from 'node:vm';

const workflow = await readFile(new URL('../../.github/workflows/system-test-candidate.yml', import.meta.url), 'utf8');

test('main candidate continues after skipped PR coalescing, while prerequisite failures stop tests', () => {
  const needs = {
    intake: {result: 'success'}, coalesce: {result: 'skipped'}, retrieve: {result: 'success'},
    baseline: {result: 'success', outputs: {has_shards: 'true'}}, materialize: {result: 'success'}
  };
  for (const [job, prerequisites] of [
    ['baseline', ['intake', 'retrieve']], ['materialize', ['baseline']],
    ['run-product-shard', ['baseline', 'materialize']]
  ]) {
    const block = workflow.split(`\n  ${job}:\n`)[1].split(/\n  [a-z][a-z-]*:\n/)[0];
    const condition = block.match(/^    if: >-\n((?:      .*\n)+)/m)[1].trim().replace(/\n\s*/g, ' ');
    assert.match(condition, /always\(\)/, `${job} must survive skipped ancestors`);
    assert.equal(runInNewContext(condition, {needs, always: () => true}, {timeout: 100}), true);
    for (const prerequisite of prerequisites) {
      for (const result of ['failure', 'cancelled', 'skipped']) {
        const rejected = structuredClone(needs);
        rejected[prerequisite].result = result;
        assert.equal(runInNewContext(condition, {needs: rejected, always: () => true}, {timeout: 100}), false,
          `${job} must reject ${prerequisite}=${result}`);
      }
    }
    if (job === 'run-product-shard') {
      const empty = structuredClone(needs); empty.baseline.outputs.has_shards = 'false';
      assert.equal(runInNewContext(condition, {needs: empty, always: () => true}, {timeout: 100}), false);
    }
  }
});

test('candidate provenance resolves the exact attested build attempt', () => {
  assert.match(
    workflow,
    /\.provenance\.build\.runAttempt \| select\(type == "number" and \. > 0\)/
  );
  assert.match(
    workflow,
    /actions\/runs\/\$build_run_id\/attempts\/\$build_run_attempt/
  );
  assert.doesNotMatch(
    workflow,
    /actions\/runs\/\$build_run_id" > intake\/build-workflow-run\.json/
  );
});

test('singleton candidates overlay the current attested main set', () => {
  const reconcile = workflow.indexOf('node system-tests/scripts/reconcile-main-candidates.mjs');
  const effectiveBaseline = workflow.indexOf('--baseline resolved/effective-baseline.json');
  const overlay = workflow.indexOf('node system-tests/scripts/resolve-set.mjs');

  assert.ok(reconcile >= 0, 'candidate workflow must reconcile current main candidates');
  assert.ok(effectiveBaseline > reconcile, 'PR overlay must consume the reconciled current-main baseline');
  assert.ok(overlay > reconcile, 'current-main reconciliation must precede the PR overlay');
  assert.match(workflow, /candidate_image:main-\$component/);
  assert.match(workflow, /No attested current-main candidate exists for \$component at \$current_sha/);
});

test('candidate status reporting survives a failed coalescing dispatch', () => {
  assert.match(
    workflow,
    /needs\.pending\.result == 'success' &&\s*\(needs\.coalesce\.result != 'success' \|\| needs\.coalesce\.outputs\.coalesced != 'true'\)/
  );
});
