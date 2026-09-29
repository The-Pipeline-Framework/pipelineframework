import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';

const workflow = await readFile(new URL('../../.github/workflows/system-test-candidate.yml', import.meta.url), 'utf8');

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
