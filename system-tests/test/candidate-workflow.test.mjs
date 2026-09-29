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
