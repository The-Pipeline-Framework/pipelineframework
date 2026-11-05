import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import test from 'node:test';

test('frozen rehearsal provenance, immutable overlay and coordinate verification', () => {
  execFileSync('python3', ['-B', '-m', 'unittest', 'discover', '-s', 'system-tests/test', '-p', 'test_rehearsal_inputs.py'], {
    cwd: new URL('../../', import.meta.url), stdio: 'pipe'
  });
});

test('rehearsals use complete train coverage and cannot promote the baseline', () => {
  const workflow = readFileSync(new URL('../../.github/workflows/system-test-full-train.yml', import.meta.url), 'utf8');
  assert.match(workflow, /inputs\.rehearsal_manifest != ''\) && inputs\.search_aws_only == true/);
  const promotion = workflow.slice(workflow.indexOf('\n  promote:'));
  assert.match(promotion, /inputs\.rehearsal_manifest == ''/);
  assert.match(workflow, /rehearsal-inputs\.py restore/);
  assert.match(workflow, /rehearsal-inputs\.py merge/);
});
