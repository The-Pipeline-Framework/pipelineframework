import {execFileSync} from 'node:child_process';
import test from 'node:test';

/** Run the Python coverage regressions as part of the Node plumbing suite. */
function verifyBomCoverageRegressions() {
  execFileSync('python3', ['-B', '-m', 'unittest', 'discover', '-s', 'system-tests/test', '-p', 'test_bom_coverage.py'], {
    cwd: new URL('../../', import.meta.url), stdio: 'pipe'
  });
}

test('BOM covers registered artifacts and rejects missing, stale or wrong-owner classifications', verifyBomCoverageRegressions);
