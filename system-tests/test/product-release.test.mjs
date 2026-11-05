import test from 'node:test';
import {execFileSync} from 'node:child_process';

test('independent release selection, publication evidence and freeze regressions', () => {
  execFileSync('python3', ['-B', '-m', 'unittest', 'discover', '-s', 'system-tests/test', '-p', 'test_product_release.py'], {
    cwd: new URL('../../', import.meta.url), stdio: 'pipe'
  });
});
