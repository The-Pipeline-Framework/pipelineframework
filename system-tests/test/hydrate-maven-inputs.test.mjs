import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {mkdtemp, readFile, rm, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {fileURLToPath} from 'node:url';

const script = (name) => fileURLToPath(new URL(`../scripts/${name}.mjs`, import.meta.url));

test('hydrates the attested POM and JAR without following the POM dependency graph', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'tpf-hydration-'));
  t.after(() => rm(root, {recursive: true, force: true}));
  const groupId = 'org.pipelineframework';
  const artifactId = 'sample';
  const version = '1.2.3-main.123456789abc';
  const repository = join(root, 'repository');
  const plan = join(root, 'plan.json');
  const location = `org/pipelineframework/sample/${version}`;
  const pom = '<project><dependencies><dependency><groupId>org.pipelineframework</groupId>'
    + '<artifactId>old</artifactId><version>1.0-SNAPSHOT</version></dependency></dependencies></project>';
  const responses = Object.fromEntries([['pom', pom], ['jar', 'jar fixture']].map(([type, bytes]) =>
    [`${artifactId}-${version}.${type}`, bytes]));
  const files = Object.entries(responses).map(([name, bytes]) =>
    ({name, sha256: createHash('sha256').update(bytes).digest('hex')}));
  await writeFile(plan, JSON.stringify({artifacts: [{
    groupId, artifactId, version, repository: 'The-Pipeline-Framework/sample', files
  }]}));
  const stub = `
    globalThis.fetch = async (url, options) => {
      const source = new URL(url);
      const responses = ${JSON.stringify(responses)};
      const name = source.pathname.split('/').at(-1);
      if (!Object.hasOwn(responses, name)) throw new Error('Unattested request: ' + url);
      if (source.href === 'https://maven.pkg.github.com/the-pipeline-framework/sample/${location}/' + name
          && options.headers.authorization === 'Basic Zml4dHVyZTp0b2tlbg==') {
        return Response.redirect('https://github-registry-files.githubusercontent.com/' + name);
      }
      if (source.origin === 'https://github-registry-files.githubusercontent.com' && !options.headers) {
        return new Response(responses[name]);
      }
      throw new Error('Unexpected origin or credentials');
    };
  `;
  const args = ['--plan', plan, '--repository', repository];
  execFileSync(process.execPath, ['--import', 'data:text/javascript,' + encodeURIComponent(stub),
    script('hydrate-maven-inputs'), ...args], {env: {...process.env, GITHUB_ACTOR: 'fixture', PACKAGE_TOKEN: 'token'}});
  execFileSync(process.execPath, [script('verify-materialized'), ...args]);
  assert.equal(await readFile(join(repository, location, `${artifactId}-${version}.pom`), 'utf8'), pom);
});
