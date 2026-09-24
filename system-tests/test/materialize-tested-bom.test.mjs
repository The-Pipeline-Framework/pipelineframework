import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {mkdtemp, readFile, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import test from 'node:test';
import {promisify} from 'node:util';

const execFileAsync = promisify(execFile);
const materializer = new URL('../scripts/materialize-tested-bom.mjs', import.meta.url).pathname;
const sha = 'abcdef1234567890abcdef1234567890abcdef12';

const sourcePom = `<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.pipelineframework</groupId>
    <artifactId>framework-parent</artifactId>
    <version>26.9.4-SNAPSHOT</version>
    <relativePath>../pom.xml</relativePath>
  </parent>
  <artifactId>pipelineframework-bom</artifactId>
  <packaging>pom</packaging>
  <build><plugins /></build>
  <dependencyManagement><dependencies><dependency>
    <groupId>org.pipelineframework</groupId>
    <artifactId>pipelineframework</artifactId>
    <version>\${pipelineframework.runtime.version}</version>
  </dependency></dependencies></dependencyManagement>
</project>
`;

function resolvedSet() {
  const components = Object.fromEntries(
    ['contracts', 'compiler', 'runtime', 'connectors', 'blocks', 'expansions']
      .map((component) => [component, {
        repository: `The-Pipeline-Framework/pipelineframework-${component}`,
        sha,
        mavenVersion: '26.9.4-main.abcdef123456',
        manifestDigest: `sha256:${'a'.repeat(64)}`
      }])
  );
  return {schemaVersion: 1, baselineDigest: `sha256:${'b'.repeat(64)}`, baselineRevision: 1, components, testHarnesses: {}, images: {}, candidates: []};
}

test('materializes one deterministic unpublished BOM over exact component versions', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'tpf-tested-bom-'));
  const source = join(directory, 'pom.xml');
  const resolved = join(directory, 'resolved.json');
  const repository = join(directory, 'repository');
  const output = join(directory, 'output.json');
  await writeFile(source, sourcePom);
  await writeFile(resolved, JSON.stringify(resolvedSet()));

  const run = () => execFileAsync(process.execPath, [materializer,
    '--source', source,
    '--resolvedSet', resolved,
    '--repository', repository,
    '--coordinationSha', sha,
    '--outputResolvedSet', output
  ]);
  await run();
  const first = JSON.parse(await readFile(output, 'utf8'));
  const version = first.coordinationBom.mavenVersion;
  assert.match(version, /^26\.9\.4-system-test\.abcdef123456\.[0-9a-f]{12}$/);
  const pomPath = join(repository, 'org', 'pipelineframework', 'pipelineframework-bom', version, `pipelineframework-bom-${version}.pom`);
  const firstPom = await readFile(pomPath, 'utf8');
  assert.match(firstPom, new RegExp(`<version>${version.replaceAll('.', '\\.')}<\\/version>`));
  assert.match(firstPom, /<pipelineframework\.runtime\.version>26\.9\.4-main\.abcdef123456<\/pipelineframework\.runtime\.version>/);
  assert.doesNotMatch(firstPom, /SNAPSHOT|<parent>|<build>|<relativePath>/);

  await run();
  const second = JSON.parse(await readFile(output, 'utf8'));
  assert.equal(second.coordinationBom.mavenVersion, version);
  assert.equal(await readFile(pomPath, 'utf8'), firstPom);
});
