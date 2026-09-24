import {createHash} from 'node:crypto';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import {dirname, join} from 'node:path';
import {parseArgs} from 'node:util';
import {canonicalJson, readJson} from './lib/contracts.mjs';

const MAVEN_COMPONENTS = ['contracts', 'compiler', 'runtime', 'connectors', 'blocks', 'expansions'];
const SHA_PATTERN = /^[0-9a-f]{40}$/;
const VERSION_PATTERN = /^(\d+\.\d+\.\d+)(?:$|[-.][A-Za-z0-9.-]+$)/;

const {values} = parseArgs({
  options: {
    source: {type: 'string'},
    resolvedSet: {type: 'string'},
    repository: {type: 'string'},
    coordinationSha: {type: 'string'},
    outputResolvedSet: {type: 'string'}
  },
  strict: true
});

for (const name of ['source', 'resolvedSet', 'repository', 'coordinationSha', 'outputResolvedSet']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
if (!SHA_PATTERN.test(values.coordinationSha)) throw new Error('--coordinationSha must be a 40-character lowercase Git SHA');

const resolvedSet = await readJson(values.resolvedSet);
const versions = Object.fromEntries(MAVEN_COMPONENTS.map((component) => {
  const version = resolvedSet.components?.[component]?.mavenVersion;
  if (typeof version !== 'string' || version.length === 0 || /SNAPSHOT/i.test(version)) {
    throw new Error(`resolved ${component} version must be immutable`);
  }
  const match = VERSION_PATTERN.exec(version);
  if (match === null) throw new Error(`resolved ${component} version has no semantic base version`);
  return [component, {version, baseVersion: match[1]}];
}));

const baseVersions = new Set(Object.values(versions).map(({baseVersion}) => baseVersion));
if (baseVersions.size !== 1) throw new Error('resolved Maven components do not share one product base version');

const sourcePom = await readFile(values.source, 'utf8');
const sourcePomSha256 = createHash('sha256').update(sourcePom).digest('hex');
const identity = canonicalJson({
  coordinationSha: values.coordinationSha,
  sourcePomSha256,
  components: Object.fromEntries(MAVEN_COMPONENTS.map((component) => [component, resolvedSet.components[component]]))
});
const identitySha256 = createHash('sha256').update(identity).digest('hex');
const baseVersion = [...baseVersions][0];
const mavenVersion = `${baseVersion}-system-test.${values.coordinationSha.slice(0, 12)}.${identitySha256.slice(0, 12)}`;

const parentPattern = /\n\s*<parent>[\s\S]*?<\/parent>\s*\n/;
const buildPattern = /\n\s*<build>[\s\S]*?<\/build>\s*\n/;
if ((sourcePom.match(new RegExp(parentPattern.source, 'g')) ?? []).length !== 1) {
  throw new Error('BOM source must contain exactly one parent declaration');
}
if ((sourcePom.match(new RegExp(buildPattern.source, 'g')) ?? []).length !== 1) {
  throw new Error('BOM source must contain exactly one build declaration');
}

const properties = MAVEN_COMPONENTS
  .map((component) => `        <pipelineframework.${component}.version>${versions[component].version}</pipelineframework.${component}.version>`)
  .join('\n');
const projectIdentity = `
    <groupId>org.pipelineframework</groupId>
    <version>${mavenVersion}</version>

    <properties>
${properties}
    </properties>
`;
const materializedPom = sourcePom
  .replace(parentPattern, projectIdentity)
  .replace(buildPattern, '\n');
if (/SNAPSHOT/i.test(materializedPom) || materializedPom.includes('<relativePath>')) {
  throw new Error('materialized BOM retains a floating or source-relative reference');
}

const artifactDirectory = join(values.repository, 'org', 'pipelineframework', 'pipelineframework-bom', mavenVersion);
const artifactPath = join(artifactDirectory, `pipelineframework-bom-${mavenVersion}.pom`);
await mkdir(artifactDirectory, {recursive: true});
await writeFile(artifactPath, materializedPom);

const output = {
  ...resolvedSet,
  coordinationBom: {
    repository: 'The-Pipeline-Framework/pipelineframework',
    sha: values.coordinationSha,
    groupId: 'org.pipelineframework',
    artifactId: 'pipelineframework-bom',
    packaging: 'pom',
    mavenVersion,
    sourcePomSha256
  }
};
await mkdir(dirname(values.outputResolvedSet), {recursive: true});
await writeFile(values.outputResolvedSet, `${canonicalJson(output)}\n`);
process.stdout.write(`Materialized org.pipelineframework:pipelineframework-bom:pom:${mavenVersion}\n`);
