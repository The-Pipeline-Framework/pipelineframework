import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';

const SHA_PATTERN = /^[0-9a-f]{40}$/;
const SHA256_PATTERN = /^[0-9a-f]{64}$/;
const DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/;
const CANDIDATE_VERSION_PATTERN = /^(\d+)\.(\d+)\.(\d+)-(?:pr\.([1-9][0-9]*)|main)\.([0-9a-f]{12})$/;
const SET_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const MAVEN_COMPONENTS = ['contracts', 'compiler', 'runtime', 'connectors', 'blocks', 'expansions'];
const SOURCE_COMPONENTS = ['examples', 'references', 'csvPayments', 'ragTurnkey'];

export async function readJson(path) {
  return JSON.parse(await readFile(path, 'utf8'));
}

export async function sha256File(path) {
  return createHash('sha256').update(await readFile(path)).digest('hex');
}

export function sha256Json(value) {
  return createHash('sha256').update(`${canonicalJson(value)}\n`).digest('hex');
}

export function canonicalJson(value) {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function fail(message) {
  throw new Error(message);
}

function object(value, name) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    fail(`${name} must be an object`);
  }
  return value;
}

function nonBlank(value, name) {
  if (typeof value !== 'string' || value.trim() !== value || value.length === 0) {
    fail(`${name} must be a non-blank, already-normalized string`);
  }
  return value;
}

function integer(value, name, minimum = 1) {
  if (!Number.isSafeInteger(value) || value < minimum) {
    fail(`${name} must be an integer >= ${minimum}`);
  }
  return value;
}

function exactKeys(value, allowed, required, name) {
  const keys = Object.keys(object(value, name));
  const extras = keys.filter((key) => !allowed.includes(key));
  const missing = required.filter((key) => !keys.includes(key));
  if (extras.length > 0) fail(`${name} contains unsupported properties: ${extras.join(', ')}`);
  if (missing.length > 0) fail(`${name} is missing required properties: ${missing.join(', ')}`);
}

function unique(values, name) {
  const duplicates = values.filter((value, index) => values.indexOf(value) !== index);
  if (duplicates.length > 0) fail(`${name} contains duplicates: ${[...new Set(duplicates)].join(', ')}`);
}

function requireSha(value, name) {
  if (typeof value !== 'string' || !SHA_PATTERN.test(value)) fail(`${name} must be a 40-character lowercase Git SHA`);
}

function requireDigest(value, name) {
  if (typeof value !== 'string' || !DIGEST_PATTERN.test(value)) fail(`${name} must be an immutable sha256 digest`);
}

function requireSha256(value, name) {
  if (typeof value !== 'string' || !SHA256_PATTERN.test(value)) fail(`${name} must be a lowercase SHA-256 value`);
}

export function parseCandidateVersion(value, name = 'candidate version') {
  const match = typeof value === 'string' ? CANDIDATE_VERSION_PATTERN.exec(value) : null;
  if (!match) fail(`${name} does not match <semver>-pr.<number>.<sha12> or <semver>-main.<sha12>`);
  return {
    baseVersion: `${match[1]}.${match[2]}.${match[3]}`,
    kind: match[4] === undefined ? 'main' : 'pr',
    pullRequestNumber: match[4] === undefined ? null : Number(match[4]),
    shortSha: match[5]
  };
}

export function validateComponentsConfig(config) {
  exactKeys(
    config,
    ['schemaVersion', 'organisation', 'coordinationRepository', 'statusContext', 'baselineImage', 'candidateManifestImage', 'coordinationConsumerVersionProperties', 'components'],
    ['schemaVersion', 'organisation', 'coordinationRepository', 'statusContext', 'baselineImage', 'candidateManifestImage', 'coordinationConsumerVersionProperties', 'components'],
    'components config'
  );
  if (config.schemaVersion !== 1) fail('components config schemaVersion must be 1');
  nonBlank(config.organisation, 'components config organisation');
  nonBlank(config.coordinationRepository, 'components config coordinationRepository');
  nonBlank(config.statusContext, 'components config statusContext');
  if (!/^ghcr\.io\/[a-z0-9._/-]+$/.test(config.baselineImage)) fail('baselineImage must be a lowercase GHCR repository');
  if (!/^ghcr\.io\/[a-z0-9._/-]+$/.test(config.candidateManifestImage)) fail('candidateManifestImage must be a lowercase GHCR repository');
  const components = object(config.components, 'components config components');
  const expected = [...MAVEN_COMPONENTS, ...SOURCE_COMPONENTS];
  exactKeys(components, expected, expected, 'components config components');
  validateVersionProperties(config.coordinationConsumerVersionProperties, expected, 'coordination consumer version properties');
  const repositories = [];
  for (const [name, component] of Object.entries(components)) {
    const allowed = ['repository', 'kind', 'allowedCoordinates', 'consumerVersionProperties', 'suiteManifest'];
    exactKeys(component, allowed, ['repository', 'kind', 'consumerVersionProperties', 'suiteManifest'], `component ${name}`);
    nonBlank(component.repository, `component ${name}.repository`);
    repositories.push(component.repository);
    if (component.kind !== (MAVEN_COMPONENTS.includes(name) ? 'maven' : 'source')) {
      fail(`component ${name}.kind does not match its architectural role`);
    }
    nonBlank(component.suiteManifest, `component ${name}.suiteManifest`);
    validateVersionProperties(component.consumerVersionProperties, expected, `component ${name}.consumerVersionProperties`);
    if (component.kind === 'maven') {
      if (!Array.isArray(component.allowedCoordinates) || component.allowedCoordinates.length === 0) {
        fail(`component ${name}.allowedCoordinates must be a non-empty array`);
      }
      component.allowedCoordinates.forEach((coordinate, index) => {
        nonBlank(coordinate, `component ${name}.allowedCoordinates[${index}]`);
        if (!/^[a-zA-Z0-9_.-]+:[a-zA-Z0-9_.-]+:(?:jar|pom|maven-plugin)$/.test(coordinate)) {
          fail(`component ${name}.allowedCoordinates[${index}] is invalid`);
        }
      });
      unique(component.allowedCoordinates, `component ${name}.allowedCoordinates`);
    } else if (component.allowedCoordinates !== undefined) {
      fail(`source component ${name} cannot declare Maven coordinates`);
    }
  }
  unique(repositories, 'component repositories');
  return config;
}

function validateVersionProperties(properties, expectedComponents, name) {
  object(properties, name);
  for (const [component, property] of Object.entries(properties)) {
    if (!expectedComponents.includes(component) && component !== 'coordinationBom') fail(`${name} references unknown component ${component}`);
    if (!MAVEN_COMPONENTS.includes(component) && component !== 'coordinationBom') fail(`${name} references non-Maven component ${component}`);
    if (typeof property !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(property)) fail(`${name}.${component} is invalid`);
  }
}

export function componentForRepository(config, repository) {
  const matches = Object.entries(config.components).filter(([, value]) => value.repository === repository);
  if (matches.length !== 1) fail(`repository is not an allowed component owner: ${repository}`);
  return matches[0][0];
}

export function validateCandidateEvent(event, config) {
  const properties = [
    'schema_version',
    'source_repository',
    'source_sha',
    'pull_request_number',
    'component',
    'candidate_version',
    'publication_run_id',
    'manifest_sha256',
    'compatibility_set_id'
  ];
  exactKeys(event, properties, properties, 'candidate event');
  if (properties.length > 10) fail('candidate event exceeds GitHub repository_dispatch top-level property limit');
  if (event.schema_version !== 1) fail('candidate event schema_version must be 1');
  requireSha(event.source_sha, 'candidate event source_sha');
  integer(event.publication_run_id, 'candidate event publication_run_id');
  requireSha256(event.manifest_sha256, 'candidate event manifest_sha256');
  const expectedComponent = componentForRepository(config, event.source_repository);
  if (event.component !== expectedComponent) fail(`candidate event component must be ${expectedComponent}`);
  if (event.compatibility_set_id !== null && (typeof event.compatibility_set_id !== 'string' || !SET_ID_PATTERN.test(event.compatibility_set_id))) {
    fail('candidate event compatibility_set_id is invalid');
  }
  const version = parseCandidateVersion(event.candidate_version, 'candidate event candidate_version');
  if (!event.source_sha.startsWith(version.shortSha)) fail('candidate version short SHA does not match source_sha');
  if (event.pull_request_number === null) {
    if (version.kind !== 'main') fail('main candidate events require a main candidate version');
  } else {
    integer(event.pull_request_number, 'candidate event pull_request_number');
    if (version.kind !== 'pr' || version.pullRequestNumber !== event.pull_request_number) {
      fail('PR candidate version does not match pull_request_number');
    }
  }
  return event;
}

export function validateCandidateManifest(manifest, config) {
  exactKeys(
    manifest,
    ['schemaVersion', 'repository', 'component', 'sourceSha', 'pullRequestNumber', 'candidateVersion', 'suiteHints', 'provenance', 'mavenArtifacts', 'images'],
    ['schemaVersion', 'repository', 'component', 'sourceSha', 'pullRequestNumber', 'candidateVersion', 'provenance', 'mavenArtifacts', 'images'],
    'candidate manifest'
  );
  if (manifest.schemaVersion !== 1) fail('candidate manifest schemaVersion must be 1');
  const component = componentForRepository(config, manifest.repository);
  if (manifest.component !== component) fail(`candidate manifest component must be ${component}`);
  requireSha(manifest.sourceSha, 'candidate manifest sourceSha');
  const version = parseCandidateVersion(manifest.candidateVersion, 'candidate manifest candidateVersion');
  if (!manifest.sourceSha.startsWith(version.shortSha)) fail('candidate manifest version short SHA does not match sourceSha');
  if (manifest.pullRequestNumber === null) {
    if (version.kind !== 'main') fail('main candidate manifest requires a main candidate version');
  } else {
    integer(manifest.pullRequestNumber, 'candidate manifest pullRequestNumber');
    if (version.kind !== 'pr' || version.pullRequestNumber !== manifest.pullRequestNumber) {
      fail('candidate manifest version does not match pullRequestNumber');
    }
  }
  exactKeys(manifest.provenance, ['build', 'publication'], ['build', 'publication'], 'candidate manifest provenance');
  validateWorkflowProvenance(manifest.provenance.build, manifest.repository, 'candidate manifest provenance.build');
  validateWorkflowProvenance(manifest.provenance.publication, manifest.repository, 'candidate manifest provenance.publication');
  const expectedBuildEvent = manifest.pullRequestNumber === null ? 'push' : 'pull_request';
  if (manifest.provenance.build.event !== expectedBuildEvent) fail(`candidate build provenance event must be ${expectedBuildEvent}`);
  if (manifest.provenance.build.workflowPath !== '.github/workflows/tpf-candidate-build.yml') fail('candidate build did not originate from the trusted build workflow');
  if (manifest.provenance.publication.event !== 'workflow_run') fail('candidate publisher provenance event must be workflow_run');
  if (manifest.provenance.publication.workflowPath !== '.github/workflows/tpf-candidate-publish.yml') fail('candidate did not originate from the trusted publisher workflow');
  const suiteHints = manifest.suiteHints ?? [];
  if (!Array.isArray(suiteHints)) fail('candidate manifest suiteHints must be an array');
  suiteHints.forEach((hint, index) => nonBlank(hint, `candidate manifest suiteHints[${index}]`));
  unique(suiteHints, 'candidate manifest suiteHints');
  if (!Array.isArray(manifest.mavenArtifacts)) fail('candidate manifest mavenArtifacts must be an array');
  if (!Array.isArray(manifest.images)) fail('candidate manifest images must be an array');
  const componentConfig = config.components[component];
  if (componentConfig.kind === 'maven' && manifest.mavenArtifacts.length === 0) fail('Maven candidate manifest must contain artifacts');
  if (componentConfig.kind === 'source' && manifest.mavenArtifacts.length !== 0) fail('source-only candidate manifest cannot contain Maven artifacts');
  const coordinates = [];
  for (const [index, artifact] of manifest.mavenArtifacts.entries()) {
    exactKeys(artifact, ['groupId', 'artifactId', 'version', 'packaging', 'files'], ['groupId', 'artifactId', 'version', 'packaging', 'files'], `mavenArtifacts[${index}]`);
    nonBlank(artifact.artifactId, `mavenArtifacts[${index}].artifactId`);
    if (artifact.version !== manifest.candidateVersion) fail(`mavenArtifacts[${index}].version must equal candidateVersion`);
    if (!['jar', 'pom', 'maven-plugin'].includes(artifact.packaging)) fail(`mavenArtifacts[${index}].packaging is unsupported`);
    const ownedCoordinate = `${artifact.groupId}:${artifact.artifactId}:${artifact.packaging}`;
    if (!componentConfig.allowedCoordinates.includes(ownedCoordinate)) {
      fail(`mavenArtifacts[${index}] coordinate is not owned by ${component}: ${ownedCoordinate}`);
    }
    if (!Array.isArray(artifact.files) || artifact.files.length === 0) fail(`mavenArtifacts[${index}].files must be non-empty`);
    const fileNames = [];
    for (const [fileIndex, file] of artifact.files.entries()) {
      exactKeys(file, ['name', 'sha256'], ['name', 'sha256'], `mavenArtifacts[${index}].files[${fileIndex}]`);
      const name = nonBlank(file.name, `mavenArtifacts[${index}].files[${fileIndex}].name`);
      if (name.includes('/') || name.includes('\\') || name === '.' || name === '..') fail('artifact file names must be basenames');
      requireSha256(file.sha256, `mavenArtifacts[${index}].files[${fileIndex}].sha256`);
      fileNames.push(name);
    }
    unique(fileNames, `mavenArtifacts[${index}] file names`);
    coordinates.push(`${artifact.groupId}:${artifact.artifactId}:${artifact.packaging}`);
  }
  unique(coordinates, 'candidate Maven coordinates');
  if (componentConfig.kind === 'maven') {
    const missing = componentConfig.allowedCoordinates.filter((coordinate) => !coordinates.includes(coordinate));
    if (missing.length > 0) fail(`candidate manifest omits required ${component} coordinates: ${missing.join(', ')}`);
  }
  for (const [index, image] of manifest.images.entries()) {
    exactKeys(image, ['repository', 'digest'], ['repository', 'digest'], `images[${index}]`);
    if (!/^ghcr\.io\/[a-z0-9._/-]+$/.test(image.repository)) fail(`images[${index}].repository must be a lowercase GHCR repository`);
    requireDigest(image.digest, `images[${index}].digest`);
  }
  unique(manifest.images.map((image) => image.repository), 'candidate image repositories');
  return manifest;
}

function validateWorkflowProvenance(provenance, repository, name) {
  exactKeys(provenance, ['repository', 'runId', 'runAttempt', 'workflowPath', 'event'], ['repository', 'runId', 'runAttempt', 'workflowPath', 'event'], name);
  if (provenance.repository !== repository) fail(`${name}.repository must equal candidate repository`);
  integer(provenance.runId, `${name}.runId`);
  integer(provenance.runAttempt, `${name}.runAttempt`);
  if (!/^\.github\/workflows\/[A-Za-z0-9._-]+\.ya?ml$/.test(provenance.workflowPath)) fail(`${name}.workflowPath is invalid`);
  if (!['pull_request', 'push', 'workflow_run'].includes(provenance.event)) fail(`${name}.event is invalid`);
}

export function validateEventAgainstManifest(event, manifest, manifestSha256) {
  const comparisons = [
    ['source_repository', 'repository'],
    ['source_sha', 'sourceSha'],
    ['pull_request_number', 'pullRequestNumber'],
    ['component', 'component'],
    ['candidate_version', 'candidateVersion'],
    ['publication_run_id', ['provenance', 'publication', 'runId']]
  ];
  for (const [eventKey, manifestPath] of comparisons) {
    const manifestValue = Array.isArray(manifestPath)
      ? manifestPath.reduce((value, key) => value[key], manifest)
      : manifest[manifestPath];
    if (event[eventKey] !== manifestValue) fail(`candidate event ${eventKey} does not match manifest`);
  }
  if (event.manifest_sha256 !== manifestSha256) fail('candidate manifest checksum does not match dispatch event');
}

export function validateGitHubProvenance(event, manifest, sourceRepository, publicationRun, buildRun, pullRequest) {
  object(sourceRepository, 'source repository');
  const sourceRepositoryName = nonBlank(sourceRepository.full_name, 'source repository full name');
  const defaultBranch = nonBlank(sourceRepository.default_branch, 'source repository default branch');
  if (sourceRepositoryName !== event.source_repository) fail('source repository metadata does not match event');
  validateRunAgainstManifest(publicationRun, manifest.provenance.publication, 'publication');
  validateRunAgainstManifest(buildRun, manifest.provenance.build, 'build');
  if (publicationRun.id !== event.publication_run_id) fail('publication workflow run ID does not match event');
  if (publicationRun.path !== '.github/workflows/tpf-candidate-publish.yml') fail('candidate did not originate from the trusted publisher workflow');
  if (publicationRun.event !== 'workflow_run') fail('candidate publisher must be triggered by workflow_run');
  if (publicationRun.head_branch !== defaultBranch) fail('candidate publisher did not run from the repository default branch');
  if (buildRun.path !== '.github/workflows/tpf-candidate-build.yml') fail('candidate did not originate from the trusted build workflow');
  if (event.pull_request_number !== null) {
    object(pullRequest, 'pull request');
    if (pullRequest.number !== event.pull_request_number) fail('pull request number does not match event');
    if (pullRequest.head?.sha !== event.source_sha) fail('candidate source SHA is stale relative to the current pull request head');
    if (pullRequest.base?.repo?.full_name !== event.source_repository) fail('pull request base repository does not match event');
    if (buildRun.event !== 'pull_request') fail('PR candidate build must be triggered by pull_request');
    const associatedPullRequest = buildRun.pull_requests?.find((candidate) => candidate.number === event.pull_request_number);
    if (associatedPullRequest === undefined) fail('build workflow run is not associated with the candidate pull request');
    if (associatedPullRequest.head?.sha !== event.source_sha) fail('build workflow run pull-request head does not match candidate source SHA');
  } else {
    if (buildRun.event !== 'push') fail('main candidate build must be triggered by push');
    if (buildRun.head_sha !== event.source_sha) fail('main candidate build head SHA does not match event');
    if (buildRun.head_branch !== defaultBranch) fail('main candidate build did not run from the repository default branch');
  }
}

function validateRunAgainstManifest(run, provenance, label) {
  object(run, `${label} workflow run`);
  if (run.id !== provenance.runId) fail(`${label} workflow run ID does not match manifest`);
  if (run.run_attempt !== provenance.runAttempt) fail(`${label} workflow run attempt does not match manifest`);
  if (run.path !== provenance.workflowPath) fail(`${label} workflow path does not match manifest`);
  if (run.event !== provenance.event) fail(`${label} workflow event does not match manifest`);
  if (run.status !== 'completed' || run.conclusion !== 'success') fail(`${label} workflow run is not successful and complete`);
  const workflowRepository = run.repository?.full_name ?? run.head_repository?.full_name;
  if (workflowRepository !== provenance.repository) fail(`${label} workflow run repository does not match manifest`);
}

function immutableMavenVersion(value, name) {
  nonBlank(value, name);
  if (/SNAPSHOT|LATEST|RELEASE/i.test(value) || /[\[\]()]/.test(value)) fail(`${name} is floating`);
}

function validateSourceEntry(entry, expectedRepository, name) {
  exactKeys(entry, ['repository', 'sha'], ['repository', 'sha'], name);
  if (entry.repository !== expectedRepository) fail(`${name}.repository must be ${expectedRepository}`);
  requireSha(entry.sha, `${name}.sha`);
}

export function validateBaseline(baseline, config) {
  exactKeys(baseline, ['schemaVersion', 'revision', 'generatedAt', 'components', 'testHarnesses', 'images'], ['schemaVersion', 'revision', 'generatedAt', 'components', 'testHarnesses', 'images'], 'baseline');
  if (baseline.schemaVersion !== 1) fail('baseline schemaVersion must be 1');
  integer(baseline.revision, 'baseline revision');
  if (typeof baseline.generatedAt !== 'string' || Number.isNaN(Date.parse(baseline.generatedAt))) fail('baseline generatedAt must be an ISO date-time');
  exactKeys(baseline.components, MAVEN_COMPONENTS, MAVEN_COMPONENTS, 'baseline components');
  exactKeys(baseline.testHarnesses, SOURCE_COMPONENTS, SOURCE_COMPONENTS, 'baseline testHarnesses');
  for (const name of MAVEN_COMPONENTS) {
    const entry = baseline.components[name];
    exactKeys(entry, ['repository', 'sha', 'mavenVersion', 'manifestDigest'], ['repository', 'sha', 'mavenVersion', 'manifestDigest'], `baseline component ${name}`);
    if (entry.repository !== config.components[name].repository) fail(`baseline component ${name}.repository is invalid`);
    requireSha(entry.sha, `baseline component ${name}.sha`);
    immutableMavenVersion(entry.mavenVersion, `baseline component ${name}.mavenVersion`);
    requireDigest(entry.manifestDigest, `baseline component ${name}.manifestDigest`);
  }
  for (const name of SOURCE_COMPONENTS) {
    validateSourceEntry(baseline.testHarnesses[name], config.components[name].repository, `baseline test harness ${name}`);
  }
  object(baseline.images, 'baseline images');
  for (const [repository, digest] of Object.entries(baseline.images)) {
    if (!/^ghcr\.io\/[a-z0-9._/-]+$/.test(repository)) fail(`baseline image repository is invalid: ${repository}`);
    requireDigest(digest, `baseline image ${repository}`);
  }
  return baseline;
}

export function validatePolicy(policy, config) {
  exactKeys(policy, ['schemaVersion', 'shards', 'suites', 'componentPolicy', 'pathRules', 'fullTrain'], ['schemaVersion', 'shards', 'suites', 'componentPolicy', 'pathRules', 'fullTrain'], 'suite policy');
  if (policy.schemaVersion !== 2) fail('suite policy schemaVersion must be 2');
  object(policy.shards, 'suite policy shards');
  object(policy.suites, 'suite policy suites');
  object(policy.componentPolicy, 'suite policy componentPolicy');
  if (!Array.isArray(policy.pathRules)) fail('suite policy pathRules must be an array');
  if (!Array.isArray(policy.fullTrain)) fail('suite policy fullTrain must be an array');
  for (const [name, shard] of Object.entries(policy.shards)) {
    exactKeys(shard, ['displayName', 'timeoutMinutes'], ['displayName', 'timeoutMinutes'], `shard ${name}`);
    nonBlank(shard.displayName, `shard ${name}.displayName`);
    if (!Number.isSafeInteger(shard.timeoutMinutes) || shard.timeoutMinutes < 1 || shard.timeoutMinutes > 360) {
      fail(`shard ${name}.timeoutMinutes must be between 1 and 360`);
    }
  }
  for (const [name, suite] of Object.entries(policy.suites)) {
    exactKeys(suite, ['owner', 'entrypoint', 'tier', 'shard'], ['owner', 'entrypoint', 'tier', 'shard'], `suite ${name}`);
    if (suite.owner !== 'coordination' && config.components[suite.owner] === undefined) fail(`suite ${name} has unknown owner`);
    nonBlank(suite.entrypoint, `suite ${name}.entrypoint`);
    if (!['pr', 'post-merge', 'heavy'].includes(suite.tier)) fail(`suite ${name}.tier is invalid`);
    if (policy.shards[suite.shard] === undefined) fail(`suite ${name} references unknown shard ${suite.shard}`);
  }
  exactKeys(policy.componentPolicy, Object.keys(config.components), Object.keys(config.components), 'suite policy componentPolicy');
  for (const [component, componentPolicy] of Object.entries(policy.componentPolicy)) {
    exactKeys(componentPolicy, ['required', 'postMerge', 'heavy'], ['required', 'postMerge', 'heavy'], `component policy ${component}`);
    for (const key of ['required', 'postMerge', 'heavy']) {
      if (!Array.isArray(componentPolicy[key])) fail(`component policy ${component}.${key} must be an array`);
      unique(componentPolicy[key], `component policy ${component}.${key}`);
      for (const suite of componentPolicy[key]) {
        if (policy.suites[suite] === undefined) fail(`component policy ${component}.${key} references unknown suite ${suite}`);
        const expectedTier = key === 'required' ? 'pr' : key === 'postMerge' ? 'post-merge' : 'heavy';
        if (policy.suites[suite].tier !== expectedTier) fail(`component policy ${component}.${key} references a suite in the wrong tier`);
      }
    }
  }
  for (const [index, rule] of policy.pathRules.entries()) {
    exactKeys(rule, ['component', 'paths', 'required'], ['component', 'paths', 'required'], `path rule ${index}`);
    if (config.components[rule.component] === undefined) fail(`path rule ${index} has unknown component`);
    if (!Array.isArray(rule.paths) || rule.paths.length === 0) fail(`path rule ${index}.paths must be a non-empty array`);
    if (!Array.isArray(rule.required)) fail(`path rule ${index}.required must be an array`);
    unique(rule.paths, `path rule ${index}.paths`);
    unique(rule.required, `path rule ${index}.required`);
    for (const path of rule.paths) nonBlank(path, `path rule ${index}.paths entry`);
    for (const suite of rule.required) {
      if (policy.suites[suite] === undefined) fail(`path rule ${index} references unknown suite ${suite}`);
      if (policy.suites[suite].tier !== 'pr') fail(`path rule ${index} references a non-PR suite`);
    }
  }
  unique(policy.fullTrain, 'suite policy fullTrain');
  for (const suite of policy.fullTrain) {
    if (policy.suites[suite] === undefined) fail(`fullTrain references unknown suite ${suite}`);
  }
  return policy;
}

function globToRegExp(glob) {
  const escaped = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&').replaceAll('**', '\u0000').replaceAll('*', '[^/]*').replaceAll('\u0000', '.*');
  return new RegExp(`^${escaped}$`);
}

function matchingPathRule(component, changedPaths, pathRules) {
  if (!Array.isArray(changedPaths) || changedPaths.length === 0) return null;
  return pathRules.find((rule) => {
    if (rule.component !== component || !Array.isArray(rule.paths) || !Array.isArray(rule.required)) return false;
    return changedPaths.every((path) => rule.paths.some((glob) => globToRegExp(glob).test(path)));
  }) ?? null;
}

export function selectSuites(component, policy, changedPaths = [], publisherHints = []) {
  const componentPolicy = policy.componentPolicy[component];
  if (componentPolicy === undefined) fail(`no suite policy exists for component ${component}`);
  const rule = matchingPathRule(component, changedPaths, policy.pathRules);
  const required = new Set(rule === null ? componentPolicy.required : rule.required);
  for (const hint of publisherHints) {
    if (policy.suites[hint] === undefined) fail(`publisher hint references unknown suite ${hint}`);
    required.add(hint);
  }
  return [...required].sort();
}

export function selectPostMergeSuites(component, policy) {
  const componentPolicy = policy.componentPolicy[component];
  if (componentPolicy === undefined) fail(`no suite policy exists for component ${component}`);
  return [...componentPolicy.postMerge].sort();
}

export function overlayBaseline(baseline, baselineDigest, candidates, candidateDigests, config) {
  validateBaseline(baseline, config);
  requireDigest(baselineDigest, 'baseline digest');
  if (!Array.isArray(candidates) || candidates.length === 0) fail('at least one candidate is required');
  if (!Array.isArray(candidateDigests) || candidateDigests.length !== candidates.length) fail('candidate digests must align with candidates');
  const result = structuredClone(baseline);
  const seen = new Set();
  const overlays = [];
  candidates.forEach((candidate, index) => {
    validateCandidateManifest(candidate, config);
    if (seen.has(candidate.component)) fail(`compatibility set contains more than one ${candidate.component} candidate`);
    seen.add(candidate.component);
    const digest = candidateDigests[index].startsWith('sha256:') ? candidateDigests[index] : `sha256:${candidateDigests[index]}`;
    requireDigest(digest, `candidate ${candidate.component} manifest digest`);
    if (config.components[candidate.component].kind === 'maven') {
      result.components[candidate.component] = {
        repository: candidate.repository,
        sha: candidate.sourceSha,
        mavenVersion: candidate.candidateVersion,
        manifestDigest: digest
      };
    } else {
      result.testHarnesses[candidate.component] = {
        repository: candidate.repository,
        sha: candidate.sourceSha
      };
    }
    for (const image of candidate.images) result.images[image.repository] = image.digest;
    overlays.push({component: candidate.component, repository: candidate.repository, sha: candidate.sourceSha, candidateVersion: candidate.candidateVersion, manifestDigest: digest, suiteHints: [...(candidate.suiteHints ?? [])]});
  });
  validateBaseline(result, config);
  return {
    schemaVersion: 1,
    baselineDigest,
    baselineRevision: baseline.revision,
    components: result.components,
    testHarnesses: result.testHarnesses,
    images: result.images,
    candidates: overlays.sort((left, right) => left.component.localeCompare(right.component))
  };
}

export function suiteMatrix(selectedSuites, policy, resolvedSet, config) {
  return selectedSuites.map((name) => {
    const suite = policy.suites[name];
    if (suite === undefined) fail(`selected unknown suite ${name}`);
    if (suite.owner === 'coordination') {
      return {suite: name, owner: 'coordination', repository: config.coordinationRepository, sha: null, manifest: null, entrypoint: suite.entrypoint, tier: suite.tier, versionProperties: config.coordinationConsumerVersionProperties};
    }
    const component = config.components[suite.owner];
    const pin = component.kind === 'maven' ? resolvedSet.components[suite.owner] : resolvedSet.testHarnesses[suite.owner];
    return {suite: name, owner: suite.owner, repository: component.repository, sha: pin.sha, manifest: component.suiteManifest, entrypoint: suite.entrypoint, tier: suite.tier, versionProperties: component.consumerVersionProperties};
  });
}

export function shardMatrix(selectedSuites, policy, resolvedSet, config) {
  const shards = new Map();
  for (const suite of suiteMatrix(selectedSuites, policy, resolvedSet, config)) {
    const shardName = policy.suites[suite.suite].shard;
    const definition = policy.shards[shardName];
    const shard = shards.get(shardName) ?? {
      shard: shardName,
      displayName: definition.displayName,
      timeoutMinutes: definition.timeoutMinutes,
      suites: []
    };
    shard.suites.push(suite);
    shards.set(shardName, shard);
  }
  return [...shards.values()]
    .map((shard) => ({...shard, suites: shard.suites.sort((left, right) => left.suite.localeCompare(right.suite))}))
    .sort((left, right) => left.shard.localeCompare(right.shard));
}

export function nextBaseline(currentBaseline, resolvedSet, generatedAt) {
  if (resolvedSet.baselineRevision !== currentBaseline.revision) fail('resolved set was not tested against the current baseline revision');
  const next = {
    schemaVersion: 1,
    revision: currentBaseline.revision + 1,
    generatedAt,
    components: structuredClone(resolvedSet.components),
    testHarnesses: structuredClone(resolvedSet.testHarnesses),
    images: structuredClone(resolvedSet.images)
  };
  if (Number.isNaN(Date.parse(generatedAt))) fail('promotion generatedAt must be an ISO date-time');
  return next;
}

export const componentKinds = Object.freeze({
  maven: Object.freeze([...MAVEN_COMPONENTS]),
  source: Object.freeze([...SOURCE_COMPONENTS])
});
