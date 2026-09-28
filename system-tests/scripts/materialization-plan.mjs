#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { readJson, validateCandidateManifest, validateComponentsConfig } from './lib/contracts.mjs';

const { values } = parseArgs({
  options: {
    components: {type: 'string'},
    manifests: {type: 'string', multiple: true},
    output: {type: 'string'}
  },
  strict: true
});
for (const name of ['components', 'manifests', 'output']) {
  if (values[name] === undefined) throw new Error(`--${name} is required`);
}
const config = validateComponentsConfig(await readJson(values.components));
const entries = [];
for (const manifestPath of values.manifests) {
  const rawManifest = await readJson(manifestPath);
  let manifest;
  try {
    // Resolved sets may contain immutable manifests promoted under an older
    // component coordinate contract. Intake validates new candidates against
    // the complete current contract; materialisation validates historical
    // manifests for identity, ownership, provenance and integrity instead.
    manifest = validateCandidateManifest(rawManifest, config, {requireAllOwnedCoordinates: false});
  } catch (error) {
    const identity = [rawManifest.component, rawManifest.repository, rawManifest.candidateVersion]
      .filter((value) => typeof value === 'string' && value.length > 0)
      .join(', ');
    throw new Error(
      `cannot materialize manifest ${manifestPath}${identity.length > 0 ? ` (${identity})` : ''}: ${error.message}`,
      {cause: error}
    );
  }
  for (const artifact of manifest.mavenArtifacts) {
    const extension = artifact.packaging === 'maven-plugin' ? 'jar' : artifact.packaging;
    const primaryName = `${artifact.artifactId}-${artifact.version}.${extension}`;
    const pomName = `${artifact.artifactId}-${artifact.version}.pom`;
    const primary = artifact.files.find((file) => file.name === primaryName);
    const pom = artifact.files.find((file) => file.name === pomName);
    if (primary === undefined) throw new Error(`manifest omits primary artifact ${primaryName}`);
    if (pom === undefined) throw new Error(`manifest omits POM ${pomName}`);
    const files = primaryName === pomName ? [primary] : [primary, pom];
    entries.push({
      component: manifest.component,
      repository: manifest.repository,
      coordinate: `${artifact.groupId}:${artifact.artifactId}:${artifact.version}:${artifact.packaging}`,
      groupId: artifact.groupId,
      artifactId: artifact.artifactId,
      version: artifact.version,
      packaging: artifact.packaging,
      files
    });
  }
}
entries.sort((left, right) => left.coordinate.localeCompare(right.coordinate));
await writeFile(values.output, `${JSON.stringify({schemaVersion: 1, artifacts: entries}, null, 2)}\n`);
