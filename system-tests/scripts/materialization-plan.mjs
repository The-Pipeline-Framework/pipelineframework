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
  const manifest = validateCandidateManifest(await readJson(manifestPath), config);
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
