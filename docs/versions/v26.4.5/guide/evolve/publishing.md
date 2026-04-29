---
search: false
---

# Publishing to Maven Central

This document explains how to publish The Pipeline Framework to Maven Central and how to manage the project's versioning and release process properly.

## TL;DR: Guarded Release Process

The release process uses the Maven Release Plugin for the root reactor, then GitHub Actions publishes framework artifacts from the release tag. Keep Maven's push step disabled so the generated release and next-development commits can be inspected before they reach `main`.

1. **Prepare locally without pushing**:
   ```bash
   ./mvnw release:prepare -DpushChanges=false -Darguments="-DskipTests"
   ```
2. **Synchronize release-coupled standalone POMs**: confirm alternate topology and standalone reference POMs moved to the next snapshot, including `examples/csv-payments/pom.pipeline-runtime.xml`, `examples/csv-payments/pom.monolith.xml`, `examples/checkout/pom.xml`, and `ai-sdk/pom.xml`.
3. **Run the release validation gate**: at minimum run version-drift checks, framework verification, CSV topology checks, and docs build before pushing.
4. **Push only after validation**: push the prepared commits to `main`, then push the immutable `vX.Y.Z` tag to trigger publishing.
5. **Verify on Maven Central**: check artifacts at <https://central.sonatype.com/>.

The GitHub Actions workflow automatically:
- Runs the workflows selected by the pushed paths on `main`
- Signs all artifacts with GPG
- Deploys to Sonatype Central
- Creates a GitHub release with notes

## Table of Contents

- [Overview](#overview)
- [Version Management](#version-management)
- [Maven Central Publishing Setup](#maven-central-publishing-setup)
- [settings.xml Configuration](#local-settingsxml-configuration)
- [GitHub Actions Workflow](#github-actions-workflow)
- [Safe Release Process](#safe-release-process)
- [Troubleshooting](#troubleshooting)

## Overview

The Pipeline Framework is published to Maven Central to make it available to developers who want to use it in their projects. This document outlines the process, configuration, and best practices for publishing releases.

## Version Management

The framework release version is defined in the root POM (`pom.xml`) as the root reactor's `<version>`. Most root-reactor modules inherit that version through parent links, but several compatibility and reference surfaces are intentionally outside that reactor or use alternate top-level POMs.

Keep these categories aligned during every release:

1. **Root reactor**: root, framework, main examples, and plugins listed in the root `pom.xml`.
2. **Alternate topology POMs**: build-entry POMs such as `examples/csv-payments/pom.pipeline-runtime.xml` and `examples/csv-payments/pom.monolith.xml`.
3. **Standalone compatibility surfaces**: POMs such as `ai-sdk/pom.xml` and reference examples outside the root reactor.

### Version Property Definition

In the root POM (`pom.xml`):
```xml
<version>26.1-SNAPSHOT</version>
```

Root-reactor children should inherit this version through the parent relationship and omit their own project `<version>` where possible. Alternate top-level POMs and standalone builds may need an explicit parent version or dependency property, so they must be checked separately.

### Using Maven Versions Plugin

To update root-reactor versions consistently, use the Maven Versions Plugin:

```bash
# Update the version across all modules
mvn versions:set -DnewVersion=1.0.0

# Verify the changes before committing
mvn versions:commit

# Or rollback if needed
mvn versions:revert
```

This ensures that modules in the selected Maven reactor are updated consistently. It does not automatically update alternate POM entrypoints or standalone surfaces that are not part of that reactor.

### Documentation Snapshot (Hybrid Versioning)

For released docs versions, snapshot the docs into `docs/versions/`:

```bash
cd docs
npm run snapshot -- --version v0.9.3
```

Then update `docs/versions.md` to mark the latest version and confirm the version selector list.


## Maven Central Publishing Setup

The Maven Central publishing configuration is located in the framework's parent POM (`framework/pom.xml`). It is 
handled by the GitHub Actions workflow, but this is a description of how such setup could be done on your local 
workstation.

### Required Plugins

The following plugins are configured in the framework POM for Maven Central compliance:

1. **Source Plugin**: Generates sources JAR
2. **Javadoc Plugin**: Generates documentation JAR
3. **GPG Plugin**: Signs artifacts
4. **Central Publishing Plugin**: Deploys to Sonatype Central

For the complete configuration, see the central-publishing profile in `framework/pom.xml`.

### Local settings.xml Configuration

To authenticate with Sonatype Central and provide GPG credentials, you could configure your `~/.m2/settings.xml` file:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>central</id>
      <username>your-sonatype-username</username>
      <password>your-encrypted-sonatype-password</password>
    </server>
  </servers>
</settings>
```

### GPG Configuration

The publishing workflow handles GPG signing on GitHub Actions, but for reference, this is how you could configure it on your local machine:

```xml
<profiles>
  <profile>
    <id>central-publishing</id>
    <properties>
      <gpg.executable>gpg</gpg.executable>
      <gpg.passphrase>your-gpg-passphrase</gpg.passphrase>
      <gpg.keyname>your-gpg-key-id</gpg.keyname>
    </properties>
  </profile>
</profiles>

<activeProfiles>
  <activeProfile>central-publishing</activeProfile>
</activeProfiles>
```

### Encrypting Passwords

To encrypt your Sonatype password on your local `settings.xml`:

1. Create the master password:
   ```bash
   mvn --encrypt-master-password
   ```
   This creates `~/.m2/settings-security.xml`

2. Encrypt your Sonatype password:
   ```bash
   mvn --encrypt-password
   ```

3. Update settings.xml with the encrypted password (prefixed with `{` and suffixed with `}`).

## GitHub Actions Workflow

The publishing process is automated using GitHub Actions.

### Required GitHub Secrets

These secrets must exist in the GitHub repository:

1. `CENTRAL_USERNAME` - Your Sonatype username
2. `CENTRAL_PASSWORD` - Your Sonatype password
3. `GPG_PRIVATE_KEY` - Your GPG private key exported with `gpg --export-secret-keys --armor <your-key-id>`
4. `GPG_PASSPHRASE` - The passphrase for your GPG key

## Safe Release Process

### Standard Release Workflow

Use the Maven Release Plugin as the versioning tool for the root reactor, but keep publication gated by explicit inspection and CI validation.

1. **Start from a clean release branch**:
   ```bash
   git status --short
   git fetch origin --tags
   ```
   Confirm the branch contains the intended docs snapshot and no unrelated local changes.

2. **Prepare the release locally without pushing**:
   ```bash
   ./mvnw release:prepare -DpushChanges=false -Darguments="-DskipTests"
   ```
   The root POM also configures `<pushChanges>false</pushChanges>` for the release plugin. Keep the command-line flag anyway so the behavior is explicit in shell history and release notes.

   The plugin creates two local commits and a local tag:
   - `[maven-release-plugin] prepare release vX.Y.Z`
   - `[maven-release-plugin] prepare for next development iteration`
   - `vX.Y.Z`

3. **Inspect the release plugin output before any push**:
   ```bash
   git log --oneline --decorate -n 5
   git diff HEAD~2..HEAD -- pom.xml framework/pom.xml examples ai-sdk plugins
   ```
   The release commit should contain the release version. The next-development commit should contain the next `-SNAPSHOT` version.

4. **Synchronize release-coupled POMs outside the root reactor**:
   The release plugin only updates POMs in the Maven reactor it runs. After `release:prepare`, check and fix alternate topology and standalone POMs so they match the next development version on `main`.

   Required checks:
   ```bash
   rg -n "X\\.Y\\.Z-SNAPSHOT" --glob "pom*.xml" --glob "!docs/versions/**"
   rg -n "X\\.Y\\.Z" examples ai-sdk --glob "pom*.xml"
   ```
   Replace `X.Y.Z` with the just-released version. There should be no remaining references to the old snapshot in active POMs after the next-development commit.

   At minimum, check:
   - `examples/csv-payments/pom.pipeline-runtime.xml`
   - `examples/csv-payments/pom.monolith.xml`
   - `examples/csv-payments/pipeline-runtime-svc/pom.xml`
   - `examples/csv-payments/monolith-svc/pom.xml`
   - `examples/checkout/pom.xml` and its child modules
   - `ai-sdk/pom.xml`

5. **Run the release validation gate before pushing**:
   ```bash
   ./mvnw -f framework/pom.xml verify
   ./examples/csv-payments/build-pipeline-runtime.sh -pl orchestrator-svc -Dcsv.runtime.layout=pipeline-runtime -Dtest=PipelineRuntimeTopologyTest -Dit.test=CsvPaymentsPipelineRuntimeEndToEndIT verify
   ./examples/csv-payments/build-monolith.sh -DskipTests
   ./mvnw -f ai-sdk/pom.xml test
   npm --prefix docs run build
   ```
   If time forces a smaller gate, record exactly which commands were skipped and do not claim full release validation.

6. **Push the validated commits**:
   ```bash
   git push origin main
   ```
   Watch the `main` workflows. If a workflow fails because of version drift, fix `main` before pushing the tag.

7. **Publish the release**:
   ```bash
   git push origin vX.Y.Z
   ```
   This triggers the GitHub Actions workflow that runs `mvn deploy` to publish framework artifacts to Maven Central.

**Note**: The `mvn release:perform` step is not used in this setup since deployment is handled by GitHub Actions when a tag is pushed.

## Important Publishing Details (Central Validation)

### Publishing scope

The publish workflow deploys only the framework artifacts (parent, runtime, deployment) and skips examples:

- Maven runs from the repo root with `-pl framework,framework/runtime,framework/deployment -am`.
- The root POM is included in the reactor but is **not deployed** (`maven.deploy.skip=true` in the root, overridden to false in `framework/pom.xml`).

Note: Publishing the `framework-parent` artifact is expected. It is the BOM/parent POM that consumers import for dependency management, so it will appear in Maven Central autocomplete results.

Publishing only framework artifacts does not mean example and SDK versions can drift. The E2E CI lanes build compatibility surfaces from the same checkout and expect their parent versions and `tpf.version` properties to match the root development version. A stale alternate POM can fail before tests start with `Non-resolvable parent POM` because its `relativePath` points to the checked-out root POM at a different version.

### Main-branch E2E impact

The CSV E2E workflows run from alternate topology POMs. During the `v26.4.4` release, `release:prepare` updated the root reactor to `26.4.5-SNAPSHOT`, but left `examples/csv-payments/pom.pipeline-runtime.xml`, `examples/csv-payments/pom.monolith.xml`, and `ai-sdk/pom.xml` at `26.4.4-SNAPSHOT`. Because the release plugin pushed by default, `main` received the drift before it was reviewed. The next `main` E2E runs then failed at Maven model resolution rather than test execution.

Treat this as a release-blocking condition:

```bash
rg -n "OLD_VERSION-SNAPSHOT" --glob "pom*.xml" --glob "!docs/versions/**"
```

The command should return no active POMs after the next-development commit is prepared.

### Flattening at publish time (property-gated)

Central validation requires resolved dependency versions. We avoid profiles for core behavior and enable flattening only during publish using a property:

- Default: `-Dtpf.flatten.skip=true` (skip flatten during normal builds)
- Publish: `-Dtpf.flatten.skip=false` (flatten only for release)

This keeps local builds clean while producing Central-compliant POMs.

### Tag immutability

Release tags are treated as immutable. If a publish fails after a tag is created, you generally **cannot** reuse the same tag:

- Create a new patch tag (e.g., `v26.2.1`, `v26.2.2`) for retries.
- If repo rules allow, tags can be created from the GitHub UI as part of a release draft.

### When to Use the Versions Plugin Approach

Use this manual approach only when you need fine-grained control or the Release Plugin is not available:

1. **Manual Version Update**:
   - Update the version using the Maven Versions Plugin:
     ```bash
     mvn versions:set -DnewVersion=1.0.0
     mvn versions:commit
     ```
   - Test the build with `mvn clean install -P central-publishing`
   - Create a Git tag (e.g., `v1.0.0`)
   - Push the tag to trigger the GitHub Actions release workflow

**Comparison**:
- **Release Plugin**: Handles version updates across the selected Maven reactor, creates local release commits, and creates the local tag. It must run with push disabled so version drift can be fixed before `main` moves.
- **Versions Plugin**: Offers more manual control but requires multiple manual steps and careful coordination

### Alternative: Manual Release Workflow

For more control, you can create a workflow dispatch that requires manual triggering:

```yaml
name: Manual Publish to Maven Central

on:
  workflow_dispatch:
    inputs:
      release_version:
        description: 'Release version (e.g., 1.0.0)'
        required: true
        type: string
      dry_run:
        description: 'Dry run? (true/false)'
        required: false
        default: true
        type: boolean
```

This approach allows manual control over when releases happen.

## Troubleshooting

**GPG Signing Errors**:
- Verify your GPG key is properly configured
- Check that the GPG key ID matches what's in your keystore
- Ensure the GPG agent is running or passphrase is provided

**Sonatype Authentication Errors**:
- Verify your credentials in settings.xml
- Ensure you're using encrypted passwords in public repositories
- Check that your Central account has permissions for the group ID

**Sonatype Central Publishing Errors**:
- Review Sonatype Central logs in the GitHub Actions workflow
- Ensure all required artifacts (JARs, sources, javadoc, signatures) are present
- Check that artifacts meet Maven Central requirements

**Main E2E Fails Before Tests Start**:
- Check for stale parent versions in alternate POMs: `rg -n "OLD_VERSION-SNAPSHOT" --glob "pom*.xml" --glob "!docs/versions/**"`
- Check standalone TPF dependency properties such as `ai-sdk/pom.xml`'s `tpf.version`
- Confirm the local root POM version matches the parent version in `examples/csv-payments/pom.pipeline-runtime.xml` and `examples/csv-payments/pom.monolith.xml`

### Testing the Setup

Before pushing a tag that triggers the release workflow:
- `mvn clean verify` - test the build locally (runs unit tests)
- `mvn clean verify -DskipITs` - test build without integration tests
- `mvn clean verify -P central-publishing` - test with the central publishing profile but without deployment
- Use a test Sonatype repository for verification

## Important Notes

- Only the framework artifacts are published to Maven Central.
- Examples and SDK surfaces are not Central artifacts, but they are release-coupled because CI and users build them against the checked-out framework version.
- The root POM orchestrates the main build while the framework POM handles publishing.
- Prefer parent inheritance inside the root reactor. Check alternate top-level POMs and standalone POM properties separately during release preparation.
- Always verify release artifacts on Maven Central after a successful deployment.
