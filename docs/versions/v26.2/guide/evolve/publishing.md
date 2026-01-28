---
search: false
---

# Publishing to Maven Central

This document explains how to publish The Pipeline Framework to Maven Central and how to manage the project's versioning and release process properly.

## TL;DR: Automatic Release Process

The release process is fully automated with GitHub Actions using the Maven Release Plugin for version management:

1. **Run the Maven Release Plugin**: `mvn release:prepare -Darguments="-DskipTests"` (updates versions across all modules and creates local tag)
2. **Push the main branch**: `git push origin main` (pushes version updates; GitHub Actions runs tests but not full native build)
3. **Push the tag**: `git push origin vx.y.z` (where x.y.z is your release version; triggers publishing workflow)
4. **Monitor GitHub Actions**: Go to the Actions tab to monitor the publishing workflow (deploys to Sonatype Central, creates GitHub release)
5. **Verify on Maven Central**: Check artifacts are published at <https://central.sonatype.com/>

The GitHub Actions workflow automatically:
- Builds and tests the complete project (when pushing to main - though full native build is skipped for release commits)
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

The Pipeline Framework uses a centralized version management system to ensure consistency across all modules:

1. **Single Source of Truth**: The version is defined in the root POM (`pom.xml`) as the `<version>` element
2. **Strict Hierarchy**: Every module links back to its parent using `<parent>`, all the way up to the root
3. **Version Omission in Children**: All child and intermediate parent modules omit their own `<version>` tag entirely, relying solely on inheritance from the root parent
4. **Updating Versions**: To update the version, change it only in the root POM

### Version Property Definition

In the root POM (`pom.xml`):
```xml
<version>26.1-SNAPSHOT</version>
```

All child modules inherit this version through the parent relationship and omit their own `<version>` element entirely.

### Using Maven Versions Plugin

To update versions across all modules consistently, use the Maven Versions Plugin:

```bash
# Update the version across all modules
mvn versions:set -DnewVersion=1.0.0

# Verify the changes before committing
mvn versions:commit

# Or rollback if needed
mvn versions:revert
```

This ensures that all modules in the multimodule project are updated consistently.

### Documentation Snapshot (Hybrid Versioning)

For major/minor releases, snapshot the docs into `docs/versions/`:

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

### Standard Release Workflow (Recommended)

The Maven Release Plugin provides a complete automated solution that handles version updates across all modules.

1. **Prepare the Release**:
   - Use the Maven Release Plugin to prepare the release:
     ```bash
     mvn release:prepare -Darguments="-DskipTests"
     ```
   - This will update versions across all modules, create a tag, and set the next development version in one step
   - The plugin will prompt for:
     - The release version (e.g., 1.0.0)
     - The SCM tag (e.g., v1.0.0)
     - The next development version (e.g., 1.0.1-SNAPSHOT)

2. **Push the Changes**:
   - Push the version update commits to the main branch:
     ```bash
     git push origin main
     ```

3. **Publish the Release**:
   - Push the tag to trigger publishing to Maven Central:
     ```bash
     git push origin v1.0.0
     ```
   - This triggers the GitHub Actions workflow that runs `mvn deploy` to publish to Maven Central

**Note**: The `mvn release:perform` step is not used in this setup since deployment is handled by GitHub Actions when a tag is pushed.

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
- **Release Plugin**: Handles version updates across all modules automatically, but you still need to push commits and tags manually
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

### Testing the Setup

Before pushing a tag that triggers the release workflow:
- `mvn clean verify` - test the build locally (runs unit tests)
- `mvn clean verify -DskipITs` - test build without integration tests
- `mvn clean verify -P central-publishing` - test with the central publishing profile but without deployment
- Use a test Sonatype repository for verification

## Important Notes

- Only the framework artifacts (not example applications) are published to Maven Central
- The examples project does NOT depend on the published framework artifacts
- The root POM orchestrates the overall build while the framework POM handles publishing
- The project now uses standard Maven practices: strict hierarchy with every module linking back to its parent using `<parent>`, all the way up to the root, and version omission in children where all child and intermediate parent modules omit their own `<version>` tag entirely, relying solely on inheritance from the root parent
- Always verify your release artifacts on Maven Central after a successful deployment
