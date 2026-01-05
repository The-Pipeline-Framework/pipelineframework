/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This is a combined bundle for the browser-based template generator
// It includes Handlebars, the templates, and the browser engine

// The templates are embedded as a JS object
const TEMPLATES = {
  "application-dev-properties": "# Pipeline framework\n#quarkus.log.category.\"org.pipelineframework\".level=DEBUG\n\n# Hibernate / ORM / Panache (if used)\n#quarkus.log.category.\"org.hibernate\".level=DEBUG\n#quarkus.log.category.\"io.quarkus.hibernate\".level=DEBUG\n\n# Enable trace logging for gRPC\n#quarkus.log.category.\"io.grpc\".level=DEBUG\n#quarkus.log.category.\"io.quarkus.grpc\".level=DEBUG\n#quarkus.log.category.\"io.vertx.grpc\".level=DEBUG\n#quarkus.log.category.\"io.mutiny\".level=DEBUG\n\nquarkus.log.console.format=%d{HH:mm:ss} %-5p [%c] (%t) %s%e%n\n\nquarkus.otel.metrics.enabled=true\nquarkus.otel.metric.export.interval=5\n\n# Suppress unnecessary information\nquarkus.log.category.\"io.quarkus.opentelemetry.runtime.QuarkusContextStorage\".level=WARN",
  "application-properties": "quarkus.http.ssl-port={{#portNumber}}{{add 8443 portOffset}}{{/portNumber}}\n\nquarkus.hibernate-orm.blocking=false\nquarkus.datasource.jdbc=false\nquarkus.hibernate-orm.packages={{basePackage}}.common.domain\nquarkus.index-dependency.\"common\".group-id={{basePackage}}\nquarkus.index-dependency.\"common\".artifact-id=common\nquarkus.hibernate-orm.enabled=true\n\n# Micrometer configuration\nquarkus.micrometer.export.prometheus.enabled=true\nquarkus.micrometer.export.prometheus.path=/q/metrics\nquarkus.micrometer.binder.http-server.enabled=true\nquarkus.micrometer.binder.http-client.enabled=true\n\n# gRPC client configuration for other services\nquarkus.grpc.clients.orchestrator-service.host=localhost\nquarkus.grpc.clients.orchestrator-service.port={{#portNumber}}{{add 8443 portOffset}}{{/portNumber}}\nquarkus.grpc.clients.orchestrator-service.plain-text=false\nquarkus.grpc.clients.orchestrator-service.use-quarkus-grpc-client=true\nquarkus.grpc.clients.orchestrator-service.tls.enabled=true\n\n# Docker image\nquarkus.container-image.builder=jib\nquarkus.container-image.registry=localhost\nquarkus.container-image.group={{basePackage}}\nquarkus.container-image.name={{serviceName}}\nquarkus.container-image.tag=latest\n",
  "application-test-properties": "quarkus.otel.enabled=false\nquarkus.otel.sdk.disabled=true\nquarkus.observability.lgtm.enabled=false\n\nquarkus.hibernate-orm.schema-management.strategy=drop-and-create\n",
  "base-entity": "package {{basePackage}}.common.domain;\n\nimport io.quarkus.hibernate.reactive.panache.PanacheEntityBase;\nimport jakarta.persistence.Column;\nimport jakarta.persistence.Id;\nimport jakarta.persistence.MappedSuperclass;\nimport java.util.UUID;\nimport lombok.Getter;\nimport lombok.Setter;\n\n@Setter\n@Getter\n@MappedSuperclass\npublic abstract class BaseEntity extends PanacheEntityBase {\n\n  @Id\n  @Column(name = \"id\", updatable = false, nullable = false)\n  public UUID id;\n\n  public BaseEntity() {\n    id = UUID.randomUUID();\n  }\n}",
  "beans-xml": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<beans xmlns=\"https://jakarta.ee/xml/ns/jakartaee\"\n       xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n       xsi:schemaLocation=\"https://jakarta.ee/xml/ns/jakartaee \n                           https://jakarta.ee/xml/ns/jakartaee/beans_4_0.xsd\"\n       version=\"4.0\"\n       bean-discovery-mode=\"annotated\">\n</beans>",
  "common-converters": "package {{basePackage}}.common.mapper;\n\nimport java.util.Currency;\nimport java.util.concurrent.atomic.AtomicInteger;\nimport java.util.concurrent.atomic.AtomicLong;\nimport java.util.List;\nimport org.mapstruct.Named;\n\npublic class CommonConverters {\n\n    @Named(\"currencyToString\")\n    public String currencyToString(Currency currency) {\n        return currency != null ? currency.getCurrencyCode() : null;\n    }\n\n    @Named(\"stringToCurrency\")\n    public Currency stringToCurrency(String code) {\n        return code != null ? Currency.getInstance(code) : null;\n    }\n\n    @Named(\"atomicIntegerToString\")\n    public String atomicIntegerToString(AtomicInteger atomicInteger) {\n        return atomicInteger != null ? String.valueOf(atomicInteger.get()) : null;\n    }\n\n    @Named(\"stringToAtomicInteger\")\n    public AtomicInteger stringToAtomicInteger(String string) {\n        return string != null ? new AtomicInteger(Integer.parseInt(string)) : null;\n    }\n\n    @Named(\"atomicLongToString\")\n    public String atomicLongToString(AtomicLong atomicLong) {\n        return atomicLong != null ? String.valueOf(atomicLong.get()) : null;\n    }\n\n    @Named(\"stringToAtomicLong\")\n    public AtomicLong stringToAtomicLong(String string) {\n        return string != null ? new AtomicLong(Long.parseLong(string)) : null;\n    }\n    \n    @Named(\"listToString\")\n    public String listToString(List<String> list) {\n        return list != null ? String.join(\",\", list) : null;\n    }\n    \n    @Named(\"stringToList\")\n    public List<String> stringToList(String string) {\n        return string != null ? java.util.Arrays.asList(string.split(\",\")) : null;\n    }\n}",
  "common-pom": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<project xmlns=\"http://maven.apache.org/POM/4.0.0\"\n         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0\n                             http://maven.apache.org/xsd/maven-4.0.0.xsd\">\n    <modelVersion>4.0.0</modelVersion>\n\n    <parent>\n        <groupId>{{basePackage}}</groupId>\n        <artifactId>{{rootProjectName}}</artifactId>\n        <version>1.0.0</version>\n        <relativePath>../pom.xml</relativePath>\n    </parent>\n\n    <artifactId>common</artifactId>\n    <version>1.0</version>\n    <packaging>jar</packaging>\n\n    <dependencies>\n        <dependency>\n            <groupId>org.pipelineframework</groupId>\n            <artifactId>pipelineframework</artifactId>\n            <version>${version.pipeline}</version>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-grpc-protoc-plugin</artifactId>\n            <version>${quarkus.platform.version}</version>\n        </dependency>\n        <dependency>\n            <groupId>com.fasterxml.jackson.core</groupId>\n            <artifactId>jackson-databind</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>com.fasterxml.jackson.dataformat</groupId>\n            <artifactId>jackson-dataformat-yaml</artifactId>\n            <version>2.20.0</version>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-junit5</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-junit5-mockito</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-arc</artifactId>\n            <scope>provided</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.smallrye.config</groupId>\n            <artifactId>smallrye-config-source-yaml</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>io.smallrye.config</groupId>\n            <artifactId>smallrye-config</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>org.jboss.slf4j</groupId>\n            <artifactId>slf4j-jboss-logmanager</artifactId>\n            <scope>runtime</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.projectlombok</groupId>\n            <artifactId>lombok</artifactId>\n            <version>1.18.36</version>\n            <scope>provided</scope>\n        </dependency>\n        <dependency>\n            <groupId>com.opencsv</groupId>\n            <artifactId>opencsv</artifactId>\n            <version>5.11</version>\n        </dependency>\n        <dependency>\n            <groupId>com.google.guava</groupId>\n            <artifactId>guava</artifactId>\n            <version>32.1.2-jre</version>\n        </dependency>\n        <!-- Hibernate -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-hibernate-reactive-panache</artifactId>\n        </dependency>\n        <!-- gRPC -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-grpc-common</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-grpc-codegen</artifactId>\n            <scope>provided</scope>\n        </dependency>\n        <dependency>\n            <groupId>jakarta.annotation</groupId>\n            <artifactId>jakarta.annotation-api</artifactId>\n            <version>2.1.1</version>\n        </dependency>\n        <dependency>\n            <groupId>org.mapstruct</groupId>\n            <artifactId>mapstruct</artifactId>\n            <version>1.5.5.Final</version>\n        </dependency>\n        <dependency>\n            <groupId>org.mapstruct</groupId>\n            <artifactId>mapstruct-processor</artifactId>\n            <version>1.5.5.Final</version>\n            <scope>provided</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.slf4j</groupId>\n            <artifactId>slf4j-nop</artifactId>\n            <version>2.0.9</version>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-jacoco</artifactId>\n            <scope>test</scope>\n        </dependency>\n\n        <!-- Observability dependencies -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-micrometer</artifactId>\n            <version>${quarkus.platform.version}</version>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-micrometer-registry-prometheus</artifactId>\n            <version>${quarkus.platform.version}</version>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-opentelemetry</artifactId>\n            <version>${quarkus.platform.version}</version>\n            <exclusions>\n                <exclusion>\n                    <groupId>io.opentelemetry.instrumentation</groupId>\n                    <artifactId>opentelemetry-jdbc</artifactId>\n                </exclusion>\n            </exclusions>\n        </dependency>\n        <dependency>\n            <groupId>io.opentelemetry</groupId>\n            <artifactId>opentelemetry-exporter-sender-grpc-managed-channel</artifactId>\n            <version>1.54.0</version>\n        </dependency>\n    </dependencies>\n\n    <build>\n        <extensions>\n            <extension>\n                <groupId>kr.motd.maven</groupId>\n                <artifactId>os-maven-plugin</artifactId>\n                <version>1.7.0</version>\n            </extension>\n        </extensions>\n        <plugins>\n            <plugin>\n                <groupId>org.apache.maven.plugins</groupId>\n                <artifactId>maven-compiler-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>org.apache.maven.plugins</groupId>\n                <artifactId>maven-surefire-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>io.quarkus</groupId>\n                <artifactId>quarkus-maven-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>com.diffplug.spotless</groupId>\n                <artifactId>spotless-maven-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>io.smallrye</groupId>\n                <artifactId>jandex-maven-plugin</artifactId>\n            </plugin>\n        </plugins>\n    </build>\n\n    <profiles>\n        <profile>\n            <id>native</id>\n            <properties>\n                <quarkus.native.enabled>true</quarkus.native.enabled>\n                <skipITs>false</skipITs>\n            </properties>\n        </profile>\n    </profiles>\n</project>",
  "domain": "package {{basePackage}}.common.domain;\n\nimport io.quarkus.hibernate.reactive.panache.PanacheEntityBase;\nimport jakarta.persistence.Column;\nimport jakarta.persistence.Id;\nimport jakarta.persistence.MappedSuperclass;\nimport java.util.UUID;\nimport lombok.Getter;\nimport lombok.Setter;\n\n{{#if hasDateFields}}\nimport java.time.LocalDate;\nimport java.time.LocalDateTime;\nimport java.time.OffsetDateTime;\nimport java.time.ZonedDateTime;\nimport java.time.Instant;\nimport java.time.Duration;\nimport java.time.Period;\n{{/if}}\n{{#if hasBigIntegerFields}}\nimport java.math.BigInteger;\n{{/if}}\n{{#if hasBigDecimalFields}}\nimport java.math.BigDecimal;\n{{/if}}\n{{#if hasCurrencyFields}}\nimport java.util.Currency;\n{{/if}}\n{{#if hasPathFields}}\nimport java.nio.file.Path;\n{{/if}}\n{{#if hasNetFields}}\nimport java.net.URI;\nimport java.net.URL;\n{{/if}}\n{{#if hasIoFields}}\nimport java.io.File;\n{{/if}}\n{{#if hasAtomicFields}}\nimport java.util.concurrent.atomic.AtomicInteger;\nimport java.util.concurrent.atomic.AtomicLong;\n{{/if}}\n{{#if hasUtilFields}}\nimport java.util.List;\n{{/if}}\n{{#if hasMapFields}}\nimport java.util.Map;\n{{/if}}\n\n@Setter\n@Getter\npublic class {{className}} extends BaseEntity {\n\n{{#each fields}}\n{{#unless (isIdField this.name)}}\n  public {{{this.type}}} {{this.name}};\n{{/unless}}\n{{/each}}\n}",
  "dto": "package {{basePackage}}.common.dto;\n\nimport com.fasterxml.jackson.databind.annotation.JsonDeserialize;\nimport com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;\nimport java.util.UUID;\nimport lombok.*;\n\n{{#if hasDateFields}}\nimport java.time.LocalDate;\nimport java.time.LocalDateTime;\nimport java.time.OffsetDateTime;\nimport java.time.ZonedDateTime;\nimport java.time.Instant;\nimport java.time.Duration;\nimport java.time.Period;\n{{/if}}\n{{#if hasBigIntegerFields}}\nimport java.math.BigInteger;\n{{/if}}\n{{#if hasBigDecimalFields}}\nimport java.math.BigDecimal;\n{{/if}}\n{{#if hasCurrencyFields}}\nimport java.util.Currency;\n{{/if}}\n{{#if hasPathFields}}\nimport java.nio.file.Path;\n{{/if}}\n{{#if hasNetFields}}\nimport java.net.URI;\nimport java.net.URL;\n{{/if}}\n{{#if hasIoFields}}\nimport java.io.File;\n{{/if}}\n{{#if hasAtomicFields}}\nimport java.util.concurrent.atomic.AtomicInteger;\nimport java.util.concurrent.atomic.AtomicLong;\n{{/if}}\n{{#if hasUtilFields}}\nimport java.util.List;\n{{/if}}\n{{#if hasMapFields}}\nimport java.util.Map;\n{{/if}}\n\n@Value\n@Builder\n@JsonDeserialize(builder = {{className}}.{{className}}Builder.class)\npublic class {{className}} {\n{{#unless hasIdField}}  UUID id;\n{{/unless}}\n{{#each fields}}\n  {{{this.type}}} {{sanitizeJavaIdentifier this.name}};\n{{/each}}\n\n  // Lombok will generate the builder, but Jackson needs to know how to interpret it\n  @JsonPOJOBuilder(withPrefix = \"\")\n  public static class {{className}}Builder {}\n}",
  "gitignore": "# Maven\ntarget/\npom.xml.tag\npom.xml.releaseBackup\npom.xml.versionsBackup\npom.xml.next\nrelease.properties\n.codeclimate.yml\n\n# Compilation\n*.class\n*.war\n*.jar\n*.ear\n\n# Log files\n*.log\n.log\n\n# IDE\n.idea/\n*.iws\n*.iml\n*.ipr\n*.sw?\n*~\n.#*\n.DS_Store\n.classpath\n.project\n.settings/\n.vscode/\n\n# Temporary files\n.tmp\ntemp/\n\n# OS Generated\n.Spotlight-V100\n.Trashes\nehthumbs.db\nIcon?\nThumbs.db\n\n# Docker\n.docker/\n\n# Generated certificates\n**/server-keystore.jks\n**/client-truststore.jks\n**/quarkus-cert.pem\n**/quarkus-key.pem",
  "mapper": "package {{basePackage}}.common.mapper;\n\nimport {{basePackage}}.common.domain.{{domainClass}};\nimport {{basePackage}}.common.dto.{{dtoClass}};\nimport {{grpcClass}};\nimport org.mapstruct.Mapper;\nimport org.mapstruct.ReportingPolicy;\nimport org.mapstruct.factory.Mappers;\n\n@SuppressWarnings(\"unused\")\n@Mapper(\n    componentModel = \"jakarta\",\n    uses = {CommonConverters.class},\n    unmappedTargetPolicy = ReportingPolicy.WARN)\npublic interface {{className}}Mapper extends org.pipelineframework.mapper.Mapper<{{grpcClass}}.{{className}}, {{dtoClass}}, {{domainClass}}> {\n\n  {{className}}Mapper INSTANCE = Mappers.getMapper( {{className}}Mapper.class );\n\n  // Domain ↔ DTO\n  @Override\n  {{dtoClass}} toDto({{domainClass}} entity);\n\n  @Override\n  {{domainClass}} fromDto({{dtoClass}} dto);\n\n  // DTO ↔ gRPC\n  @Override\n  {{grpcClass}}.{{className}} toGrpc({{dtoClass}} dto);\n\n  @Override\n  {{dtoClass}} fromGrpc({{grpcClass}}.{{className}} grpc);\n}",
  "mvnw-cmd": "<# : batch portion\n@REM ----------------------------------------------------------------------------\n@REM Licensed to the Apache Software Foundation (ASF) under one\n@REM or more contributor license agreements.  See the NOTICE file\n@REM distributed with this work for additional information\n@REM regarding copyright ownership.  The ASF licenses this file\n@REM to you under the Apache License, Version 2.0 (the\n@REM \"License\"); you may not use this file except in compliance\n@REM with the License.  You may obtain a copy of the License at\n@REM\n@REM    http://www.apache.org/licenses/LICENSE-2.0\n@REM\n@REM Unless required by applicable law or agreed to in writing,\n@REM software distributed under the License is distributed on an\n@REM \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n@REM KIND, either express or implied.  See the License for the\n@REM specific language governing permissions and limitations\n@REM under the License.\n@REM ----------------------------------------------------------------------------\n\n@REM ----------------------------------------------------------------------------\n@REM Apache Maven Wrapper startup batch script, version 3.3.4\n@REM\n@REM Optional ENV vars\n@REM   MVNW_REPOURL - repo url base for downloading maven distribution\n@REM   MVNW_USERNAME/MVNW_PASSWORD - user and password for downloading maven\n@REM   MVNW_VERBOSE - true: enable verbose log; others: silence the output\n@REM ----------------------------------------------------------------------------\n\n@IF \"%__MVNW_ARG0_NAME__%\"==\"\" (SET __MVNW_ARG0_NAME__=%~nx0)\n@SET __MVNW_CMD__=\n@SET __MVNW_ERROR__=\n@SET __MVNW_PSMODULEP_SAVE=%PSModulePath%\n@SET PSModulePath=\n@FOR /F \"usebackq tokens=1* delims==\" %%A IN (`powershell -noprofile \"& {$scriptDir='%~dp0'; $script='%__MVNW_ARG0_NAME__%'; icm -ScriptBlock ([Scriptblock]::Create((Get-Content -Raw '%~f0'))) -NoNewScope}\"`) DO @(\n  IF \"%%A\"==\"MVN_CMD\" (set __MVNW_CMD__=%%B) ELSE IF \"%%B\"==\"\" (echo %%A) ELSE (echo %%A=%%B)\n)\n@SET PSModulePath=%__MVNW_PSMODULEP_SAVE%\n@SET __MVNW_PSMODULEP_SAVE=\n@SET __MVNW_ARG0_NAME__=\n@SET MVNW_USERNAME=\n@SET MVNW_PASSWORD=\n@IF NOT \"%__MVNW_CMD__%\"==\"\" (\"%__MVNW_CMD__%\" %*)\n@echo Cannot start maven from wrapper >&2 && exit /b 1\n@GOTO :EOF\n: end batch / begin powershell #>\n\n$ErrorActionPreference = \"Stop\"\nif ($env:MVNW_VERBOSE -eq \"true\") {\n  $VerbosePreference = \"Continue\"\n}\n\n# calculate distributionUrl, requires .mvn/wrapper/maven-wrapper.properties\n$distributionUrl = (Get-Content -Raw \"$scriptDir/.mvn/wrapper/maven-wrapper.properties\" | ConvertFrom-StringData).distributionUrl\nif (!$distributionUrl) {\n  Write-Error \"cannot read distributionUrl property in $scriptDir/.mvn/wrapper/maven-wrapper.properties\"\n}\n\nswitch -wildcard -casesensitive ( $($distributionUrl -replace '^.*/','') ) {\n  \"maven-mvnd-*\" {\n    $USE_MVND = $true\n    $distributionUrl = $distributionUrl -replace '-bin\\.[^.]*$',\"-windows-amd64.zip\"\n    $MVN_CMD = \"mvnd.cmd\"\n    break\n  }\n  default {\n    $USE_MVND = $false\n    $MVN_CMD = $script -replace '^mvnw','mvn'\n    break\n  }\n}\n\n# apply MVNW_REPOURL and calculate MAVEN_HOME\n# maven home pattern: ~/.m2/wrapper/dists/{apache-maven-<version>,maven-mvnd-<version>-<platform>}/<hash>\nif ($env:MVNW_REPOURL) {\n  $MVNW_REPO_PATTERN = if ($USE_MVND -eq $False) { \"/org/apache/maven/\" } else { \"/maven/mvnd/\" }\n  $distributionUrl = \"$env:MVNW_REPOURL$MVNW_REPO_PATTERN$($distributionUrl -replace \"^.*$MVNW_REPO_PATTERN\",'')\"\n}\n$distributionUrlName = $distributionUrl -replace '^.*/',''\n$distributionUrlNameMain = $distributionUrlName -replace '\\.[^.]*$','' -replace '-bin$',''\n\n$MAVEN_M2_PATH = \"$HOME/.m2\"\nif ($env:MAVEN_USER_HOME) {\n  $MAVEN_M2_PATH = \"$env:MAVEN_USER_HOME\"\n}\n\nif (-not (Test-Path -Path $MAVEN_M2_PATH)) {\n    New-Item -Path $MAVEN_M2_PATH -ItemType Directory | Out-Null\n}\n\n$MAVEN_WRAPPER_DISTS = $null\nif ((Get-Item $MAVEN_M2_PATH).Target[0] -eq $null) {\n  $MAVEN_WRAPPER_DISTS = \"$MAVEN_M2_PATH/wrapper/dists\"\n} else {\n  $MAVEN_WRAPPER_DISTS = (Get-Item $MAVEN_M2_PATH).Target[0] + \"/wrapper/dists\"\n}\n\n$MAVEN_HOME_PARENT = \"$MAVEN_WRAPPER_DISTS/$distributionUrlNameMain\"\n$MAVEN_HOME_NAME = ([System.Security.Cryptography.SHA256]::Create().ComputeHash([byte[]][char[]]$distributionUrl) | ForEach-Object {$_.ToString(\"x2\")}) -join ''\n$MAVEN_HOME = \"$MAVEN_HOME_PARENT/$MAVEN_HOME_NAME\"\n\nif (Test-Path -Path \"$MAVEN_HOME\" -PathType Container) {\n  Write-Verbose \"found existing MAVEN_HOME at $MAVEN_HOME\"\n  Write-Output \"MVN_CMD=$MAVEN_HOME/bin/$MVN_CMD\"\n  exit $?\n}\n\nif (! $distributionUrlNameMain -or ($distributionUrlName -eq $distributionUrlNameMain)) {\n  Write-Error \"distributionUrl is not valid, must end with *-bin.zip, but found $distributionUrl\"\n}\n\n# prepare tmp dir\n$TMP_DOWNLOAD_DIR_HOLDER = New-TemporaryFile\n$TMP_DOWNLOAD_DIR = New-Item -Itemtype Directory -Path \"$TMP_DOWNLOAD_DIR_HOLDER.dir\"\n$TMP_DOWNLOAD_DIR_HOLDER.Delete() | Out-Null\ntrap {\n  if ($TMP_DOWNLOAD_DIR.Exists) {\n    try { Remove-Item $TMP_DOWNLOAD_DIR -Recurse -Force | Out-Null }\n    catch { Write-Warning \"Cannot remove $TMP_DOWNLOAD_DIR\" }\n  }\n}\n\nNew-Item -Itemtype Directory -Path \"$MAVEN_HOME_PARENT\" -Force | Out-Null\n\n# Download and Install Apache Maven\nWrite-Verbose \"Couldn't find MAVEN_HOME, downloading and installing it ...\"\nWrite-Verbose \"Downloading from: $distributionUrl\"\nWrite-Verbose \"Downloading to: $TMP_DOWNLOAD_DIR/$distributionUrlName\"\n\n$webclient = New-Object System.Net.WebClient\nif ($env:MVNW_USERNAME -and $env:MVNW_PASSWORD) {\n  $webclient.Credentials = New-Object System.Net.NetworkCredential($env:MVNW_USERNAME, $env:MVNW_PASSWORD)\n}\n[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12\n$webclient.DownloadFile($distributionUrl, \"$TMP_DOWNLOAD_DIR/$distributionUrlName\") | Out-Null\n\n# If specified, validate the SHA-256 sum of the Maven distribution zip file\n$distributionSha256Sum = (Get-Content -Raw \"$scriptDir/.mvn/wrapper/maven-wrapper.properties\" | ConvertFrom-StringData).distributionSha256Sum\nif ($distributionSha256Sum) {\n  if ($USE_MVND) {\n    Write-Error \"Checksum validation is not supported for maven-mvnd. `nPlease disable validation by removing 'distributionSha256Sum' from your maven-wrapper.properties.\"\n  }\n  Import-Module $PSHOME\\Modules\\Microsoft.PowerShell.Utility -Function Get-FileHash\n  if ((Get-FileHash \"$TMP_DOWNLOAD_DIR/$distributionUrlName\" -Algorithm SHA256).Hash.ToLower() -ne $distributionSha256Sum) {\n    Write-Error \"Error: Failed to validate Maven distribution SHA-256, your Maven distribution might be compromised. If you updated your Maven version, you need to update the specified distributionSha256Sum property.\"\n  }\n}\n\n# unzip and move\nExpand-Archive \"$TMP_DOWNLOAD_DIR/$distributionUrlName\" -DestinationPath \"$TMP_DOWNLOAD_DIR\" | Out-Null\n\n# Find the actual extracted directory name (handles snapshots where filename != directory name)\n$actualDistributionDir = \"\"\n\n# First try the expected directory name (for regular distributions)\n$expectedPath = Join-Path \"$TMP_DOWNLOAD_DIR\" \"$distributionUrlNameMain\"\n$expectedMvnPath = Join-Path \"$expectedPath\" \"bin/$MVN_CMD\"\nif ((Test-Path -Path $expectedPath -PathType Container) -and (Test-Path -Path $expectedMvnPath -PathType Leaf)) {\n  $actualDistributionDir = $distributionUrlNameMain\n}\n\n# If not found, search for any directory with the Maven executable (for snapshots)\nif (!$actualDistributionDir) {\n  Get-ChildItem -Path \"$TMP_DOWNLOAD_DIR\" -Directory | ForEach-Object {\n    $testPath = Join-Path $_.FullName \"bin/$MVN_CMD\"\n    if (Test-Path -Path $testPath -PathType Leaf) {\n      $actualDistributionDir = $_.Name\n    }\n  }\n}\n\nif (!$actualDistributionDir) {\n  Write-Error \"Could not find Maven distribution directory in extracted archive\"\n}\n\nWrite-Verbose \"Found extracted Maven distribution directory: $actualDistributionDir\"\nRename-Item -Path \"$TMP_DOWNLOAD_DIR/$actualDistributionDir\" -NewName $MAVEN_HOME_NAME | Out-Null\ntry {\n  Move-Item -Path \"$TMP_DOWNLOAD_DIR/$MAVEN_HOME_NAME\" -Destination $MAVEN_HOME_PARENT | Out-Null\n} catch {\n  if (! (Test-Path -Path \"$MAVEN_HOME\" -PathType Container)) {\n    Write-Error \"fail to move MAVEN_HOME\"\n  }\n} finally {\n  try { Remove-Item $TMP_DOWNLOAD_DIR -Recurse -Force | Out-Null }\n  catch { Write-Warning \"Cannot remove $TMP_DOWNLOAD_DIR\" }\n}\n\nWrite-Output \"MVN_CMD=$MAVEN_HOME/bin/$MVN_CMD\"\n",
  "mvnw": "#!/bin/sh\n# ----------------------------------------------------------------------------\n# Licensed to the Apache Software Foundation (ASF) under one\n# or more contributor license agreements.  See the NOTICE file\n# distributed with this work for additional information\n# regarding copyright ownership.  The ASF licenses this file\n# to you under the Apache License, Version 2.0 (the\n# \"License\"); you may not use this file except in compliance\n# with the License.  You may obtain a copy of the License at\n#\n#    http://www.apache.org/licenses/LICENSE-2.0\n#\n# Unless required by applicable law or agreed to in writing,\n# software distributed under the License is distributed on an\n# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\n# KIND, either express or implied.  See the License for the\n# specific language governing permissions and limitations\n# under the License.\n# ----------------------------------------------------------------------------\n\n# ----------------------------------------------------------------------------\n# Apache Maven Wrapper startup batch script, version 3.3.4\n#\n# Optional ENV vars\n# -----------------\n#   JAVA_HOME - location of a JDK home dir, required when download maven via java source\n#   MVNW_REPOURL - repo url base for downloading maven distribution\n#   MVNW_USERNAME/MVNW_PASSWORD - user and password for downloading maven\n#   MVNW_VERBOSE - true: enable verbose log; debug: trace the mvnw script; others: silence the output\n# ----------------------------------------------------------------------------\n\nset -euf\n[ \"${MVNW_VERBOSE-}\" != debug ] || set -x\n\n# OS specific support.\nnative_path() { printf %s\\\\n \"$1\"; }\ncase \"$(uname)\" in\nCYGWIN* | MINGW*)\n  [ -z \"${JAVA_HOME-}\" ] || JAVA_HOME=\"$(cygpath --unix \"$JAVA_HOME\")\"\n  native_path() { cygpath --path --windows \"$1\"; }\n  ;;\nesac\n\n# set JAVACMD and JAVACCMD\nset_java_home() {\n  # For Cygwin and MinGW, ensure paths are in Unix format before anything is touched\n  if [ -n \"${JAVA_HOME-}\" ]; then\n    if [ -x \"$JAVA_HOME/jre/sh/java\" ]; then\n      # IBM's JDK on AIX uses strange locations for the executables\n      JAVACMD=\"$JAVA_HOME/jre/sh/java\"\n      JAVACCMD=\"$JAVA_HOME/jre/sh/javac\"\n    else\n      JAVACMD=\"$JAVA_HOME/bin/java\"\n      JAVACCMD=\"$JAVA_HOME/bin/javac\"\n\n      if [ ! -x \"$JAVACMD\" ] || [ ! -x \"$JAVACCMD\" ]; then\n        echo \"The JAVA_HOME environment variable is not defined correctly, so mvnw cannot run.\" >&2\n        echo \"JAVA_HOME is set to \\\"$JAVA_HOME\\\", but \\\"\\$JAVA_HOME/bin/java\\\" or \\\"\\$JAVA_HOME/bin/javac\\\" does not exist.\" >&2\n        return 1\n      fi\n    fi\n  else\n    JAVACMD=\"$(\n      'set' +e\n      'unset' -f command 2>/dev/null\n      'command' -v java\n    )\" || :\n    JAVACCMD=\"$(\n      'set' +e\n      'unset' -f command 2>/dev/null\n      'command' -v javac\n    )\" || :\n\n    if [ ! -x \"${JAVACMD-}\" ] || [ ! -x \"${JAVACCMD-}\" ]; then\n      echo \"The java/javac command does not exist in PATH nor is JAVA_HOME set, so mvnw cannot run.\" >&2\n      return 1\n    fi\n  fi\n}\n\n# hash string like Java String::hashCode\nhash_string() {\n  str=\"${1:-}\" h=0\n  while [ -n \"$str\" ]; do\n    char=\"${str%\"${str#?}\"}\"\n    h=$(((h * 31 + $(LC_CTYPE=C printf %d \"'$char\")) % 4294967296))\n    str=\"${str#?}\"\n  done\n  printf %x\\\\n $h\n}\n\nverbose() { :; }\n[ \"${MVNW_VERBOSE-}\" != true ] || verbose() { printf %s\\\\n \"${1-}\"; }\n\ndie() {\n  printf %s\\\\n \"$1\" >&2\n  exit 1\n}\n\ntrim() {\n  # MWRAPPER-139:\n  #   Trims trailing and leading whitespace, carriage returns, tabs, and linefeeds.\n  #   Needed for removing poorly interpreted newline sequences when running in more\n  #   exotic environments such as mingw bash on Windows.\n  printf \"%s\" \"${1}\" | tr -d '[:space:]'\n}\n\nscriptDir=\"$(dirname \"$0\")\"\nscriptName=\"$(basename \"$0\")\"\n\n# parse distributionUrl and optional distributionSha256Sum, requires .mvn/wrapper/maven-wrapper.properties\nwhile IFS=\"=\" read -r key value; do\n  case \"${key-}\" in\n  distributionUrl) distributionUrl=$(trim \"${value-}\") ;;\n  distributionSha256Sum) distributionSha256Sum=$(trim \"${value-}\") ;;\n  esac\ndone <\"$scriptDir/.mvn/wrapper/maven-wrapper.properties\"\n[ -n \"${distributionUrl-}\" ] || die \"cannot read distributionUrl property in $scriptDir/.mvn/wrapper/maven-wrapper.properties\"\n\ncase \"${distributionUrl##*/}\" in\nmaven-mvnd-*bin.*)\n  MVN_CMD=mvnd.sh _MVNW_REPO_PATTERN=/maven/mvnd/\n  case \"${PROCESSOR_ARCHITECTURE-}${PROCESSOR_ARCHITEW6432-}:$(uname -a)\" in\n  *AMD64:CYGWIN* | *AMD64:MINGW*) distributionPlatform=windows-amd64 ;;\n  :Darwin*x86_64) distributionPlatform=darwin-amd64 ;;\n  :Darwin*arm64) distributionPlatform=darwin-aarch64 ;;\n  :Linux*x86_64*) distributionPlatform=linux-amd64 ;;\n  *)\n    echo \"Cannot detect native platform for mvnd on $(uname)-$(uname -m), use pure java version\" >&2\n    distributionPlatform=linux-amd64\n    ;;\n  esac\n  distributionUrl=\"${distributionUrl%-bin.*}-$distributionPlatform.zip\"\n  ;;\nmaven-mvnd-*) MVN_CMD=mvnd.sh _MVNW_REPO_PATTERN=/maven/mvnd/ ;;\n*) MVN_CMD=\"mvn${scriptName#mvnw}\" _MVNW_REPO_PATTERN=/org/apache/maven/ ;;\nesac\n\n# apply MVNW_REPOURL and calculate MAVEN_HOME\n# maven home pattern: ~/.m2/wrapper/dists/{apache-maven-<version>,maven-mvnd-<version>-<platform>}/<hash>\n[ -z \"${MVNW_REPOURL-}\" ] || distributionUrl=\"$MVNW_REPOURL$_MVNW_REPO_PATTERN${distributionUrl#*\"$_MVNW_REPO_PATTERN\"}\"\ndistributionUrlName=\"${distributionUrl##*/}\"\ndistributionUrlNameMain=\"${distributionUrlName%.*}\"\ndistributionUrlNameMain=\"${distributionUrlNameMain%-bin}\"\nMAVEN_USER_HOME=\"${MAVEN_USER_HOME:-${HOME}/.m2}\"\nMAVEN_HOME=\"${MAVEN_USER_HOME}/wrapper/dists/${distributionUrlNameMain-}/$(hash_string \"$distributionUrl\")\"\n\nexec_maven() {\n  unset MVNW_VERBOSE MVNW_USERNAME MVNW_PASSWORD MVNW_REPOURL || :\n  exec \"$MAVEN_HOME/bin/$MVN_CMD\" \"$@\" || die \"cannot exec $MAVEN_HOME/bin/$MVN_CMD\"\n}\n\nif [ -d \"$MAVEN_HOME\" ]; then\n  verbose \"found existing MAVEN_HOME at $MAVEN_HOME\"\n  exec_maven \"$@\"\nfi\n\ncase \"${distributionUrl-}\" in\n*?-bin.zip | *?maven-mvnd-?*-?*.zip) ;;\n*) die \"distributionUrl is not valid, must match *-bin.zip or maven-mvnd-*.zip, but found '${distributionUrl-}'\" ;;\nesac\n\n# prepare tmp dir\nif TMP_DOWNLOAD_DIR=\"$(mktemp -d)\" && [ -d \"$TMP_DOWNLOAD_DIR\" ]; then\n  clean() { rm -rf -- \"$TMP_DOWNLOAD_DIR\"; }\n  trap clean HUP INT TERM EXIT\nelse\n  die \"cannot create temp dir\"\nfi\n\nmkdir -p -- \"${MAVEN_HOME%/*}\"\n\n# Download and Install Apache Maven\nverbose \"Couldn't find MAVEN_HOME, downloading and installing it ...\"\nverbose \"Downloading from: $distributionUrl\"\nverbose \"Downloading to: $TMP_DOWNLOAD_DIR/$distributionUrlName\"\n\n# select .zip or .tar.gz\nif ! command -v unzip >/dev/null; then\n  distributionUrl=\"${distributionUrl%.zip}.tar.gz\"\n  distributionUrlName=\"${distributionUrl##*/}\"\nfi\n\n# verbose opt\n__MVNW_QUIET_WGET=--quiet __MVNW_QUIET_CURL=--silent __MVNW_QUIET_UNZIP=-q __MVNW_QUIET_TAR=''\n[ \"${MVNW_VERBOSE-}\" != true ] || __MVNW_QUIET_WGET='' __MVNW_QUIET_CURL='' __MVNW_QUIET_UNZIP='' __MVNW_QUIET_TAR=v\n\n# normalize http auth\ncase \"${MVNW_PASSWORD:+has-password}\" in\n'') MVNW_USERNAME='' MVNW_PASSWORD='' ;;\nhas-password) [ -n \"${MVNW_USERNAME-}\" ] || MVNW_USERNAME='' MVNW_PASSWORD='' ;;\nesac\n\nif [ -z \"${MVNW_USERNAME-}\" ] && command -v wget >/dev/null; then\n  verbose \"Found wget ... using wget\"\n  wget ${__MVNW_QUIET_WGET:+\"$__MVNW_QUIET_WGET\"} \"$distributionUrl\" -O \"$TMP_DOWNLOAD_DIR/$distributionUrlName\" || die \"wget: Failed to fetch $distributionUrl\"\nelif [ -z \"${MVNW_USERNAME-}\" ] && command -v curl >/dev/null; then\n  verbose \"Found curl ... using curl\"\n  curl ${__MVNW_QUIET_CURL:+\"$__MVNW_QUIET_CURL\"} -f -L -o \"$TMP_DOWNLOAD_DIR/$distributionUrlName\" \"$distributionUrl\" || die \"curl: Failed to fetch $distributionUrl\"\nelif set_java_home; then\n  verbose \"Falling back to use Java to download\"\n  javaSource=\"$TMP_DOWNLOAD_DIR/Downloader.java\"\n  targetZip=\"$TMP_DOWNLOAD_DIR/$distributionUrlName\"\n  cat >\"$javaSource\" <<-END\n\tpublic class Downloader extends java.net.Authenticator\n\t{\n\t  protected java.net.PasswordAuthentication getPasswordAuthentication()\n\t  {\n\t    return new java.net.PasswordAuthentication( System.getenv( \"MVNW_USERNAME\" ), System.getenv( \"MVNW_PASSWORD\" ).toCharArray() );\n\t  }\n\t  public static void main( String[] args ) throws Exception\n\t  {\n\t    setDefault( new Downloader() );\n\t    java.nio.file.Files.copy( java.net.URI.create( args[0] ).toURL().openStream(), java.nio.file.Paths.get( args[1] ).toAbsolutePath().normalize() );\n\t  }\n\t}\n\tEND\n  # For Cygwin/MinGW, switch paths to Windows format before running javac and java\n  verbose \" - Compiling Downloader.java ...\"\n  \"$(native_path \"$JAVACCMD\")\" \"$(native_path \"$javaSource\")\" || die \"Failed to compile Downloader.java\"\n  verbose \" - Running Downloader.java ...\"\n  \"$(native_path \"$JAVACMD\")\" -cp \"$(native_path \"$TMP_DOWNLOAD_DIR\")\" Downloader \"$distributionUrl\" \"$(native_path \"$targetZip\")\"\nfi\n\n# If specified, validate the SHA-256 sum of the Maven distribution zip file\nif [ -n \"${distributionSha256Sum-}\" ]; then\n  distributionSha256Result=false\n  if [ \"$MVN_CMD\" = mvnd.sh ]; then\n    echo \"Checksum validation is not supported for maven-mvnd.\" >&2\n    echo \"Please disable validation by removing 'distributionSha256Sum' from your maven-wrapper.properties.\" >&2\n    exit 1\n  elif command -v sha256sum >/dev/null; then\n    if echo \"$distributionSha256Sum  $TMP_DOWNLOAD_DIR/$distributionUrlName\" | sha256sum -c - >/dev/null 2>&1; then\n      distributionSha256Result=true\n    fi\n  elif command -v shasum >/dev/null; then\n    if echo \"$distributionSha256Sum  $TMP_DOWNLOAD_DIR/$distributionUrlName\" | shasum -a 256 -c >/dev/null 2>&1; then\n      distributionSha256Result=true\n    fi\n  else\n    echo \"Checksum validation was requested but neither 'sha256sum' or 'shasum' are available.\" >&2\n    echo \"Please install either command, or disable validation by removing 'distributionSha256Sum' from your maven-wrapper.properties.\" >&2\n    exit 1\n  fi\n  if [ $distributionSha256Result = false ]; then\n    echo \"Error: Failed to validate Maven distribution SHA-256, your Maven distribution might be compromised.\" >&2\n    echo \"If you updated your Maven version, you need to update the specified distributionSha256Sum property.\" >&2\n    exit 1\n  fi\nfi\n\n# unzip and move\nif command -v unzip >/dev/null; then\n  unzip ${__MVNW_QUIET_UNZIP:+\"$__MVNW_QUIET_UNZIP\"} \"$TMP_DOWNLOAD_DIR/$distributionUrlName\" -d \"$TMP_DOWNLOAD_DIR\" || die \"failed to unzip\"\nelse\n  tar xzf${__MVNW_QUIET_TAR:+\"$__MVNW_QUIET_TAR\"} \"$TMP_DOWNLOAD_DIR/$distributionUrlName\" -C \"$TMP_DOWNLOAD_DIR\" || die \"failed to untar\"\nfi\n\n# Find the actual extracted directory name (handles snapshots where filename != directory name)\nactualDistributionDir=\"\"\n\n# First try the expected directory name (for regular distributions)\nif [ -d \"$TMP_DOWNLOAD_DIR/$distributionUrlNameMain\" ]; then\n  if [ -f \"$TMP_DOWNLOAD_DIR/$distributionUrlNameMain/bin/$MVN_CMD\" ]; then\n    actualDistributionDir=\"$distributionUrlNameMain\"\n  fi\nfi\n\n# If not found, search for any directory with the Maven executable (for snapshots)\nif [ -z \"$actualDistributionDir\" ]; then\n  # enable globbing to iterate over items\n  set +f\n  for dir in \"$TMP_DOWNLOAD_DIR\"/*; do\n    if [ -d \"$dir\" ]; then\n      if [ -f \"$dir/bin/$MVN_CMD\" ]; then\n        actualDistributionDir=\"$(basename \"$dir\")\"\n        break\n      fi\n    fi\n  done\n  set -f\nfi\n\nif [ -z \"$actualDistributionDir\" ]; then\n  verbose \"Contents of $TMP_DOWNLOAD_DIR:\"\n  verbose \"$(ls -la \"$TMP_DOWNLOAD_DIR\")\"\n  die \"Could not find Maven distribution directory in extracted archive\"\nfi\n\nverbose \"Found extracted Maven distribution directory: $actualDistributionDir\"\nprintf %s\\\\n \"$distributionUrl\" >\"$TMP_DOWNLOAD_DIR/$actualDistributionDir/mvnw.url\"\nmv -- \"$TMP_DOWNLOAD_DIR/$actualDistributionDir\" \"$MAVEN_HOME\" || [ -d \"$MAVEN_HOME\" ] || die \"fail to move MAVEN_HOME\"\n\nclean || :\nexec_maven \"$@\"\n",
  "orchestrator-application-dev-properties": "#\n# Copyright (c) 2023-2025 Mariano Barcia\n#\n# Licensed under the Apache License, Version 2.0 (the \"License\");\n# you may not use this file except in compliance with the License.\n# You may obtain a copy of the License at\n#\n#     http://www.apache.org/licenses/LICENSE-2.0\n#\n# Unless required by applicable law or agreed to in writing, software\n# distributed under the License is distributed on an \"AS IS\" BASIS,\n# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n# See the License for the specific language governing permissions and\n# limitations under the License.\n#\n\n# Enable debug logging for pipeline framework\nquarkus.log.category.\"{{basePackage}}.orchestrator\".level=DEBUG\nquarkus.log.category.\"org.pipelineframework\".level=DEBUG\n\n# Pipeline configuration for development\npipeline.defaults.retry-limit=5\npipeline.defaults.retry-wait-ms=200\n\n# Disable OpenTelemetry in development\nquarkus.otel.sdk.disabled=true\nquarkus.otel.enabled=false\nquarkus.otel.traces.enabled=false\nquarkus.otel.metrics.enabled=false\nquarkus.otel.logs.enabled=false",
  "orchestrator-application-properties": "quarkus.package.main-class={{basePackage}}.orchestrator.OrchestratorApplication\n\nquarkus.hibernate-orm.enabled=false\nquarkus.datasource.jdbc=false\n\n# Pipeline Configuration\npipeline.defaults.retry-limit=10\npipeline.defaults.retry-wait-ms=500\npipeline.defaults.recover-on-failure=false\npipeline.defaults.max-backoff=30000\npipeline.defaults.jitter=false\npipeline.defaults.parallel=false\n\npipeline-cli.generate-cli=true\npipeline-cli.version=0.9.2\npipeline-cli.cli-version=0.9.2\npipeline-cli.cli-description=\"{{appName}} CLI\"\npipeline-cli.cli-name=\"{{appName}} Processing System\"\n\n# Pipeline step configurations\n{{#each steps}}\n{{#if this.parallel}}\npipeline.step.\"{{../basePackage}}.{{serviceNameForPackage}}.pipeline.{{this.serviceNameFormatted}}GrpcClientStep\".parallel=true\n{{/if}}\n{{/each}}\n\n{{#each steps}}\n# Talk to {{serviceName}} service\nquarkus.grpc.clients.{{replace serviceName \"-svc\" \"\"}}.host=localhost\nquarkus.grpc.clients.{{replace serviceName \"-svc\" \"\"}}.port={{add 8443 portOffset}}\nquarkus.grpc.clients.{{replace serviceName \"-svc\" \"\"}}.plain-text=false\nquarkus.grpc.clients.{{replace serviceName \"-svc\" \"\"}}.use-quarkus-grpc-client=true\nquarkus.grpc.clients.{{replace serviceName \"-svc\" \"\"}}.tls.enabled=true\n{{/each}}\n\n# Talk to orchestrator-svc (self)\nquarkus.grpc.clients.orchestrator-service.host=localhost\nquarkus.grpc.clients.orchestrator-service.port=8443\nquarkus.grpc.clients.orchestrator-service.plain-text=false\nquarkus.grpc.clients.orchestrator-service.use-quarkus-grpc-client=true\nquarkus.grpc.clients.orchestrator-service.tls.enabled=true\n\n# Vert.x configuration\n# Server endpoints are disabled as the orchestrator is purely a gRPC client\nquarkus.http.host-enabled=false\n\n# gRPC server configuration\n# Server endpoints are disabled as the orchestrator is purely a gRPC client\nquarkus.grpc.server.use-separate-server=false\nquarkus.grpc.server.port=8450\n# Increase msg size limit for all clients to 32M\nquarkus.grpc.clients.default.max-inbound-message-size=33554432\n\n# TLS configuration\nquarkus.tls.trust-store.jks.path=client-truststore.jks\nquarkus.tls.trust-store.jks.password=secret\n\n# Micrometer configuration\nquarkus.micrometer.export.prometheus.enabled=true\nquarkus.micrometer.export.prometheus.path=/q/metrics\nquarkus.micrometer.binder.http-server.enabled=true\nquarkus.micrometer.binder.http-client.enabled=true\n\n# OpenTelemetry configuration\nquarkus.otel.enabled=true\nquarkus.otel.exporter.otlp.endpoint=http://otel-collector:4317\nquarkus.otel.exporter.otlp.protocol=grpc\nquarkus.otel.traces.enabled=true\nquarkus.otel.metrics.enabled=true\nquarkus.otel.logs.enabled=true\n\n# Docker image\nquarkus.container-image.builder=jib\nquarkus.container-image.registry=localhost\nquarkus.container-image.group={{basePackage}}\nquarkus.container-image.name={{serviceName}}\nquarkus.container-image.tag=latest\n",
  "orchestrator-application-test-properties": "# Test-specific configuration for orchestrator\nquarkus.otel.enabled=false\nquarkus.otel.sdk.disabled=true\n\nquarkus.datasource.devservices.reuse=false",
  "orchestrator-application": "/*\n * Copyright (c) 2023-2025 Mariano Barcia\n *\n * Licensed under the Apache License, Version 2.0 (the \"License\");\n * you may not use this file except in compliance with the License.\n * You may obtain a copy of the License at\n *\n *     http://www.apache.org/licenses/LICENSE-2.0\n *\n * Unless required by applicable law or agreed to in writing, software\n * distributed under the License is distributed on an \"AS IS\" BASIS,\n * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n * See the License for the specific language governing permissions and\n * limitations under the License.\n */\n\npackage {{basePackage}}.orchestrator;\n\nimport io.quarkus.runtime.QuarkusApplication;\nimport io.smallrye.mutiny.Multi;\nimport jakarta.enterprise.context.Dependent;\nimport jakarta.inject.Inject;\nimport org.pipelineframework.PipelineExecutionService;\nimport {{basePackage}}.common.domain.{{firstInputTypeName}};\nimport picocli.CommandLine;\nimport picocli.CommandLine.Command;\nimport picocli.CommandLine.Option;\nimport java.util.concurrent.Callable;\n\n/**\n * Main application class for the orchestrator service.\n * This class provides the proper Quarkus integration for the orchestrator CLI.\n */\n@Command(name = \"orchestrator\", mixinStandardHelpOptions = true, version = \"1.0.0\",\n         description = \"{{appName}} Orchestrator Service\")\n@Dependent\npublic class OrchestratorApplication implements QuarkusApplication, Callable<Integer> {\n\n    @Option(\n        names = {\"-i\", \"--input\"}, \n        description = \"Input value for the pipeline\",\n        defaultValue = \"\"\n    )\n    String input;\n\n    @Inject\n    PipelineExecutionService pipelineExecutionService;\n\n    public static void main(String[] args) {\n        io.quarkus.runtime.Quarkus.run(OrchestratorApplication.class, args);\n    }\n\n    @Override\n    public int run(String... args) {\n        return new CommandLine(this).execute(args);\n    }\n\n    public Integer call() {\n        // Use command line option if provided, otherwise fall back to environment variable\n        String actualInput = input;\n        if (actualInput == null || actualInput.trim().isEmpty()) {\n            actualInput = System.getenv(\"PIPELINE_INPUT\");\n        }\n        \n        if (actualInput == null || actualInput.trim().isEmpty()) {\n            System.err.println(\"Input parameter is required\");\n            return CommandLine.ExitCode.USAGE;\n        }\n        \n        Multi<{{firstInputTypeName}}> inputMulti = getInputMulti(actualInput);\n\n        // Execute the pipeline with the processed input using injected service\n        pipelineExecutionService.executePipeline(inputMulti)\n            .collect().asList()\n            .await().indefinitely();\n\n        System.out.println(\"Pipeline execution completed\");\n        return CommandLine.ExitCode.OK;\n    }\n\n    // This method needs to be implemented by the user after template generation\n    // based on their specific input type and requirements\n    private Multi<{{firstInputTypeName}}> getInputMulti(String input) {\n        // TODO: User needs to implement this method after template generation\n        // Create and return appropriate Multi based on the input and first step requirements\n        // For example:\n        // {{firstInputTypeName}} inputItem = new {{firstInputTypeName}}();\n        // inputItem.setField(input);\n        // return Multi.createFrom().item(inputItem);\n        \n        throw new UnsupportedOperationException(\"Method getInputMulti needs to be implemented by user after template generation\");\n    }\n}",
  "orchestrator-pom": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<project xmlns=\"http://maven.apache.org/POM/4.0.0\"\n         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0\n                             http://maven.apache.org/xsd/maven-4.0.0.xsd\">\n    <modelVersion>4.0.0</modelVersion>\n    <artifactId>orchestrator-svc</artifactId>\n    <!-- version is not needed as it is automatically inherited from the parent-->\n    <packaging>jar</packaging>\n\n    <parent>\n        <groupId>{{basePackage}}</groupId>\n        <artifactId>{{rootProjectName}}</artifactId>\n        <version>1.0.0</version>\n        <relativePath>../pom.xml</relativePath>\n    </parent>\n\n    <dependencies>\n        {{#steps}}\n        <dependency>\n            <groupId>{{basePackage}}</groupId>\n            <artifactId>{{serviceName}}</artifactId>\n            <version>${version.pipeline}</version>\n        </dependency>\n        {{/steps}}\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-picocli</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>org.projectlombok</groupId>\n            <artifactId>lombok</artifactId>\n            <version>1.18.36</version>\n        </dependency>\n        <!-- Quarkus REST dependencies -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-grpc</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>jakarta.annotation</groupId>\n            <artifactId>jakarta.annotation-api</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>jakarta.resource</groupId>\n            <artifactId>jakarta.resource-api</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>org.testcontainers</groupId>\n            <artifactId>postgresql</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.testcontainers</groupId>\n            <artifactId>junit-jupiter</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-rest</artifactId>\n        </dependency>\n        <!-- Health check dependency -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-smallrye-health</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>jakarta.validation</groupId>\n            <artifactId>jakarta.validation-api</artifactId>\n            <version>3.1.1</version>\n        </dependency>\n        <dependency>\n            <groupId>org.assertj</groupId>\n            <artifactId>assertj-core</artifactId>\n            <version>3.26.3</version>\n            <scope>test</scope>\n        </dependency>\n    </dependencies>\n\n    <build>\n        <plugins>\n            <plugin>\n                <groupId>org.apache.maven.plugins</groupId>\n                <artifactId>maven-compiler-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>org.apache.maven.plugins</groupId>\n                <artifactId>maven-surefire-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>io.quarkus</groupId>\n                <artifactId>quarkus-maven-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>com.diffplug.spotless</groupId>\n                <artifactId>spotless-maven-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>io.smallrye</groupId>\n                <artifactId>jandex-maven-plugin</artifactId>\n            </plugin>\n        </plugins>\n    </build>\n\n</project>",
  "parent-pom": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<project xmlns=\"http://maven.apache.org/POM/4.0.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd\">\n    <modelVersion>4.0.0</modelVersion>\n    <groupId>{{basePackage}}</groupId>\n    <artifactId>{{artifactId}}</artifactId>\n    <version>1.0.0</version>\n    <packaging>pom</packaging>\n    <name>{{name}}</name>\n\n    <modules>\n        <module>common</module>\n        {{#steps}}\n        <module>{{serviceName}}</module>\n        {{/steps}}\n        <module>orchestrator-svc</module>\n    </modules>\n\n    <properties>\n        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>\n        <maven.compiler.source>21</maven.compiler.source>\n        <maven.compiler.target>21</maven.compiler.target>\n        <enablePreview>true</enablePreview>\n        <quarkus.platform.version>3.29.4</quarkus.platform.version>\n        <quarkus.platform.group-id>io.quarkus.platform</quarkus.platform.group-id>\n        <quarkus.platform.artifact-id>quarkus-bom</quarkus.platform.artifact-id>\n        <protobuf.version>4.28.2</protobuf.version>\n        <grpc.version>1.63.0</grpc.version>\n        <version.jandex>3.5.0</version.jandex>\n        <version.pipeline>0.9.2</version.pipeline>\n        <spotless.plugin.version>2.44.0</spotless.plugin.version>\n        <google-java-format.version>1.25.0</google-java-format.version>\n    </properties>\n\n    <dependencyManagement>\n        <dependencies>\n            <!-- Quarkus BOM -->\n            <dependency>\n                <groupId>${quarkus.platform.group-id}</groupId>\n                <artifactId>${quarkus.platform.artifact-id}</artifactId>\n                <version>${quarkus.platform.version}</version>\n                <type>pom</type>\n                <scope>import</scope>\n            </dependency>\n            <!-- Pipeline Framework BOM -->\n            <dependency>\n                <groupId>org.pipelineframework</groupId>\n                <artifactId>framework-parent</artifactId>\n                <version>${version.pipeline}</version>\n                <type>pom</type>\n                <scope>import</scope>\n            </dependency>\n        </dependencies>\n    </dependencyManagement>\n\n    <build>\n        <pluginManagement>\n            <plugins>\n                <plugin>\n                    <groupId>org.apache.maven.plugins</groupId>\n                    <artifactId>maven-compiler-plugin</artifactId>\n                    <version>3.14.0</version>\n                    <executions>\n                        <!-- Normal compile -->\n                        <execution>\n                            <id>default-compile</id>\n                            <phase>compile</phase>\n                            <goals><goal>compile</goal></goals>\n                        </execution>\n                    </executions>\n                    <configuration>\n                        <source>21</source>\n                        <target>21</target>\n                        <annotationProcessorPaths>\n                            <!-- lombok must come first -->\n                            <path>\n                                <groupId>org.projectlombok</groupId>\n                                <artifactId>lombok</artifactId>\n                                <version>1.18.36</version>\n                            </path>\n                            <path>\n                                <groupId>org.mapstruct</groupId>\n                                <artifactId>mapstruct-processor</artifactId>\n                                <version>1.5.5.Final</version>\n                            </path>\n                            <path>\n                                <groupId>org.pipelineframework</groupId>\n                                <artifactId>pipelineframework-deployment</artifactId>\n                                <version>${version.pipeline}</version>\n                            </path>\n                            <path>\n                                <groupId>io.quarkus</groupId>\n                                <artifactId>quarkus-extension-processor</artifactId>\n                                <version>${quarkus.platform.version}</version>\n                            </path>\n                        </annotationProcessorPaths>\n                        <compilerArgs>\n                            <arg>--enable-preview</arg>\n                        </compilerArgs>\n                    </configuration>\n                </plugin>\n                <!-- \n                The annotation processor automatically handles generation of gRPC services and client steps \n                based on the @PipelineStep annotation. Generation type (client vs server) is determined by \n                the pipeline.generate-cli property. -->\n                <plugin>\n                    <groupId>io.quarkus</groupId>\n                    <artifactId>quarkus-maven-plugin</artifactId>\n                    <version>${quarkus.platform.version}</version>\n                    <configuration>\n                        <jvmArgs>--enable-preview</jvmArgs>\n                        <sourceDir>${project.build.directory}/generated-sources/annotations</sourceDir>\n                    </configuration>\n                    <executions>\n                        <execution>\n                            <goals>\n                                <goal>build</goal>\n                                <goal>generate-code</goal>\n                                <goal>generate-code-tests</goal>\n                            </goals>\n                        </execution>\n                    </executions>\n                </plugin>\n                <plugin>\n                    <groupId>org.apache.maven.plugins</groupId>\n                    <artifactId>maven-surefire-plugin</artifactId>\n                    <version>3.5.1</version>\n                    <dependencies>\n                        <dependency>\n                            <groupId>org.ow2.asm</groupId>\n                            <artifactId>asm</artifactId>\n                            <version>9.5</version>\n                        </dependency>\n                    </dependencies>\n                    <configuration>\n                        <useSystemClassLoader>true</useSystemClassLoader>\n                        <forkCount>1</forkCount>\n                        <reuseForks>true</reuseForks>\n                        <systemPropertyVariables>\n                            <java.util.logging.manager>org.jboss.logmanager.LogManager</java.util.logging.manager>\n                        </systemPropertyVariables>\n                        <argLine>--enable-preview -XX:+EnableDynamicAgentLoading</argLine>\n                    </configuration>\n                </plugin>\n                <plugin>\n                    <groupId>com.diffplug.spotless</groupId>\n                    <artifactId>spotless-maven-plugin</artifactId>\n                    <version>${spotless.plugin.version}</version>\n                    <configuration>\n                        <java>\n                            <googleJavaFormat>\n                                <version>${google-java-format.version}</version>\n                                <style>AOSP</style>\n                            </googleJavaFormat>\n                            <removeUnusedImports/>\n                        </java>\n                        <formats>\n                            <format>\n                                <includes>\n                                    <include>src/main/**/*.java</include>\n                                    <include>jbang/*.java</include>\n                                </includes>\n                            </format>\n                        </formats>\n                    </configuration>\n                    <executions>\n                        <execution>\n                            <!-- Runs in compile phase to fail fast in case of formatting issues.-->\n                            <id>spotless-check</id>\n                            <phase>package</phase>\n                            <goals>\n                                <goal>check</goal>\n                            </goals>\n                        </execution>\n                    </executions>\n                </plugin>\n                <plugin>\n                    <groupId>org.apache.maven.plugins</groupId>\n                    <artifactId>maven-failsafe-plugin</artifactId>\n                    <version>3.5.1</version>\n                    <configuration>\n                        <argLine>--enable-preview</argLine>\n                    </configuration>\n                    <executions>\n                        <execution>\n                            <goals>\n                                <goal>integration-test</goal>\n                                <goal>verify</goal>\n                            </goals>\n                        </execution>\n                    </executions>\n                </plugin>\n                <plugin>\n                    <groupId>io.smallrye</groupId>\n                    <artifactId>jandex-maven-plugin</artifactId>\n                    <version>${version.jandex}</version>\n                    <executions>\n                        <execution>\n                            <id>make-index</id>\n                            <goals>\n                                <goal>jandex</goal>\n                            </goals>\n                        </execution>\n                    </executions>\n                </plugin>\n            </plugins>\n        </pluginManagement>\n    </build>\n\n    <profiles>\n        <profile>\n            <id>native</id>\n            <properties>\n                <quarkus.native.enabled>true</quarkus.native.enabled>\n            </properties>\n        </profile>\n    </profiles>\n\n</project>",
  "proto": "syntax = \"proto3\";\n\noption java_package = \"{{basePackage}}.grpc\";\n\n{{#unless isFirstStep}}\nimport \"{{previousStepName}}.proto\";\n{{/unless}}\n\n{{#if isFirstStep}}\nmessage {{inputTypeName}} {\n{{#each inputFields}}\n{{#if (isListType this.type)}}\n  repeated {{listInnerType this.type}} {{this.name}} = {{this.fieldNumber}};\n{{else}}\n{{#if (isMapType this.type)}}\n  map<{{mapKeyType this.type}}, {{mapValueType this.type}}> {{this.name}} = {{this.fieldNumber}};\n{{else}}\n  {{this.protoType}} {{this.name}} = {{this.fieldNumber}};\n{{/if}}\n{{/if}}\n{{/each}}\n}\n{{/if}}\n\nmessage {{outputTypeName}} {\n{{#each outputFields}}\n{{#if (isListType this.type)}}\n  repeated {{listInnerType this.type}} {{this.name}} = {{this.fieldNumber}};\n{{else}}\n{{#if (isMapType this.type)}}\n  map<{{mapKeyType this.type}}, {{mapValueType this.type}}> {{this.name}} = {{this.fieldNumber}};\n{{else}}\n  {{this.protoType}} {{this.name}} = {{this.fieldNumber}};\n{{/if}}\n{{/if}}\n{{/each}}\n}\n\nservice Process{{serviceNameFormatted}}Service {\n{{#if (isExpansion cardinality)}}\n  rpc remoteProcess({{#if isFirstStep}}{{inputTypeName}}{{else}}{{previousStepOutputTypeName}}{{/if}}) returns (stream {{outputTypeName}});\n{{/if}}\n{{#if (isReduction cardinality)}}\n  rpc remoteProcess(stream {{#if isFirstStep}}{{inputTypeName}}{{/if}}{{#unless isFirstStep}}{{previousStepOutputTypeName}}{{/unless}}) returns ({{outputTypeName}});\n{{/if}}\n{{#unless (isExpansion cardinality)}}\n{{#unless (isReduction cardinality)}}\n  rpc remoteProcess({{#if isFirstStep}}{{inputTypeName}}{{else}}{{previousStepOutputTypeName}}{{/if}}) returns ({{outputTypeName}});\n{{/unless}}\n{{/unless}}\n}",
  "readme": "# {{appName}}\n\nThis is a generated pipeline application built with the Pipeline Framework.\n\n## Prerequisites\n\n- Java 21\n- Maven 3.8+\n\n## Verifying the Generated Application\n\nTo verify that the application was generated correctly:\n\n```bash\ncd {{appName}}\n./mvnw clean verify\n```\n\nThis will compile all modules, run tests, and verify that there are no syntax or dependency issues.\n\n## Running the Application\n\n### In Development Mode\n\nUse the Quarkus plugin in IntelliJ IDEA or run with:\n\n```bash\n./mvnw compile quarkus:dev\n```\n\n## Architecture\n\nThis application follows the pipeline pattern with multiple microservices, each responsible for a specific step in the processing workflow. Services communicate via gRPC, and the orchestrator coordinates the overall pipeline execution.",
  "step-pom": "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<project xmlns=\"http://maven.apache.org/POM/4.0.0\"\n         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0\n                             http://maven.apache.org/xsd/maven-4.0.0.xsd\">\n    <modelVersion>4.0.0</modelVersion>\n    <artifactId>{{serviceName}}</artifactId>\n    <!-- version is not needed as it is automatically inherited from the parent-->\n    <packaging>jar</packaging>\n\n    <parent>\n        <groupId>{{basePackage}}</groupId>\n        <artifactId>{{rootProjectName}}</artifactId>\n        <version>1.0.0</version>\n        <relativePath>../pom.xml</relativePath>\n    </parent>\n\n    <dependencies>\n        <dependency>\n            <groupId>{{basePackage}}</groupId>\n            <artifactId>common</artifactId>\n            <version>${version.pipeline}</version>\n            <scope>compile</scope>\n        </dependency>\n        <!-- Hibernate Reactive for reactive persistence (non-virtual threads) -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-hibernate-reactive-panache</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-reactive-pg-client</artifactId>\n        </dependency>\n        <!-- Hibernate ORM for virtual thread persistence -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-hibernate-orm</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-jdbc-postgresql</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>org.projectlombok</groupId>\n            <artifactId>lombok</artifactId>\n            <version>1.18.36</version>\n            <scope>provided</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.mapstruct</groupId>\n            <artifactId>mapstruct</artifactId>\n            <version>1.5.5.Final</version>\n        </dependency>\n        <dependency>\n            <groupId>org.mapstruct</groupId>\n            <artifactId>mapstruct-processor</artifactId>\n            <version>1.5.5.Final</version>\n            <scope>provided</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-grpc</artifactId>\n        </dependency>\n        <!-- Quarkus REST dependencies -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-rest</artifactId>\n        </dependency>\n        <dependency>\n            <groupId>org.jboss.slf4j</groupId>\n            <artifactId>slf4j-jboss-logmanager</artifactId>\n            <scope>runtime</scope>\n        </dependency>\n        <!-- Health check dependency -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-smallrye-health</artifactId>\n        </dependency>\n\n        <!-- Test Dependencies -->\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-junit5</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-test-vertx</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.quarkus</groupId>\n            <artifactId>quarkus-junit5-mockito</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>io.rest-assured</groupId>\n            <artifactId>rest-assured</artifactId>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.mockito</groupId>\n            <artifactId>mockito-core</artifactId>\n            <version>5.14.2</version>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.mockito</groupId>\n            <artifactId>mockito-junit-jupiter</artifactId>\n            <version>5.14.2</version>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.mockito</groupId>\n            <artifactId>mockito-inline</artifactId>\n            <version>5.2.0</version>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.slf4j</groupId>\n            <artifactId>slf4j-nop</artifactId>\n            <version>2.0.9</version>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>org.assertj</groupId>\n            <artifactId>assertj-core</artifactId>\n            <version>3.26.3</version>\n            <scope>test</scope>\n        </dependency>\n        <dependency>\n            <groupId>jakarta.validation</groupId>\n            <artifactId>jakarta.validation-api</artifactId>\n            <version>3.1.1</version>\n        </dependency>\n    </dependencies>\n\n    <build>\n        <plugins>\n            <plugin>\n                <groupId>org.apache.maven.plugins</groupId>\n                <artifactId>maven-compiler-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>io.smallrye</groupId>\n                <artifactId>jandex-maven-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>org.apache.maven.plugins</groupId>\n                <artifactId>maven-surefire-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>io.quarkus</groupId>\n                <artifactId>quarkus-maven-plugin</artifactId>\n            </plugin>\n            <plugin>\n                <groupId>com.diffplug.spotless</groupId>\n                <artifactId>spotless-maven-plugin</artifactId>\n            </plugin>\n        </plugins>\n    </build>\n</project>",
  "step-service": "package {{basePackage}}.{{serviceNameForPackage}}.service;\n\nimport {{basePackage}}.common.domain.{{inputTypeName}};\nimport {{basePackage}}.common.domain.{{outputTypeName}};\nimport org.pipelineframework.grpc.{{grpcAdapter}};\nimport org.pipelineframework.annotation.PipelineStep;\nimport org.pipelineframework.service.{{reactiveServiceInterface}};\nimport org.pipelineframework.step.{{stepType}};\nimport io.smallrye.mutiny.Uni;\nimport io.smallrye.mutiny.Multi;\nimport jakarta.enterprise.context.ApplicationScoped;\nimport lombok.Getter;\nimport org.jboss.logging.Logger;\n\n@PipelineStep(\n    inputType = {{basePackage}}.common.domain.{{inputTypeName}}.class,\n    outputType = {{basePackage}}.common.domain.{{outputTypeName}}.class,\n    stepType = org.pipelineframework.step.{{stepType}}.class,\n    backendType = org.pipelineframework.grpc.{{grpcAdapter}}.class,\n    inboundMapper = {{basePackage}}.common.mapper.{{inputTypeName}}Mapper.class,\n    outboundMapper = {{basePackage}}.common.mapper.{{outputTypeName}}Mapper.class,\n    runOnVirtualThreads = false\n)\n@ApplicationScoped\n@Getter\npublic class Process{{serviceNamePascal}}Service\n    implements {{reactiveServiceInterface}}<{{inputTypeName}}, {{outputTypeName}}> {\n\n  @Override\n  public {{{processMethodReturnType}}} process({{{processMethodParamType}}} input) {\n    Logger logger = Logger.getLogger(getClass());\n\n    // TODO implement business logic here\n    logger.infof(\"Processing input: %s\", input);\n    \n    {{outputTypeName}} output = new {{outputTypeName}}();\n    // Set output fields based on input\n    // TODO: Add actual business logic here\n    \n    return {{{returnStatement}}};\n  }\n}\n"
};

// Handlebars template engine for the browser
(function(global, undefined) {
  "use strict";

  // If Handlebars is not available, we'll need to provide a minimal implementation
  // For now, we'll assume Handlebars is included via a script tag or similar

  // Register helper functions
  if (typeof Handlebars !== 'undefined') {
    // Register helper for replacing characters in strings
    Handlebars.registerHelper('replace', function(str, find, replace) {
        if (typeof str !== 'string') return str;
        return str.replace(new RegExp(find, 'g'), replace);
    });

    // Register helper for converting to lowercase
    Handlebars.registerHelper('lowercase', function(str) {
        return typeof str === 'string' ? str.toLowerCase() : str;
    });

    // Register helper for checking if a step is the first one
    Handlebars.registerHelper('isFirstStep', function(index) {
        return index === 0;
    });

    // Register helper for checking cardinality types
    Handlebars.registerHelper('isExpansion', function(cardinality) {
        return cardinality === 'EXPANSION';
    });

    Handlebars.registerHelper('isReduction', function(cardinality) {
        return cardinality === 'REDUCTION';
    });

    Handlebars.registerHelper('isSideEffect', function(cardinality) {
        return cardinality === 'SIDE_EFFECT';
    });

    // Register helper for checking if a type is a list
    Handlebars.registerHelper('isListType', function(type) {
        if (!type || typeof type !== 'string') return false;
        
        // Simple list check (for basic List)
        if (type === 'List') return true;
        
        // Pattern check for generic List (e.g. List<String>, List<MyCustomType>)
        return type.startsWith('List<');
    });

    // Register helper for extracting list inner type
    Handlebars.registerHelper('listInnerType', function(type) {
        if (!type || !type.startsWith('List<') || !type.endsWith('>')) {
            return type;
        }
        return type.substring(5, type.length - 1).toLowerCase();
    });

    // Register helper for checking if a type is a map
    Handlebars.registerHelper('isMapType', function(type) {
        if (!type || typeof type !== 'string') return false;
        
        // Simple map check (for basic Map)
        if (type === 'Map') return true;
        
        // Pattern check for generic Map (e.g. Map<String,Integer>, Map<MyKey,MyValue>)
        return type.startsWith('Map<');
    });

    // Register helper for extracting map key type
    Handlebars.registerHelper('mapKeyType', function(type) {
        if (!type || typeof type !== 'string') return 'string';
        
        // If it's just 'Map', return a default key type
        if (type === 'Map') return 'string';
        
        // If it doesn't follow the Map<key, value> pattern, return default
        if (!type.startsWith('Map<') || !type.includes(', ') || !type.endsWith('>')) {
            return 'string';
        }
        const parts = type.substring(4, type.length - 1).split(', ');
        let keyType = parts[0] || 'string';
        // Convert Java types to protobuf types
        switch(keyType) {
            case 'String':
                return 'string';
            case 'Integer':
                return 'int32';
            case 'Long':
                return 'int64';
            case 'Double':
                return 'double';
            case 'Boolean':
                return 'bool';
            case 'UUID':
                return 'string';
            case 'BigDecimal':
                return 'string';
            case 'Currency':
                return 'string';
            case 'Path':
                return 'string';
            case 'LocalDateTime':
                return 'string';
            case 'LocalDate':
                return 'string';
            case 'OffsetDateTime':
                return 'string';
            case 'ZonedDateTime':
                return 'string';
            case 'Instant':
                return 'string';
            case 'Duration':
                return 'string';
            case 'Period':
                return 'string';
            case 'URI':
                return 'string';
            case 'URL':
                return 'string';
            case 'File':
                return 'string';
            case 'BigInteger':
                return 'string';
            case 'AtomicInteger':
                return 'int32';
            case 'AtomicLong':
                return 'int64';
            case 'List<String>':
                return 'string';
            default:
                return keyType.toLowerCase();
        }
    });

    // Register helper for extracting map value type
    Handlebars.registerHelper('mapValueType', function(type) {
        if (!type || typeof type !== 'string') return 'string';
        
        // If it's just 'Map', return a default value type
        if (type === 'Map') return 'string';
        
        // If it doesn't follow the Map<key, value> pattern, return default
        if (!type.startsWith('Map<') || !type.includes(', ') || !type.endsWith('>')) {
            return 'string';
        }
        const parts = type.substring(4, type.length - 1).split(', ');
        let valueType = parts[1] || 'string';
        // Convert Java types to protobuf types
        switch(valueType) {
            case 'String':
                return 'string';
            case 'Integer':
                return 'int32';
            case 'Long':
                return 'int64';
            case 'Double':
                return 'double';
            case 'Boolean':
                return 'bool';
            case 'UUID':
                return 'string';
            case 'BigDecimal':
                return 'string';
            case 'Currency':
                return 'string';
            case 'Path':
                return 'string';
            case 'LocalDateTime':
                return 'string';
            case 'LocalDate':
                return 'string';
            case 'OffsetDateTime':
                return 'string';
            case 'ZonedDateTime':
                return 'string';
            case 'Instant':
                return 'string';
            case 'Duration':
                return 'string';
            case 'Period':
                return 'string';
            case 'URI':
                return 'string';
            case 'URL':
                return 'string';
            case 'File':
                return 'string';
            case 'BigInteger':
                return 'string';
            case 'AtomicInteger':
                return 'int32';
            case 'AtomicLong':
                return 'int64';
            case 'List<String>':
                return 'string';
            default:
                return valueType.toLowerCase();
        }
    });

    // Register helper to check if any field is a map type
    Handlebars.registerHelper('hasMapFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type.startsWith('Map<'));
    });

    // Register helper for checking various import flags
    Handlebars.registerHelper('hasDateFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => 
            ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period'].includes(field.type)
        );
    });

    Handlebars.registerHelper('hasBigIntegerFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type === 'BigInteger');
    });

    Handlebars.registerHelper('hasBigDecimalFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type === 'BigDecimal');
    });

    Handlebars.registerHelper('hasCurrencyFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type === 'Currency');
    });

    Handlebars.registerHelper('hasPathFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type === 'Path');
    });

    Handlebars.registerHelper('hasNetFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => ['URI', 'URL'].includes(field.type));
    });

    Handlebars.registerHelper('hasIoFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type === 'File');
    });

    Handlebars.registerHelper('hasAtomicFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => ['AtomicInteger', 'AtomicLong'].includes(field.type));
    });

    Handlebars.registerHelper('hasUtilFields', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.type === 'List<String>');
    });

    Handlebars.registerHelper('hasIdField', function(fields) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => field.name === 'id');
    });

    // Register helper to convert base package to path format
    Handlebars.registerHelper('toPath', function(basePackage) {
        return basePackage.replace(/\./g, '/');
    });

    // Register helper to format service name for proto classes
    Handlebars.registerHelper('formatForProtoClassName', function(serviceName) {
        // Convert service names like "process-customer-svc" to "ProcessCustomerSvc"
        if (!serviceName) return '';
        const parts = serviceName.split('-');
        return parts.map(part => {
            if (!part) return '';
            return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
        }).join('');
    });

    // Register helper to check if a field is an ID field
    Handlebars.registerHelper('isIdField', function(fieldName) {
        return fieldName === 'id';
    });
    
    // Register helper for sanitizing Java identifiers
    Handlebars.registerHelper('sanitizeJavaIdentifier', function(fieldName) {
        if (typeof fieldName !== 'string') return fieldName;
        
        // Reserved words in Java that need to be escaped
        const reservedWords = [
            'abstract', 'assert', 'boolean', 'break', 'byte', 'case', 'catch', 'char', 'class',
            'const', 'continue', 'default', 'do', 'double', 'else', 'enum', 'extends', 'final',
            'finally', 'float', 'for', 'goto', 'if', 'implements', 'import', 'instanceof', 'int',
            'interface', 'long', 'native', 'new', 'package', 'private', 'protected', 'public',
            'return', 'short', 'static', 'strictfp', 'super', 'switch', 'synchronized', 'this',
            'throw', 'throws', 'transient', 'try', 'void', 'volatile', 'while', 'true', 'false', 'null'
        ];
        
        // Check if it's a reserved word
        if (reservedWords.includes(fieldName.toLowerCase())) {
            return fieldName + '_';  // Append underscore to reserved words
        }
        
        // Replace invalid characters with underscore
        let sanitized = fieldName.replace(/[^a-zA-Z0-9_$]/g, '_');
        
        // Ensure it doesn't start with a number
        if (sanitized.length > 0 && /\d/.test(sanitized[0])) {
            sanitized = '_' + sanitized;
        }
        
        // If it became empty (which shouldn't happen with real input), return a default name
        if (sanitized === '') {
            sanitized = 'field';
        }
        
        return sanitized;
    });
  }

  // Browser template engine class
  class BrowserTemplateEngine {
    constructor(templates) {
        this.templates = templates || TEMPLATES;
        this.compiledTemplates = new Map();
        this.loadTemplates();
    }

    loadTemplates() {
        // In browser environment, templates are passed in as an object
        // Each template is a string that needs to be compiled
        for (const [name, templateStr] of Object.entries(this.templates)) {
            if (typeof Handlebars !== 'undefined') {
                this.compiledTemplates.set(name, Handlebars.compile(templateStr));
            } else {
                console.error("Handlebars is not available. Please include Handlebars before using BrowserTemplateEngine.");
            }
        }
    }

    render(templateName, context) {
        const template = this.compiledTemplates.get(templateName);
        if (!template) {
            throw new Error(`Template ${templateName} not found`);
        }
        return template(context);
    }

    async generateApplication(appName, basePackage, steps, fileCallback) {
        // For sequential pipeline, update input types of steps after the first one
        // to match the output type of the previous step
        for (let i = 1; i < steps.length; i++) {
            const currentStep = steps[i];
            const previousStep = steps[i - 1];
            // Set the input type of the current step to the output type of the previous step
            currentStep.inputTypeName = previousStep.outputTypeName;
            currentStep.inputFields = Array.isArray(previousStep.outputFields) 
                ? previousStep.outputFields.map(field => ({...field}))  // Shallow copy each field object to avoid shared references
                : previousStep.outputFields; // Copy input fields from previous step's outputs
        }

        // Generate parent POM
        await this.generateParentPom(appName, basePackage, steps, fileCallback);

        // Generate common module
        await this.generateCommonModule(appName, basePackage, steps, fileCallback);

        // Generate each step service
        for (let i = 0; i < steps.length; i++) {
            await this.generateStepService(appName, basePackage, steps[i], i, steps, fileCallback);
        }

        // Generate orchestrator
        await this.generateOrchestrator(appName, basePackage, steps, fileCallback);

        // Generate docker-compose
        await this.generateDockerCompose(appName, steps, fileCallback);

        // Generate utility scripts
        await this.generateUtilityScripts(fileCallback);

        // Generate observability configs
        await this.generateObservabilityConfigs(fileCallback);

        // Generate mvnw files
        await this.generateMvNWFiles(fileCallback);

        // Generate Maven wrapper files
        await this.generateMavenWrapperFiles(fileCallback);

        // Generate other files
        await this.generateOtherFiles(appName, fileCallback);
    }

    async generateParentPom(appName, basePackage, steps, fileCallback) {
        const context = {
            basePackage,
            artifactId: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-'),
            name: appName,
            steps
        };

        const rendered = this.render('parent-pom', context);
        await fileCallback('pom.xml', rendered);
    }

    async generateCommonModule(appName, basePackage, steps, fileCallback) {
        // Generate common POM
        await this.generateCommonPom(appName, basePackage, fileCallback);

        // Generate proto files for each step
        for (let i = 0; i < steps.length; i++) {
            await this.generateProtoFile(steps[i], basePackage, i, steps, fileCallback);
        }

        // Generate entities, DTOs, and mappers for each step
        for (let i = 0; i < steps.length; i++) {
            const step = steps[i];
            await this.generateDomainClasses(step, basePackage, i, fileCallback);
            await this.generateDtoClasses(step, basePackage, i, fileCallback);
            await this.generateMapperClasses(step, basePackage, i, fileCallback);
        }

        // Generate base entity
        await this.generateBaseEntity(basePackage, fileCallback);

        // Generate common converters
        await this.generateCommonConverters(basePackage, fileCallback);
    }

    async generateCommonPom(appName, basePackage, fileCallback) {
        const context = {
            basePackage,
            rootProjectName: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-')
        };

        const rendered = this.render('common-pom', context);
        await fileCallback('common/pom.xml', rendered);
    }

    async generateProtoFile(step, basePackage, stepIndex, allSteps, fileCallback) {
        // Process input fields to add field numbers
        if (step.inputFields && Array.isArray(step.inputFields)) {
            for (let i = 0; i < step.inputFields.length; i++) {
                step.inputFields[i].fieldNumber = (i + 1).toString();
            }
        }

        // Process output fields to add field numbers starting after input fields
        if (step.outputFields && Array.isArray(step.outputFields)) {
            const outputStartNumber = (step.inputFields ? step.inputFields.length : 0) + 1;
            for (let i = 0; i < step.outputFields.length; i++) {
                step.outputFields[i].fieldNumber = (outputStartNumber + i).toString();
            }
        }

        const context = {
            ...step,
            basePackage,
            isExpansion: step.cardinality === 'EXPANSION',
            isReduction: step.cardinality === 'REDUCTION',
            isFirstStep: stepIndex === 0,
            ...(stepIndex > 0 && {
                previousStepName: allSteps[stepIndex - 1].serviceName,
                previousStepOutputTypeName: allSteps[stepIndex - 1].outputTypeName
            }),
            // Format the service name properly for the proto file
            serviceNameFormatted: this.formatForClassName(
                step.name.replace('Process ', '').trim()
            )
        };

        const rendered = this.render('proto', context);
        await fileCallback(`common/src/main/proto/${step.serviceName}.proto`, rendered);
    }

    async generateDomainClasses(step, basePackage, stepIndex, fileCallback) {
        // Process input domain class only for first step
        if (stepIndex === 0 && step.inputFields && step.inputTypeName) {
            const inputContext = {
                ...step,
                basePackage,
                className: step.inputTypeName,
                fields: step.inputFields,
                hasDateFields: this.hasImportFlag(step.inputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.inputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.inputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.inputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.inputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.inputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.inputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.inputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.inputFields, ['List<String>']),
                hasIdField: step.inputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('domain', inputContext);
            const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.domain')}/${step.inputTypeName}.java`;
            await fileCallback(filePath, rendered);
        }

        // Process output domain class for all steps
        if (step.outputFields && step.outputTypeName) {
            const outputContext = {
                ...step,
                basePackage,
                className: step.outputTypeName,
                fields: step.outputFields,
                hasDateFields: this.hasImportFlag(step.outputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.outputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.outputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.outputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.outputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.outputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.outputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.outputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.outputFields, ['List<String>']),
                hasIdField: step.outputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('domain', outputContext);
            const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.domain')}/${step.outputTypeName}.java`;
            await fileCallback(filePath, rendered);
        }
    }

    async generateBaseEntity(basePackage, fileCallback) {
        const context = { basePackage };
        const rendered = this.render('base-entity', context);
        const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.domain')}/BaseEntity.java`;
        await fileCallback(filePath, rendered);
    }

    async generateDtoClasses(step, basePackage, stepIndex, fileCallback) {
        // Process input DTO class only for first step
        if (stepIndex === 0 && step.inputFields && step.inputTypeName) {
            const inputContext = {
                ...step,
                basePackage,
                className: step.inputTypeName + 'Dto',
                fields: step.inputFields,
                hasDateFields: this.hasImportFlag(step.inputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.inputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.inputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.inputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.inputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.inputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.inputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.inputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.inputFields, ['List<String>']),
                hasIdField: step.inputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('dto', inputContext);
            const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.dto')}/${step.inputTypeName}Dto.java`;
            await fileCallback(filePath, rendered);
        }

        // Process output DTO class for all steps
        if (step.outputFields && step.outputTypeName) {
            const outputContext = {
                ...step,
                basePackage,
                className: step.outputTypeName + 'Dto',
                fields: step.outputFields,
                hasDateFields: this.hasImportFlag(step.outputFields, ['LocalDate', 'LocalDateTime', 'OffsetDateTime', 'ZonedDateTime', 'Instant', 'Duration', 'Period']),
                hasBigIntegerFields: this.hasImportFlag(step.outputFields, ['BigInteger']),
                hasBigDecimalFields: this.hasImportFlag(step.outputFields, ['BigDecimal']),
                hasCurrencyFields: this.hasImportFlag(step.outputFields, ['Currency']),
                hasPathFields: this.hasImportFlag(step.outputFields, ['Path']),
                hasNetFields: this.hasImportFlag(step.outputFields, ['URI', 'URL']),
                hasIoFields: this.hasImportFlag(step.outputFields, ['File']),
                hasAtomicFields: this.hasImportFlag(step.outputFields, ['AtomicInteger', 'AtomicLong']),
                hasUtilFields: this.hasImportFlag(step.outputFields, ['List<String>']),
                hasIdField: step.outputFields.some(field => field.name === 'id')
            };

            const rendered = this.render('dto', outputContext);
            const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.dto')}/${step.outputTypeName}Dto.java`;
            await fileCallback(filePath, rendered);
        }
    }

    async generateMapperClasses(step, basePackage, stepIndex, fileCallback) {
        // Generate input mapper class only for first step (since other steps reference previous step's output)
        if (stepIndex === 0 && step.inputTypeName) {
            await this.generateMapperClass(step.inputTypeName, step, basePackage, fileCallback);
        }

        // Generate output mapper class for all steps
        if (step.outputTypeName) {
            await this.generateMapperClass(step.outputTypeName, step, basePackage, fileCallback);
        }
    }

    async generateMapperClass(className, step, basePackage, fileCallback) {
        const context = {
            ...step,
            basePackage,
            className,
            domainClass: className.replace('Dto', ''),
            dtoClass: className + 'Dto',
            grpcClass: basePackage + '.grpc.' + this.formatForProtoClassName(step.serviceName)
        };

        const rendered = this.render('mapper', context);
        const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.mapper')}/${className}Mapper.java`;
        await fileCallback(filePath, rendered);
    }

    async generateCommonConverters(basePackage, fileCallback) {
        const context = { basePackage };
        const rendered = this.render('common-converters', context);
        const filePath = `common/src/main/java/${this.toPath(basePackage + '.common.mapper')}/CommonConverters.java`;
        await fileCallback(filePath, rendered);
    }

    async generateStepService(appName, basePackage, step, stepIndex, allSteps, fileCallback) {
        const serviceNameForPackage = step.serviceName.replace('-svc', '').replace(/-/g, '_');

        // Add rootProjectName to step map
        step.rootProjectName = appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-');

        // Generate step POM
        await this.generateStepPom(step, basePackage, fileCallback);

        // Generate the service class
        await this.generateStepServiceClass(appName, basePackage, step, stepIndex, allSteps, fileCallback);

        // Generate Dockerfile
        await this.generateDockerfile(step.serviceName, fileCallback);
    }

    async generateStepPom(step, basePackage, fileCallback) {
        const context = { ...step, basePackage };
        const rendered = this.render('step-pom', context);
        const filePath = `${step.serviceName}/pom.xml`;
        await fileCallback(filePath, rendered);
    }

    async generateStepServiceClass(appName, basePackage, step, stepIndex, allSteps, fileCallback) {
        const context = { ...step };
        context.basePackage = basePackage;
        context.serviceName = step.serviceName.replace('-svc', '');
        // Convert hyphens to underscores for valid Java package names
        context.serviceNameForPackage = step.serviceName.replace('-svc', '').replace(/-/g, '_');

        // Format service name for proto-generated class names
        const protoClassName = this.formatForProtoClassName(step.serviceName);
        context.protoClassName = protoClassName;

        // Determine the inputGrpcType proto class name based on step position
        if (stepIndex === 0) {
            // For the first step, inputGrpcType comes from the same proto file
            context.inputGrpcProtoClassName = protoClassName;
        } else {
            // For subsequent steps, inputGrpcType comes from the previous step's proto file
            const previousStep = allSteps[stepIndex - 1];
            const previousProtoClassName = this.formatForProtoClassName(previousStep.serviceName);
            context.inputGrpcProtoClassName = previousProtoClassName;
        }

        // Use the serviceNameCamel field from the configuration to form the gRPC class names
        const serviceNameCamel = step.serviceNameCamel;
        // Convert camelCase to PascalCase
        const serviceNamePascal = serviceNameCamel.charAt(0).toUpperCase() + serviceNameCamel.slice(1);

        // Extract the entity name from the PascalCase service name to match proto service names
        const entityName = this.extractEntityName(serviceNamePascal);

        // For gRPC class names
        const grpcServiceName = 'MutinyProcess' + entityName + 'ServiceGrpc';
        const grpcStubName = grpcServiceName + '.MutinyProcess' + entityName + 'ServiceStub';
        const grpcImplName = grpcServiceName + '.Process' + entityName + 'ServiceImplBase';

        context.grpcServiceName = grpcServiceName;
        context.grpcStubName = grpcStubName;
        context.grpcImplName = grpcImplName;
        context.serviceNamePascal = serviceNamePascal;
        context.serviceNameFormatted = step.name;

        let reactiveServiceInterface = 'ReactiveService';
        let grpcAdapter = 'GrpcReactiveServiceAdapter';
        let processMethodReturnType = `Uni<${step.outputTypeName}>`;
        let processMethodParamType = step.inputTypeName;
        let returnStatement = 'Uni.createFrom().item(output)';

        if (step.cardinality === 'EXPANSION') {
            reactiveServiceInterface = 'ReactiveStreamingService';
            grpcAdapter = 'GrpcServiceStreamingAdapter';
            processMethodReturnType = `Multi<${step.outputTypeName}>`;
            returnStatement = 'Multi.createFrom().item(output)';
        } else if (step.cardinality === 'REDUCTION') {
            reactiveServiceInterface = 'ReactiveStreamingClientService';
            grpcAdapter = 'GrpcServiceClientStreamingAdapter';
            processMethodParamType = `Multi<${step.inputTypeName}>`;
            returnStatement = 'Uni.createFrom().item(output)';
        } else if (step.cardinality === 'SIDE_EFFECT') {
            reactiveServiceInterface = 'ReactiveService';
            grpcAdapter = 'GrpcReactiveServiceAdapter';
            returnStatement = 'Uni.createFrom().item(input)';
        }

        context.reactiveServiceInterface = reactiveServiceInterface;
        context.grpcAdapter = grpcAdapter;
        context.processMethodReturnType = processMethodReturnType;
        context.processMethodParamType = processMethodParamType;
        context.returnStatement = returnStatement;

        const rendered = this.render('step-service', context);
        const filePath = `${step.serviceName}/src/main/java/${this.toPath(basePackage + '.' + context.serviceNameForPackage + '.service')}/Process${serviceNamePascal}Service.java`;
        await fileCallback(filePath, rendered);
    }

    async generateDockerfile(serviceName, fileCallback) {
        const context = { serviceName };
        const rendered = this.render('dockerfile', context);
        await fileCallback(`${serviceName}/Dockerfile`, rendered);
    }

    async generateOrchestrator(appName, basePackage, steps, fileCallback) {
        // Generate orchestrator POM
        await this.generateOrchestratorPom(appName, basePackage, fileCallback);

        // Generate Dockerfile
        await this.generateDockerfile('orchestrator-svc', fileCallback);
    }

    async generateOrchestratorPom(appName, basePackage, fileCallback) {
        const context = {
            basePackage,
            artifactId: 'orchestrator-svc',
            rootProjectName: appName.toLowerCase().replace(/[^a-zA-Z0-9]/g, '-')
        };

        const rendered = this.render('orchestrator-pom', context);
        await fileCallback('orchestrator-svc/pom.xml', rendered);
    }

    async generateDockerCompose(appName, steps, fileCallback) {
        // Process steps to add additional properties
        const processedSteps = steps.map((step, i) => ({
            ...step,
            portNumber: 8444 + i,
            serviceNameUpperCase: step.serviceName.toUpperCase().replace(/-/g, '_')
        }));

        const context = {
            appName,
            steps: processedSteps
        };

        const rendered = this.render('docker-compose', context);
        await fileCallback('docker-compose.yml', rendered);
    }

    async generateUtilityScripts(fileCallback) {
        // Generate up-docker.sh
        const context = {};
        const upDockerContent = this.render('up-docker', context);
        await fileCallback('up-docker.sh', upDockerContent);

        // Generate down-docker.sh
        const downDockerContent = this.render('down-docker', context);
        await fileCallback('down-docker.sh', downDockerContent);

        // Generate up-local.sh
        const upLocalContent = this.render('up-local', context);
        await fileCallback('up-local.sh', upLocalContent);

        // Generate down-local.sh
        const downLocalContent = this.render('down-local', context);
        await fileCallback('down-local.sh', downLocalContent);
    }

    async generateObservabilityConfigs(fileCallback) {
        // Generate otel-collector-config.yaml
        const context = {};
        const otelContent = this.render('otel-collector-config', context);
        await fileCallback('otel-collector-config.yaml', otelContent);

        // Generate prometheus.yml
        const prometheusContent = this.render('prometheus', context);
        await fileCallback('prometheus.yml', prometheusContent);

        // Generate grafana datasources
        const grafanaDatasourcesContent = this.render('grafana-datasources', context);
        await fileCallback('grafana-datasources.yaml', grafanaDatasourcesContent);

        // Generate grafana dashboards
        const grafanaDashboardsContent = this.render('grafana-dashboards', context);
        await fileCallback('grafana-dashboards.yaml', grafanaDashboardsContent);

        // Generate tempo config
        const tempoContent = this.render('tempo', context);
        await fileCallback('tempo.yaml', tempoContent);
    }

    async generateMvNWFiles(fileCallback) {
        // Create mvnw (Unix)
        const context = {};
        const mvnwContent = this.render('mvnw', context);
        await fileCallback('mvnw', mvnwContent);

        // Create mvnw.cmd (Windows)
        const mvnwCmdContent = this.render('mvnw-cmd', context);
        await fileCallback('mvnw.cmd', mvnwCmdContent);
    }

    async generateMavenWrapperFiles(fileCallback) {
        // Create .mvn/wrapper directory and maven-wrapper.properties
        // This is a simple content for the maven wrapper properties
        const mavenWrapperProperties = `# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
wrapperVersion=3.3.4
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.11/apache-maven-3.9.11-bin.zip
wrapperUrl=https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.4/maven-wrapper-3.3.4.jar
`;
        await fileCallback('.mvn/wrapper/maven-wrapper.properties', mavenWrapperProperties);
    }

    async generateOtherFiles(appName, fileCallback) {
        // Create README
        const readmeContext = { appName };
        const readmeContent = this.render('readme', readmeContext);
        await fileCallback('README.md', readmeContent);

        // Create .gitignore
        const gitignoreContent = this.render('gitignore', {});
        await fileCallback('.gitignore', gitignoreContent);
    }

    // Utility methods
    toPath(packageName) {
        return packageName.replace(/\./g, '/');
    }

    hasImportFlag(fields, types) {
        if (!Array.isArray(fields)) return false;
        return fields.some(field => types.includes(field.type));
    }

    formatForClassName(input) {
        if (!input) return '';
        // Split by spaces and capitalize each word
        const parts = input.split(' ');
        return parts
            .filter(part => part)
            .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
            .join('');
    }

    formatForProtoClassName(input) {
        if (!input) return '';
        // Convert service names like "process-customer-svc" to "ProcessCustomerSvc"
        const parts = input.split('-');
        return parts
            .filter(part => part)
            .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
            .join('');
    }

    extractEntityName(serviceNamePascal) {
        // If it starts with "Process", return everything after "Process"
        if (serviceNamePascal.startsWith('Process')) {
            return serviceNamePascal.substring('Process'.length);
        }
        // For other cases, we'll default to the whole string
        return serviceNamePascal;
    }
  }

  // Export for browser environments
  if (typeof window !== 'undefined') {
      window.BrowserTemplateEngine = BrowserTemplateEngine;
  }

  // Export for Node.js environments (if needed)
  if (typeof module !== 'undefined' && module.exports) {
      module.exports = BrowserTemplateEngine;
  }
})(this);