#!/usr/bin/env pwsh

$ErrorActionPreference = "Stop"

$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$tempDir = Join-Path $rootDir "target/dev-certs-tmp"
$outputDir = Join-Path $rootDir "target/dev-certs"

if (Test-Path $tempDir) {
    Remove-Item -Path $tempDir -Recurse -Force
}
New-Item -Path $tempDir -ItemType Directory | Out-Null
New-Item -Path $outputDir -ItemType Directory -Force | Out-Null

$keystore = Join-Path $tempDir "server-keystore.p12"
$certPem = Join-Path $tempDir "quarkus-cert.pem"
$truststore = Join-Path $tempDir "client-truststore.jks"

keytool `
    -genkeypair `
    -alias server `
    -keyalg RSA `
    -keysize 2048 `
    -storetype PKCS12 `
    -keystore $keystore `
    -storepass secret `
    -keypass secret `
    -dname "CN=localhost, OU=Dev, O=CSV Payments PoC, L=San Francisco, ST=CA, C=US" `
    -ext "SAN=dns:localhost,ip:127.0.0.1,ip:::1" `
    -validity 365

keytool `
    -exportcert `
    -alias server `
    -keystore $keystore `
    -storepass secret `
    -rfc `
    -file $certPem

keytool `
    -importcert `
    -alias server `
    -file $certPem `
    -keystore $truststore `
    -storepass secret `
    -noprompt

$services = @(
    "persistence-svc",
    "input-csv-file-processing-svc",
    "payments-processing-svc",
    "payment-status-svc",
    "output-csv-file-processing-svc",
    "orchestrator-svc"
)

foreach ($svc in $services) {
    $svcDir = Join-Path $outputDir $svc
    New-Item -Path $svcDir -ItemType Directory -Force | Out-Null
    Copy-Item -Path $keystore -Destination (Join-Path $svcDir "server-keystore.jks") -Force
}

$orchestratorDir = Join-Path $outputDir "orchestrator-svc"
New-Item -Path $orchestratorDir -ItemType Directory -Force | Out-Null
Copy-Item -Path $truststore -Destination (Join-Path $orchestratorDir "client-truststore.jks") -Force

Remove-Item -Path $tempDir -Recurse -Force

Write-Host "Development certificates generated successfully."
