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

package org.pipelineframework;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 * Service for checking the health of dependent services before pipeline execution.
 */
@ApplicationScoped
public class HealthCheckService {

    private static final Logger LOG = Logger.getLogger(HealthCheckService.class);

    /**
     * Default constructor for HealthCheckService.
     */
    public HealthCheckService() {
    }

    /**
     * Creates an SSL context based on the gRPC client's truststore configuration.
     *
     * Supports the following keystore types based on file extension:
     * - JKS (Java KeyStore) for files with .jks extension
     * - PKCS12 for files with .p12, .pfx, or .pkcs12 extensions
     * - Defaults to JKS if no recognized extension is found
     *
     * @param grpcClientName the name of the gRPC client whose truststore configuration to use
     * @return SSL context with trust anchors loaded from the configured truststore
     */
    private javax.net.ssl.SSLContext createSslContextForGrpcClient(String grpcClientName) {
        try {
            // First, try to get the truststore configuration for this specific gRPC client
            String trustStorePath = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".tls.trust-store-file", String.class)
                    .orElse(ConfigProvider.getConfig()
                            .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".tls.trust-store", String.class)
                            .orElse(null));

            String trustStorePassword = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".tls.trust-store-password", String.class)
                    .orElse(ConfigProvider.getConfig()
                            .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".tls.trust-password", String.class)
                            .orElse(null));

            // If client-specific truststore is not configured, try the global truststore
            if (trustStorePath == null) {
                trustStorePath = ConfigProvider.getConfig()
                        .getOptionalValue("quarkus.tls.trust-store.jks.path", String.class)
                        .orElse(ConfigProvider.getConfig()
                                .getOptionalValue("quarkus.tls.trust-store.path", String.class)
                                .orElse(null));

                if (trustStorePath != null) {
                    trustStorePassword = ConfigProvider.getConfig()
                            .getOptionalValue("quarkus.tls.trust-store.jks.password", String.class)
                            .orElse(ConfigProvider.getConfig()
                                    .getOptionalValue("quarkus.tls.trust-store.password", String.class)
                                    .orElse("secret")); // Default to 'secret' if not specified
                }
            }

            // Use default password if not specified
            if (trustStorePassword == null) {
                trustStorePassword = "changeit"; // Default Java truststore password
            }

            if (trustStorePath != null) {
                // Try to load the truststore from the classpath first, then as a file
                InputStream trustStoreStream = getTrustStoreStream(trustStorePath);

                if (trustStoreStream != null) {
                    try {
                        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());

                        // Determine keystore type based on file extension, default to JKS
                        String keyStoreType = determineKeyStoreType(trustStorePath);
                        java.security.KeyStore ts = java.security.KeyStore.getInstance(keyStoreType);
                        try (trustStoreStream) {
                            ts.load(trustStoreStream, trustStorePassword.toCharArray());
                        }
                        tmf.init(ts);

                        javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
                        sslContext.init(null, tmf.getTrustManagers(), null);

                        LOG.info("Using custom truststore for gRPC client '" + grpcClientName + "'");
                        return sslContext;
                    } catch (Exception e) {
                        LOG.warn("Failed to load truststore from: " + trustStorePath, e);
                    }
                } else {
                    LOG.warn("Truststore file not found for gRPC client '" + grpcClientName + "': " + trustStorePath);
                }
            }

            // If no specific truststore is configured, fall back to the default
            return javax.net.ssl.SSLContext.getDefault();
        } catch (Exception e) {
            LOG.warn("Failed to create SSL context with custom truststore, falling back to default", e);
            try {
                return javax.net.ssl.SSLContext.getDefault();
            } catch (Exception ex) {
                LOG.error("Failed to get default SSL context", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private static InputStream getTrustStoreStream(String trustStorePath) throws FileNotFoundException {
        InputStream trustStoreStream = null;

        // Try loading from classpath first
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader != null) {
            trustStoreStream = classLoader.getResourceAsStream(trustStorePath);
        }

        if (trustStoreStream == null) {
            // If not found in classpath, try as a regular file
            java.io.File trustStoreFile = new java.io.File(trustStorePath);
            if (trustStoreFile.exists()) {
                trustStoreStream = new java.io.FileInputStream(trustStoreFile);
            }
        }
        return trustStoreStream;
    }

    /**
     * Determines the keystore type based on the file extension.
     *
     * Currently supports:
     * - .p12, .pfx: PKCS12 format
     * - .jks: JKS format
     * - .pkcs12: PKCS12 format
     * - Default: JKS format
     *
     * @param trustStorePath the path to the truststore file
     * @return the appropriate keystore type string
     */
    private String determineKeyStoreType(String trustStorePath) {
        if (trustStorePath != null) {
            String lowerPath = trustStorePath.toLowerCase();
            if (lowerPath.endsWith(".p12") || lowerPath.endsWith(".pfx") || lowerPath.endsWith(".pkcs12")) {
                return "PKCS12";
            } else if (lowerPath.endsWith(".jks")) {
                return "JKS";
            }
        }
        // Default to JKS if no specific extension is recognized
        return "JKS";
    }

    /**
     * Checks the health of all dependent services before running the pipeline.
     * This method inspects each step to detect gRPC client dependencies and checks their health endpoints.
     * Retries every 5 seconds for up to 2 minutes before giving up.
     *
     * @param steps the list of pipeline steps to check for dependent services
     * @return true if all dependent services are healthy, false otherwise
     */
    public boolean checkHealthOfDependentServices(List<Object> steps) {
        LOG.info("Checking health of dependent services before pipeline execution...");

        // Extract all gRPC client names from the steps
        Set<String> grpcClientNames = new HashSet<>();
        Set<RestClientInfo> restClients = new HashSet<>();
        for (Object step : steps) {
            if (step != null) {
                Set<String> stepClientNames = extractGrpcClientNames(step);
                grpcClientNames.addAll(stepClientNames);
                restClients.addAll(extractRestClientInfos(step));
            }
        }
        if (LOG.isInfoEnabled()) {
            LOG.infof("Detected gRPC clients: %s", grpcClientNames);
            LOG.infof("Detected REST clients: %s",
                restClients.stream().map(RestClientInfo::configKey).toList());
        }

        if (grpcClientNames.isEmpty() && restClients.isEmpty()) {
            LOG.info("No gRPC or REST client dependencies found. Proceeding with pipeline execution.");
            return true;
        }

        // Create a Uni that performs the health checks and apply retry logic
        Uni<Boolean> healthCheckUni = Uni.createFrom().item(Unchecked.supplier(() -> {
            boolean allHealthy = true;
            Set<String> unhealthyServices = new HashSet<>();

            // Check the health of each gRPC client endpoint
            for (String grpcClientName : grpcClientNames) {
                if (!isGrpcClientServiceHealthy(grpcClientName)) {
                    allHealthy = false;
                    unhealthyServices.add("grpc:" + grpcClientName);
                }
            }

            // Check the health of each REST client endpoint
            for (RestClientInfo restClient : restClients) {
                if (!isRestClientServiceHealthy(restClient)) {
                    allHealthy = false;
                    unhealthyServices.add("rest:" + restClient.configKey());
                }
            }

            if (allHealthy) {
                LOG.info("All dependent services are healthy. Proceeding with pipeline execution.");
                return true;
            } else {
                LOG.warnf("Health check failed. Services not healthy: %s", unhealthyServices);
                throw new RuntimeException("Health check failed");
            }
        }))
        .onFailure().retry()
        .withBackOff(Duration.ofSeconds(5), Duration.ofSeconds(5))
        .atMost(24); // 24 attempts with 5s backoff = ~2 minutes

        try {
            return healthCheckUni.await().indefinitely();
        } catch (Exception e) {
            LOG.errorf("Health checks failed after maximum attempts. Pipeline execution will not proceed. %s", e.getMessage());
            return false;
        }
    }

    /**
     * Checks the health of a gRPC client service by attempting to access its health endpoint.
     * Uses the gRPC client configuration from the application properties to determine the host and port.
     *
     * @param grpcClientName the name of the gRPC client
     * @return true if the service is healthy, false otherwise
     */
    private boolean isGrpcClientServiceHealthy(String grpcClientName) {
        try {
            // Get host and port from configuration
            String host = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".host", String.class)
                    .orElse("localhost");

            String portStr = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".port", String.class)
                    .orElse("8080");

            int port = Integer.parseInt(portStr);

            // Determine if the service uses HTTPS based on TLS configuration
            boolean useTls = isGrpcClientTlsEnabled(grpcClientName);

            // Get health endpoint path from configuration, default to /q/health
            String healthPath = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".health-path", String.class)
                    .orElse("/q/health");

            // Construct the health check URL
            String protocol = useTls ? "https" : "http";
            String healthUrl = String.format("%s://%s:%d%s", protocol, host, port, healthPath);
            String locationLabel = host + ":" + port;
            return checkHttpServiceHealth("gRPC", grpcClientName, URI.create(healthUrl), locationLabel,
                () -> resolveGrpcSslContext(grpcClientName));
        } catch (Exception e) {
            LOG.info("✗ Error checking health of gRPC client '" + grpcClientName + "' service. Error: " + e.getMessage());
            return false;
        }
    }

    /**
     * Finds gRPC client names declared on the given step by scanning its fields and superclasses for the `@GrpcClient` annotation.
     *
     * If the annotation's value is empty, the field name is used as the client name.
     *
     * @param step the step instance to inspect for gRPC client fields
     * @return a set of discovered gRPC client names
     */
    public Set<String> extractGrpcClientNames(Object step) {
        Set<String> grpcClientNames = new HashSet<>();

	    // Walk the class hierarchy to check all fields including superclasses
        Class<?> currentClass = step.getClass();
        while (currentClass != null && currentClass != Object.class) {
            // Check all declared fields in the current class
            for (Field field : currentClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(io.quarkus.grpc.GrpcClient.class)) {
                    io.quarkus.grpc.GrpcClient grpcClientAnnotation = field.getAnnotation(io.quarkus.grpc.GrpcClient.class);
                    String clientName = grpcClientAnnotation.value();

                    // If the value is empty, use the field name as default
                    if (clientName.isEmpty()) {
                        clientName = field.getName();
                    }

                    grpcClientNames.add(clientName);
                }
            }
            // Move to the superclass
            currentClass = currentClass.getSuperclass();
        }

        return grpcClientNames;
    }

    /**
     * Finds REST client config keys declared on the given step by scanning its fields and superclasses for the `@RestClient` annotation.
     *
     * If the `@RegisterRestClient` configKey is empty, the fully qualified interface name is used.
     *
     * @param step the step instance to inspect for REST client fields
     * @return a set of discovered REST client infos
     */
    public Set<RestClientInfo> extractRestClientInfos(Object step) {
        Set<RestClientInfo> restClients = new HashSet<>();

        Class<?> currentClass = step.getClass();
        while (currentClass != null && currentClass != Object.class) {
            for (Field field : currentClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(org.eclipse.microprofile.rest.client.inject.RestClient.class)) {
                    Class<?> restClientType = field.getType();
                    RestClientInfo info = resolveRestClientInfo(restClientType);
                    if (info != null) {
                        restClients.add(info);
                    }
                }
            }
            currentClass = currentClass.getSuperclass();
        }

        return restClients;
    }

    private RestClientInfo resolveRestClientInfo(Class<?> restClientType) {
        if (restClientType == null) {
            return null;
        }
        org.eclipse.microprofile.rest.client.inject.RegisterRestClient register =
            restClientType.getAnnotation(org.eclipse.microprofile.rest.client.inject.RegisterRestClient.class);
        String configKey = null;
        if (register != null && register.configKey() != null && !register.configKey().isBlank()) {
            configKey = register.configKey();
        }
        if (configKey == null || configKey.isBlank()) {
            configKey = restClientType.getName();
        }
        return new RestClientInfo(configKey, restClientType.getName());
    }

    private boolean isRestClientServiceHealthy(RestClientInfo restClient) {
        String baseUrl = resolveRestClientBaseUrl(restClient);
        if (baseUrl == null || baseUrl.isBlank()) {
            LOG.info("✗ REST client '" + restClient.configKey() + "' base URL is not configured.");
            return false;
        }

        String healthPath = resolveRestClientHealthPath(restClient);
        URI baseUri;
        try {
            baseUri = URI.create(baseUrl);
        } catch (IllegalArgumentException e) {
            LOG.info("✗ REST client '" + restClient.configKey() + "' has invalid base URL: " + baseUrl);
            return false;
        }

        String combinedPath = combinePaths(baseUri.getPath(), healthPath);
        URI healthUri = URI.create(String.format("%s://%s:%d%s",
            baseUri.getScheme(),
            baseUri.getHost(),
            baseUri.getPort() == -1 ? ("https".equalsIgnoreCase(baseUri.getScheme()) ? 443 : 80) : baseUri.getPort(),
            combinedPath));
        LOG.info("Checking REST client '" + restClient.configKey() + "' at " + healthUri);

        return checkHttpServiceHealth("REST", restClient.configKey(), healthUri, healthUri.toString(),
            () -> resolveRestSslContext(restClient));
    }

    private boolean checkHttpServiceHealth(
        String clientType,
        String clientName,
        URI healthUri,
        String locationLabel,
        Supplier<javax.net.ssl.SSLContext> sslContextSupplier
    ) {
        boolean useTls = "https".equalsIgnoreCase(healthUri.getScheme());

        HttpClient.Builder clientBuilder = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5));

        if (useTls) {
            javax.net.ssl.SSLContext sslContext;
            if (sslContextSupplier != null) {
                sslContext = sslContextSupplier.get();
            } else {
                try {
                    sslContext = javax.net.ssl.SSLContext.getDefault();
                } catch (Exception ex) {
                    LOG.error("Failed to get default SSL context", ex);
                    return false;
                }
            }
            if (sslContext != null) {
                clientBuilder.sslContext(sslContext);
            }
        }

        HttpClient serviceHttpClient = clientBuilder.build();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(healthUri)
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

        try {
            CompletableFuture<HttpResponse<String>> responseFuture =
                serviceHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

            HttpResponse<String> response = responseFuture
                .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                .join();

            boolean isHealthy = response.statusCode() == 200;
            String label = clientType + " client '" + clientName + "'";
            if (isHealthy) {
                LOG.info("✓ " + label + " service at " + locationLabel + " is healthy");
            } else {
                LOG.info("✗ " + label + " service at " + locationLabel +
                    " is not healthy. Status: " + response.statusCode());
            }
            return isHealthy;
        } catch (java.util.concurrent.CompletionException e) {
            if (e.getCause() instanceof java.util.concurrent.TimeoutException) {
                LOG.info("✗ " + clientType + " client '" + clientName + "' service health check timed out");
            } else {
                LOG.info("✗ " + clientType + " client '" + clientName + "' service is not accessible. Error: " + e.getMessage());
            }
            return false;
        } catch (Exception e) {
            LOG.info("✗ Error checking health of " + clientType + " client '" + clientName +
                "' service. Error: " + e.getMessage());
            return false;
        }
    }

    private javax.net.ssl.SSLContext createSslContextFromGlobalTrustStore() {
        try {
            String trustStorePath = ConfigProvider.getConfig()
                .getOptionalValue("quarkus.tls.trust-store.jks.path", String.class)
                .orElse(ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.tls.trust-store.path", String.class)
                    .orElse(null));

            if (trustStorePath == null || trustStorePath.isBlank()) {
                return javax.net.ssl.SSLContext.getDefault();
            }

            String trustStorePassword = ConfigProvider.getConfig()
                .getOptionalValue("quarkus.tls.trust-store.jks.password", String.class)
                .orElse(ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.tls.trust-store.password", String.class)
                    .orElse("secret"));

            InputStream trustStoreStream = getTrustStoreStream(trustStorePath);
            if (trustStoreStream == null) {
                LOG.warn("Truststore file not found: " + trustStorePath);
                return javax.net.ssl.SSLContext.getDefault();
            }

            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());

            String keyStoreType = determineKeyStoreType(trustStorePath);
            java.security.KeyStore ts = java.security.KeyStore.getInstance(keyStoreType);
            try (trustStoreStream) {
                ts.load(trustStoreStream, trustStorePassword.toCharArray());
            }
            tmf.init(ts);

            javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            LOG.warn("Failed to create SSL context from global truststore, falling back to default", e);
            try {
                return javax.net.ssl.SSLContext.getDefault();
            } catch (Exception ex) {
                LOG.error("Failed to get default SSL context", ex);
                throw new RuntimeException(ex);
            }
        }
    }

    private boolean isGrpcClientTlsEnabled(String grpcClientName) {
        if (resolveGrpcTlsConfigurationName(grpcClientName) != null) {
            return true;
        }
        return ConfigProvider.getConfig()
            .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".tls.enabled", Boolean.class)
            .orElse(false);
    }

    private javax.net.ssl.SSLContext resolveGrpcSslContext(String grpcClientName) {
        String tlsConfigName = resolveGrpcTlsConfigurationName(grpcClientName);
        if (tlsConfigName != null) {
            javax.net.ssl.SSLContext tlsContext = createSslContextFromTlsConfigName(tlsConfigName);
            if (tlsContext != null) {
                return tlsContext;
            }
        }
        return createSslContextForGrpcClient(grpcClientName);
    }

    private javax.net.ssl.SSLContext resolveRestSslContext(RestClientInfo restClient) {
        String tlsConfigName = resolveRestTlsConfigurationName(restClient);
        if (tlsConfigName != null) {
            javax.net.ssl.SSLContext tlsContext = createSslContextFromTlsConfigName(tlsConfigName);
            if (tlsContext != null) {
                return tlsContext;
            }
        }
        return createSslContextFromGlobalTrustStore();
    }

    private String resolveGrpcTlsConfigurationName(String grpcClientName) {
        String directKey = "quarkus.grpc.clients." + grpcClientName + ".tls-configuration-name";
        String tlsName = ConfigProvider.getConfig()
            .getOptionalValue(directKey, String.class)
            .orElse(null);
        if (tlsName != null && !tlsName.isBlank()) {
            return tlsName;
        }
        return resolvePipelineTlsConfigurationName();
    }

    private String resolveRestTlsConfigurationName(RestClientInfo restClient) {
        String key = restClient.configKey();
        String directKey = "quarkus.rest-client." + key + ".tls-configuration-name";
        String tlsName = ConfigProvider.getConfig()
            .getOptionalValue(directKey, String.class)
            .orElse(null);
        if (tlsName == null) {
            String quotedKey = "quarkus.rest-client.\"" + key + "\".tls-configuration-name";
            tlsName = ConfigProvider.getConfig()
                .getOptionalValue(quotedKey, String.class)
                .orElse(null);
        }
        if (tlsName != null && !tlsName.isBlank()) {
            return tlsName;
        }
        return resolvePipelineTlsConfigurationName();
    }

    private String resolvePipelineTlsConfigurationName() {
        String tlsName = ConfigProvider.getConfig()
            .getOptionalValue("pipeline.client.tls-configuration-name", String.class)
            .orElse(null);
        return tlsName != null && !tlsName.isBlank() ? tlsName : null;
    }

    private javax.net.ssl.SSLContext createSslContextFromTlsConfigName(String tlsConfigName) {
        if (tlsConfigName == null || tlsConfigName.isBlank()) {
            return null;
        }
        try {
            Boolean trustAll = getTlsConfigValue(tlsConfigName, "trust-all", Boolean.class);
            if (Boolean.TRUE.equals(trustAll)) {
                return createTrustAllSslContext();
            }

            String trustStorePath = getTlsConfigValue(tlsConfigName, "trust-store.jks.path", String.class);
            if (trustStorePath == null || trustStorePath.isBlank()) {
                trustStorePath = getTlsConfigValue(tlsConfigName, "trust-store.path", String.class);
            }
            if (trustStorePath == null || trustStorePath.isBlank()) {
                return null;
            }

            String trustStorePassword = getTlsConfigValue(tlsConfigName, "trust-store.jks.password", String.class);
            if (trustStorePassword == null || trustStorePassword.isBlank()) {
                trustStorePassword = getTlsConfigValue(tlsConfigName, "trust-store.password", String.class);
            }
            if (trustStorePassword == null || trustStorePassword.isBlank()) {
                trustStorePassword = "secret";
            }

            InputStream trustStoreStream = getTrustStoreStream(trustStorePath);
            if (trustStoreStream == null) {
                LOG.warn("Truststore file not found: " + trustStorePath);
                return null;
            }

            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(
                javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());

            String keyStoreType = determineKeyStoreType(trustStorePath);
            java.security.KeyStore ts = java.security.KeyStore.getInstance(keyStoreType);
            try (trustStoreStream) {
                ts.load(trustStoreStream, trustStorePassword.toCharArray());
            }
            tmf.init(ts);

            javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            LOG.warn("Failed to create SSL context from TLS config '" + tlsConfigName + "'", e);
            return null;
        }
    }

    private <T> T getTlsConfigValue(String tlsConfigName, String suffix, Class<T> type) {
        String directKey = "quarkus.tls." + tlsConfigName + "." + suffix;
        T value = ConfigProvider.getConfig()
            .getOptionalValue(directKey, type)
            .orElse(null);
        if (value != null) {
            return value;
        }
        String quotedKey = "quarkus.tls.\"" + tlsConfigName + "\"." + suffix;
        return ConfigProvider.getConfig()
            .getOptionalValue(quotedKey, type)
            .orElse(null);
    }

    private javax.net.ssl.SSLContext createTrustAllSslContext() {
        try {
            javax.net.ssl.TrustManager[] trustAll = new javax.net.ssl.TrustManager[] {
                new javax.net.ssl.X509TrustManager() {
                    @Override
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                        return new java.security.cert.X509Certificate[0];
                    }

                    @Override
                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                    }

                    @Override
                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
                    }
                }
            };
            javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
            sslContext.init(null, trustAll, new java.security.SecureRandom());
            return sslContext;
        } catch (Exception e) {
            LOG.warn("Failed to create trust-all SSL context", e);
            return null;
        }
    }

    private String resolveRestClientBaseUrl(RestClientInfo restClient) {
        String key = restClient.configKey();
        String url = resolveRestClientConfigValue(key, "url");
        if (url != null && !url.isBlank()) {
            return url;
        }
        return resolveRestClientConfigValue(key, "uri");
    }

    private String resolveRestClientHealthPath(RestClientInfo restClient) {
        String key = restClient.configKey();
        String path = resolveRestClientConfigValue(key, "health-path");
        if (path != null && !path.isBlank()) {
            return path;
        }
        return ConfigProvider.getConfig()
            .getOptionalValue("quarkus.rest-client.health-path", String.class)
            .orElse("/q/health");
    }

    private String resolveRestClientConfigValue(String configKey, String suffix) {
        String directKey = "quarkus.rest-client." + configKey + "." + suffix;
        String value = ConfigProvider.getConfig()
            .getOptionalValue(directKey, String.class)
            .orElse(null);
        if (value != null) {
            return value;
        }

        String quotedKey = "quarkus.rest-client.\"" + configKey + "\"." + suffix;
        return ConfigProvider.getConfig()
            .getOptionalValue(quotedKey, String.class)
            .orElse(null);
    }

    private String combinePaths(String basePath, String healthPath) {
        String normalizedBase = (basePath == null || basePath.isBlank()) ? "" : basePath;
        String normalizedHealth = (healthPath == null || healthPath.isBlank()) ? "/q/health" : healthPath;

        if (!normalizedHealth.startsWith("/")) {
            normalizedHealth = "/" + normalizedHealth;
        }

        if (normalizedBase.isEmpty() || "/".equals(normalizedBase)) {
            return normalizedHealth;
        }

        if (normalizedBase.endsWith("/") && normalizedHealth.startsWith("/")) {
            return normalizedBase + normalizedHealth.substring(1);
        }
        if (!normalizedBase.endsWith("/") && !normalizedHealth.startsWith("/")) {
            return normalizedBase + "/" + normalizedHealth;
        }
        return normalizedBase + normalizedHealth;
    }

    private record RestClientInfo(String configKey, String interfaceName) {}
}
