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
	 * Creates an SSLContext that accepts all certificates (insecure).
	 * <p>
	 * If building the permissive context fails, returns the platform default SSLContext.
	 *
	 * @return an SSLContext that does not validate peer certificates, or the default SSLContext if creation of an insecure context and retrieval of the default SSLContext fail
	 * @throws RuntimeException if both creation of the insecure context and retrieval of the default SSLContext fail
	 */
    private javax.net.ssl.SSLContext createInsecureSslContext() {
        try {
            javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
            javax.net.ssl.TrustManager[] trustAllCerts = new javax.net.ssl.TrustManager[] {
                    new javax.net.ssl.X509TrustManager() {
                        /**
                         * List the certificate issuer authorities trusted by this trust manager.
                         *
                         * @return an array of trusted CA issuer certificates, or `null` if no specific issuers are defined
                         */
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return null;
                        }
                        /**
 * Accepts any client certificate chain without performing validation.
 *
 * @param certs the client certificate chain presented during the TLS handshake; may be null or empty
 * @param authType the key exchange algorithm used for authentication (for example "RSA"); ignored
 */
public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) { }
                        /**
 * Accepts any X.509 certificate chain without performing validation.
 *
 * @param certs the certificate chain presented by the peer, may be null or empty
 * @param authType the authentication type based on the certificate, typically a key exchange algorithm (e.g. "RSA")
 */
public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) { }
                    }
            };
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            return sslContext;
        } catch (Exception e) {
            LOG.warn("Failed to create insecure SSL context, proceeding with default", e);
            try {
	            return javax.net.ssl.SSLContext.getDefault();
            } catch (Exception ex) {
                LOG.error("Failed to get default SSL context", ex);
                throw new RuntimeException(ex);
            }
        }
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
        for (Object step : steps) {
            if (step != null) {
                Set<String> stepClientNames = extractGrpcClientNames(step);
                grpcClientNames.addAll(stepClientNames);
            }
        }

        if (grpcClientNames.isEmpty()) {
            LOG.info("No gRPC client dependencies found. Proceeding with pipeline execution.");
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
                    unhealthyServices.add(grpcClientName);
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
            boolean useTls = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".tls.enabled", Boolean.class)
                    .orElse(false);

            // Get health endpoint path from configuration, default to /q/health
            String healthPath = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".health-path", String.class)
                    .orElse("/q/health");

            // Use a custom SSL context based on service configuration
            boolean allowInsecureSSL = ConfigProvider.getConfig()
                    .getOptionalValue("quarkus.grpc.clients." + grpcClientName + ".allow-insecure-ssl", Boolean.class)
                    .orElse(
                            ConfigProvider.getConfig()
                                    .getOptionalValue("quarkus.grpc.clients.allow-insecure-ssl", Boolean.class)
                                    .orElse(false));

            HttpClient.Builder clientBuilder = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5));

            if (useTls) {
                javax.net.ssl.SSLContext sslContext;
                if (allowInsecureSSL) {
                    LOG.warn("Using insecure SSL context with disabled certificate validation for gRPC client '" +
                            grpcClientName + "'. This setting MUST NOT be enabled in production!");
                    sslContext = createInsecureSslContext();
                } else {
                    // Try to use the same truststore configuration as the gRPC client
                    sslContext = createSslContextForGrpcClient(grpcClientName);
                }
                clientBuilder.sslContext(sslContext);
            }

            HttpClient serviceHttpClient = clientBuilder.build();

            // Construct the health check URL
            String protocol = useTls ? "https" : "http";
            String healthUrl = String.format("%s://%s:%d%s", protocol, host, port, healthPath);

            // Create and execute the HTTP request
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(healthUrl))
                    .timeout(Duration.ofSeconds(5))  // Increase timeout
                    .GET()
                    .build();

            // Use sendAsync with a timeout to avoid blocking indefinitely and handle interruptions better
            CompletableFuture<HttpResponse<String>> responseFuture =
                    serviceHttpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

            // Wait for the response with timeout
            HttpResponse<String> response = responseFuture
                    .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                    .join(); // This is safe in this context since we have timeout handling

            boolean isHealthy = response.statusCode() == 200;
            if (isHealthy) {
                LOG.info("✓ gRPC client '" + grpcClientName + "' service at " + host + ":" + port + " is healthy");
            } else {
                LOG.info("✗ gRPC client '" + grpcClientName + "' service at " + host + ":" + port + " is not healthy. Status: " + response.statusCode());
            }

            return isHealthy;
        } catch (java.util.concurrent.CompletionException e) {
            // Handle TimeoutException and other completion-related exceptions
            if (e.getCause() instanceof java.util.concurrent.TimeoutException) {
                LOG.info("✗ gRPC client '" + grpcClientName + "' service health check timed out");
            } else {
                LOG.info("✗ gRPC client '" + grpcClientName + "' service is not accessible. Error: " + e.getMessage());
            }
            return false;
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
}
