/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.processor.renderer;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Factory for creating cloud function handler renderers based on provider configuration.
 *
 * <p>Supports the following cloud providers:</p>
 * <ul>
 *     <li>{@code aws} - AWS Lambda (default)</li>
 *     <li>{@code azure} - Azure Functions</li>
 *     <li>{@code gcp} - Google Cloud Functions</li>
 * </ul>
 *
 * <p>Provider selection precedence:</p>
 * <ol>
 *     <li>Explicit {@code pipeline.function.provider} system property</li>
 *     <li>Auto-detection based on Quarkus extension in classpath</li>
 *     <li>Default to AWS Lambda</li>
 * </ol>
 */
public final class FunctionHandlerRendererFactory {

    private static final String PROVIDER_AWS = "aws";
    private static final String PROVIDER_AZURE = "azure";
    private static final String PROVIDER_GCP = "gcp";
    private static final String PROVIDER_DEFAULT = PROVIDER_AWS;

    private static final String EXTENSION_AWS = "io.quarkus.amazon.lambda.runtime.AmazonLambdaHandler";
    private static final String EXTENSION_AZURE = "io.quarkus.azure.functions.runtime.AzureFunctionsHandler";
    private static final String EXTENSION_GCP = "com.google.cloud.functions.HttpFunction";

    /**
     * Private constructor to prevent instantiation of this utility factory class.
     */
    private FunctionHandlerRendererFactory() {
        // Prevent instantiation
    }

    /**
     * Creates a renderer based on explicit provider or auto-detection.
     *
     * @return configured function handler renderer
     */
    public static AbstractFunctionHandlerRenderer createRenderer() {
        String provider = getExplicitProvider();
        if (provider != null) {
            return createRendererForProvider(provider);
        }

        provider = detectProviderFromClasspath();
        if (provider != null) {
            return createRendererForProvider(provider);
        }

        return createRendererForProvider(PROVIDER_DEFAULT);
    }

    /**
     * Selects and returns an orchestrator renderer for cloud functions using explicit configuration or auto-detection.
     *
     * <p>The provider selection follows this precedence: explicit system properties (`pipeline.function.provider`,
     * legacy `tpf.function.provider`), auto-detection from the classpath, then the default provider ("aws").</p>
     *
     * @return the orchestrator function handler renderer for the resolved provider
     */
    public static AbstractOrchestratorFunctionHandlerRenderer createOrchestratorRenderer() {
        String provider = getExplicitProvider();
        if (provider != null) {
            return createOrchestratorRendererForProvider(provider);
        }

        provider = detectProviderFromClasspath();
        if (provider != null) {
            return createOrchestratorRendererForProvider(provider);
        }

        return createOrchestratorRendererForProvider(PROVIDER_DEFAULT);
    }

    /**
     * Create a function handler renderer for the specified cloud provider.
     *
     * @param provider the provider name; accepted values are "aws", "azure", or "gcp" (case-insensitive, surrounding whitespace is ignored)
     * @return an AbstractFunctionHandlerRenderer implementation configured for the given provider
     * @throws IllegalArgumentException if {@code provider} is {@code null} or not one of the supported values
     */
    public static AbstractFunctionHandlerRenderer createRendererForProvider(String provider) {
        if (provider == null) {
            throw new IllegalArgumentException("Provider must not be null. Expected one of: aws, azure, gcp");
        }
        String normalized = provider.toLowerCase(Locale.ROOT).trim();

        return switch (normalized) {
            case PROVIDER_AWS -> new AwsLambdaFunctionHandlerRenderer();
            case PROVIDER_AZURE -> new AzureFunctionsHandlerRenderer();
            case PROVIDER_GCP -> new GoogleCloudFunctionsHandlerRenderer();
            default -> throw new IllegalArgumentException(
                "Unsupported function provider: '" + provider + "'. " +
                "Expected one of: aws, azure, gcp");
        };
    }

    /**
     * Create an orchestrator renderer for the given cloud provider.
     *
     * @param provider the provider identifier; accepted values are "aws", "azure", or "gcp"
     * @return an AbstractOrchestratorFunctionHandlerRenderer implementation for the specified provider
     * @throws IllegalArgumentException if {@code provider} is null or not one of "aws", "azure", or "gcp"
     */
    public static AbstractOrchestratorFunctionHandlerRenderer createOrchestratorRendererForProvider(String provider) {
        if (provider == null) {
            throw new IllegalArgumentException("Provider must not be null. Expected one of: aws, azure, gcp");
        }
        String normalized = provider.toLowerCase(Locale.ROOT).trim();

        return switch (normalized) {
            case PROVIDER_AWS -> new AwsLambdaOrchestratorRenderer();
            case PROVIDER_AZURE -> new AzureOrchestratorRenderer();
            case PROVIDER_GCP -> new GcpOrchestratorRenderer();
            default -> throw new IllegalArgumentException(
                "Unsupported function provider: '" + provider + "'. " +
                "Expected one of: aws, azure, gcp");
        };
    }

    /**
     * Retrieve the explicitly configured function provider from system properties.
     *
     * Checks "pipeline.function.provider" and falls back to the legacy "tpf.function.provider".
     *
     * @return the provider name trimmed, or {@code null} if no explicit provider is configured
     */
    private static String getExplicitProvider() {
        String provider = System.getProperty("pipeline.function.provider");
        if (provider != null && !provider.isBlank()) {
            return provider.trim();
        }

        // Also check legacy property name for backwards compatibility
        provider = System.getProperty("tpf.function.provider");
        if (provider != null && !provider.isBlank()) {
            return provider.trim();
        }

        return null;
    }

    /**
     * Detects the cloud function provider by checking for known Quarkus extension classes on the classpath.
     *
     * <p>If multiple providers are present, selects one with precedence AWS &gt; Azure &gt; GCP and prints a warning
     * to System.err indicating the chosen provider and how to override it via the system property.</p>
     *
     * @return `aws`, `azure`, or `gcp` when detected; `null` if none are detected
     */
    private static String detectProviderFromClasspath() {
        boolean hasAws = isClassAvailable(EXTENSION_AWS);
        boolean hasAzure = isClassAvailable(EXTENSION_AZURE);
        boolean hasGcp = isClassAvailable(EXTENSION_GCP);

        int count = (hasAws ? 1 : 0) + (hasAzure ? 1 : 0) + (hasGcp ? 1 : 0);

        if (count == 0) {
            return null;
        }

        if (count > 1) {
            // Multiple providers detected - log warning and use precedence
            // AWS > Azure > GCP
            // Build override list dynamically based on detected providers
            List<String> availableProviders = new ArrayList<>();
            if (hasAws) availableProviders.add(PROVIDER_AWS);
            if (hasAzure) availableProviders.add(PROVIDER_AZURE);
            if (hasGcp) availableProviders.add(PROVIDER_GCP);
            String overrideList = String.join("|", availableProviders);

            if (hasAws) {
                System.err.println("[TPF] Multiple function providers detected. Using AWS Lambda (aws). " +
                    "Set -Dpipeline.function.provider=" + overrideList + " to override.");
                return PROVIDER_AWS;
            }
            if (hasAzure) {
                System.err.println("[TPF] Multiple function providers detected. Using Azure Functions (azure). " +
                    "Set -Dpipeline.function.provider=" + overrideList + " to override.");
                return PROVIDER_AZURE;
            }
        }

        if (hasAws) return PROVIDER_AWS;
        if (hasAzure) return PROVIDER_AZURE;
        if (hasGcp) return PROVIDER_GCP;

        // This line is unreachable - all cases are covered above
        throw new IllegalStateException("Provider detection failed despite positive classpath check");
    }

    /**
     * Determines whether the specified fully-qualified class name is present on the classpath.
     *
     * This check uses the factory's class loader and does not initialize the class.
     *
     * @param className the fully-qualified name of the class to look up
     * @return `true` if the class can be located by FunctionHandlerRendererFactory's class loader, `false` otherwise
     */
    private static boolean isClassAvailable(String className) {
        try {
            Class.forName(className, false, FunctionHandlerRendererFactory.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
