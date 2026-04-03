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

package org.pipelineframework.search.orchestrator.service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for Search Pipeline deployed to Azure Functions.
 *
 * This test validates:
 * - Azure Functions HTTP trigger invocation
 * - Pipeline execution through Function boundaries
 * - Cardinality handling (ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE)
 * - Response serialization/deserialization across Function transport
 *
 * Prerequisites:
 * - Azure Functions deployment must be running
 * - AZURE_FUNCTION_APP_URL environment variable must be set
 * - Tests run against real Azure infrastructure (not mocked)
 */
@QuarkusIntegrationTest
@EnabledIfEnvironmentVariable(named = "AZURE_FUNCTION_APP_URL", matches = ".+")
class AzureFunctionsEndToEndIT {

    private static final Logger LOG = Logger.getLogger(AzureFunctionsEndToEndIT.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static String functionAppUrl;
    private static HttpClient httpClient;

    /**
     * Initializes shared test configuration for the Azure Functions end-to-end integration tests.
     *
     * Reads the `AZURE_FUNCTION_APP_URL` environment variable into `functionAppUrl`, removes a trailing
     * slash if present, logs the resolved base URL, and builds a reusable `HttpClient` configured to
     * use HTTP/2 with a 30-second connection timeout.
     */
    @BeforeAll
    static void setup() {
        functionAppUrl = System.getenv("AZURE_FUNCTION_APP_URL");

        // Remove trailing slash if present
        if (functionAppUrl.endsWith("/")) {
            functionAppUrl = functionAppUrl.substring(0, functionAppUrl.length() - 1);
        }

        LOG.infof("Testing Azure Functions deployment at: %s", functionAppUrl);

        httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(30))
            .build();

        // Verify connectivity before running any tests
        verifyConnectivity();
    }

    private static void verifyConnectivity() {
        String healthUrl = functionAppUrl + "/q/health/live";
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(healthUrl))
            .timeout(Duration.ofSeconds(10))
            .GET()
            .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            boolean isHealthy = response.statusCode() >= 200 && response.statusCode() < 600;
            org.junit.jupiter.api.Assumptions.assumeTrue(isHealthy,
                "Skipping suite due to unavailable health endpoint: HTTP " + response.statusCode());
        } catch (Exception e) {
            org.junit.jupiter.api.Assumptions.assumeTrue(false,
                "Skipping suite due to unavailable health endpoint: " + e.getMessage());
        }
    }

    /**
     * Executes a single (one-to-one) pipeline run against the Azure Functions /api/pipeline/run endpoint and validates the response.
     *
     * Verifies the HTTP response status is 200 and that the returned JSON contains a `docId` equal to the UUID sent in the request.
     */
    @Test
    void testPipelineRunUnaryInvocation() throws Exception {
        // Test basic ONE_TO_ONE pipeline invocation through Azure Functions
        String pipelineRunUrl = functionAppUrl + "/api/pipeline/run";

        // Create a simple crawl request for unary processing
        UUID testDocId = UUID.randomUUID();
        String testUrl = "https://example.com/test-" + testDocId;

        String requestBody = OBJECT_MAPPER.writeValueAsString(
            createCrawlRequest(testDocId, testUrl));

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(pipelineRunUrl))
            .timeout(Duration.ofSeconds(120))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.infof("Pipeline run response status: %d", response.statusCode());
        // Log truncated response body to avoid leaking sensitive data
        String bodyPreview = response.body().length() > 200 
            ? response.body().substring(0, 200) + "..." 
            : response.body();
        LOG.infof("Pipeline run response body (truncated): %s", bodyPreview);

        // Verify response
        assertEquals(200, response.statusCode(),
            "Pipeline execution should succeed");

        JsonNode responseJson = OBJECT_MAPPER.readTree(response.body());
        assertTrue(responseJson.has("docId"), "Response should contain docId");
        assertEquals(testDocId.toString(), responseJson.get("docId").asText(),
            "Response docId should match request");
    }

    /**
     * Verifies fan-out (tokenization) and fan-in (indexing) behavior of the pipeline using a long content payload.
     *
     * Sends a POST to the pipeline run endpoint with a generated document and extended content, asserts HTTP 200,
     * validates the presence of `docId`, `indexVersion`, `tokenBatchCount`, and `success` in the response, and
     * ensures `tokenBatchCount` is greater than zero.
     *
     * @throws Exception if the HTTP request, serialization, or response processing fails
     */
    @Test
    void testFanOutFanInCardinality() throws Exception {
        // Test ONE_TO_MANY -> MANY_TO_ONE pipeline path
        // This exercises Tokenize Content (fan-out) and Index Document (fan-in)
        String pipelineRunUrl = functionAppUrl + "/api/pipeline/run";

        UUID testDocId = UUID.randomUUID();
        String testUrl = "https://example.com/article-" + testDocId;

        // Create crawl request with longer content to potentially trigger multi-batch tokenization
        JsonNode requestBody = OBJECT_MAPPER.createObjectNode()
            .put("docId", testDocId.toString())
            .put("sourceUrl", testUrl)
            .put("fetchMethod", "GET")
            .put("accept", "text/html")
            .put("acceptLanguage", "en-US")
            .put("content", generateLongTestContent());

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(pipelineRunUrl))
            .timeout(Duration.ofSeconds(180)) // Longer timeout for fan-out/fan-in
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(requestBody)))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.infof("Fan-out/fan-in response status: %d", response.statusCode());
        // Log truncated response body to avoid leaking sensitive data
        String bodyPreview = response.body().length() > 200 
            ? response.body().substring(0, 200) + "..." 
            : response.body();
        LOG.infof("Fan-out/fan-in response body (truncated): %s", bodyPreview);

        assertEquals(200, response.statusCode(),
            "Fan-out/fan-in pipeline execution should succeed");

        JsonNode responseJson = OBJECT_MAPPER.readTree(response.body());
        assertTrue(responseJson.has("docId"), "Response should contain docId");
        assertTrue(responseJson.has("indexVersion"), "Response should contain indexVersion");
        assertTrue(responseJson.has("tokenBatchCount"), "Response should contain tokenBatchCount");
        assertTrue(responseJson.has("success"), "Response should contain success flag");

        // Verify fan-in aggregated the batches correctly
        int tokenBatchCount = responseJson.get("tokenBatchCount").asInt();
        assertTrue(tokenBatchCount > 0, "Should have processed at least one token batch");

        LOG.infof("Processed %d token batches for document %s", tokenBatchCount, testDocId);
    }

    /**
     * Executes three sequential pipeline run requests against the function app to verify each invocation completes successfully and operates independently.
     *
     * For each invocation this test:
     * - Sends a JSON crawl request to the pipeline run endpoint.
     * - Asserts the HTTP response status is 200.
     * - Asserts the response JSON contains a `success` flag set to `true`.
     */
    @Test
    void testMultipleSequentialInvocations() throws Exception {
        // Test multiple sequential invocations to verify statelessness
        String pipelineRunUrl = functionAppUrl + "/api/pipeline/run";

        for (int i = 0; i < 3; i++) {
            UUID testDocId = UUID.randomUUID();
            String testUrl = "https://example.com/seq-" + testDocId;

            String requestBody = OBJECT_MAPPER.writeValueAsString(
                createCrawlRequest(testDocId, testUrl));

            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(pipelineRunUrl))
                .timeout(Duration.ofSeconds(120))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            LOG.infof("Sequential invocation %d response status: %d", i + 1, response.statusCode());

            assertEquals(200, response.statusCode(),
                String.format("Sequential invocation %d should succeed", i + 1));

            JsonNode responseJson = OBJECT_MAPPER.readTree(response.body());
            assertTrue(responseJson.has("success"), "Response should contain success flag");
            assertTrue(responseJson.get("success").asBoolean(), "Success flag should be true");
        }
    }

    /**
     * Generates a long test content string to potentially trigger multi-batch tokenization.
     */
    private String generateLongTestContent() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            sb.append("This is a test paragraph number ").append(i)
              .append(" with some sample text for tokenization purposes. ")
              .append("The quick brown fox jumps over the lazy dog. ")
              .append("Lorem ipsum dolor sit amet, consectetur adipiscing elit. ");
        }
        return sb.toString();
    }

    /**
     * Builds a JSON crawl request payload for the pipeline.
     *
     * The returned object contains the fields: `docId`, `sourceUrl`, `fetchMethod`, `accept`, and `acceptLanguage`.
     *
     * @param docId     the UUID to use as the document identifier (as a string)
     * @param sourceUrl the source URL to fetch for the crawl request
     * @return          a JsonNode representing the crawl request payload
     */
    private JsonNode createCrawlRequest(UUID docId, String sourceUrl) {
        return OBJECT_MAPPER.createObjectNode()
            .put("docId", docId.toString())
            .put("sourceUrl", sourceUrl)
            .put("fetchMethod", "GET")
            .put("accept", "text/html")
            .put("acceptLanguage", "en-US");
    }
}
