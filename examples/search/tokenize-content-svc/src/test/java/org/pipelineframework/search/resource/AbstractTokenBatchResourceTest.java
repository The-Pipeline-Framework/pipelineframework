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

package org.pipelineframework.search.resource;

import java.util.Collections;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.restassured.path.json.JsonPath;
import io.restassured.RestAssured;
import io.restassured.config.SSLConfig;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class AbstractTokenBatchResourceTest {
    private static final int TOKENS_PER_BATCH = 4;
    private static final int LONG_CONTENT_REPEAT_COUNT =
            (TOKENS_PER_BATCH * 3) + 1;
    private static final Pattern TOKENS_HASH_PATTERN = Pattern.compile("tokensHash");

    @BeforeAll
    static void setUp() {
        RestAssured.useRelaxedHTTPSValidation();
        RestAssured.config =
                RestAssured.config().sslConfig(SSLConfig.sslConfig().relaxedHTTPSValidation());
        RestAssured.port =
                Integer.parseInt(System.getProperty("quarkus.http.test-ssl-port", "8446"));
    }

    @AfterAll
    static void tearDown() {
        RestAssured.reset();
    }

    @Test
    void testTokenBatchWithValidData() {
        String requestBody =
                """
                {
                  "docId": "%s",
                  "content": "Search pipeline content for tokenization."
                }
                """
                        .formatted(UUID.randomUUID());

        String responseBody = given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/parsed-document/")
                .then()
                .statusCode(200)
                .extract()
                .asString();

        String firstPayload = firstSsePayload(responseBody);
        JsonPath json = JsonPath.from(firstPayload);
        assertNotNull(json.getString("docId"));
        assertNotNull(json.getString("tokens"));
        assertNotNull(json.getString("tokenizedAt"));
    }

    @Test
    void testTokenBatchWithInvalidUUID() {
        String requestBody =
                """
                {
                  "docId": "invalid-uuid",
                  "content": "Search pipeline content for tokenization."
                }
                """;

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/parsed-document/")
                .then()
                .statusCode(400);
    }

    @Test
    void testTokenBatchStreamsMultipleBatchesForLongContent() {
        // Tie payload length to the service batch size so the test always forces multiple batches.
        String longContent = String.join(" ", Collections.nCopies(
                LONG_CONTENT_REPEAT_COUNT, "tokenizable-content-for-batch-splitting"));
        String requestBody =
                """
                {
                  "docId": "%s",
                  "content": "%s"
                }
                """
                        .formatted(UUID.randomUUID(), longContent);

        String responseBody = given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/parsed-document/")
                .then()
                .statusCode(200)
                .extract()
                .asString();

        int tokenHashOccurrences = countOccurrences(responseBody, TOKENS_HASH_PATTERN);
        assertTrue(tokenHashOccurrences >= 2, "expected at least two streamed TokenBatch items");
    }

    private static int countOccurrences(String value, Pattern pattern) {
        Matcher matcher = pattern.matcher(value);
        return Math.toIntExact(matcher.results().count());
    }

    private static String firstSsePayload(String responseBody) {
        for (String line : responseBody.split("\\R")) {
            if (line.startsWith("data:")) {
                return line.substring("data:".length()).trim();
            }
        }
        throw new IllegalStateException("No SSE data payload found in response");
    }
}
