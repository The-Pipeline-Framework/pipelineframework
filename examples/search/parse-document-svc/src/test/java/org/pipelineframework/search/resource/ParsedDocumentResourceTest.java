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

package org.pipelineframework.search.resource;

import java.util.UUID;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.RestAssured;
import io.restassured.config.SSLConfig;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class ParsedDocumentResourceTest {
    private static final String PARSED_DOCUMENT_ENDPOINT = "/api/v1/parsed-document/";

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(
                isGeneratedRestResourcePresent(),
                "Generated REST resource not present in this build; skipping REST endpoint assertions.");

        // Configure RestAssured to use HTTPS and trust all certificates for testing
        RestAssured.useRelaxedHTTPSValidation();
        RestAssured.config =
                RestAssured.config().sslConfig(SSLConfig.sslConfig().relaxedHTTPSValidation());
        RestAssured.port =
                Integer.parseInt(System.getProperty("quarkus.http.test-ssl-port", "8445"));
    }

    @AfterAll
    static void tearDown() {
        // Reset RestAssured to default configuration
        RestAssured.reset();
    }

    private static boolean isGeneratedRestResourcePresent() {
        try {
            Class.forName(
                    "org.pipelineframework.search.parse_document.service.pipeline.ProcessParseDocumentResource");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Test
    void testProcessParseDocumentWithValidData() {
        String requestBody =
                """
                {
                  "docId": "%s",
                  "rawContent": "Title: Example Doc\\nBody: Search pipeline content."
                }
                """
                        .formatted(UUID.randomUUID());

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post(PARSED_DOCUMENT_ENDPOINT)
                .then()
                .statusCode(200)
                .body("docId", notNullValue())
                .body("title", notNullValue())
                .body("content", notNullValue())
                .body("extractedAt", notNullValue());
    }

    @Test
    void testProcessParseDocumentWithInvalidUUID() {
        String requestBody =
                """
                {
                  "docId": "invalid-uuid",
                  "rawContent": "Title: Example Doc\\nBody: Search pipeline content."
                }
                """;

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post(PARSED_DOCUMENT_ENDPOINT)
                .then()
                .statusCode(400);
    }

    @Test
    void testProcessParseDocumentWithMissingRequiredFields() {
        String requestBody =
                """
                {
                  "docId": "%s"
                }
                """
                        .formatted(UUID.randomUUID());

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post(PARSED_DOCUMENT_ENDPOINT)
                .then()
                .statusCode(400);
    }
}
