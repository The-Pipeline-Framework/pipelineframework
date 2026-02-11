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

import java.util.UUID;

import io.restassured.RestAssured;
import io.restassured.config.SSLConfig;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.notNullValue;

abstract class AbstractTokenBatchResourceTest {

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

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/token-batch/")
                .then()
                .statusCode(200)
                .body("docId", notNullValue())
                .body("tokens", notNullValue())
                .body("tokenizedAt", notNullValue());
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
                .post("/api/v1/token-batch/")
                .then()
                .statusCode(400);
    }

    @Test
    void testTokenBatchWithMissingRequiredFields() {
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
                .post("/api/v1/token-batch/")
                .then()
                .statusCode(400);
    }
}
