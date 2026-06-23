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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

@QuarkusTest
class IndexAckResourceTest {

    @BeforeAll
    static void setUp() {
        Assumptions.assumeTrue(
                isGeneratedRestResourcePresent(),
                "Generated REST resource not present in this build; skipping REST endpoint assertions.");

        int httpsPort = Integer.parseInt(System.getProperty("quarkus.http.test-ssl-port", "8447"));
        Assumptions.assumeTrue(
                isRestServerReachable(httpsPort),
                "REST test server is not active for this build; skipping REST endpoint assertions.");

        // Configure RestAssured to use HTTPS and trust all certificates for testing
        RestAssured.useRelaxedHTTPSValidation();
        RestAssured.config =
                RestAssured.config().sslConfig(SSLConfig.sslConfig().relaxedHTTPSValidation());
        RestAssured.port = httpsPort;
    }

    @AfterAll
    static void tearDown() {
        // Reset RestAssured to default configuration
        RestAssured.reset();
    }

    private static boolean isGeneratedRestResourcePresent() {
        try {
            Class.forName(
                    "org.pipelineframework.search.index_document.service.pipeline.ProcessSummarizeIndexWritesResource",
                    false,
                    Thread.currentThread().getContextClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    private static boolean isRestServerReachable(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 250);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    void testIndexAckWithValidData() {
        String requestBody =
                """
                [
                  {
                    "docId": "%s",
                    "externalId": "external-0",
                    "commandId": "cmd-0",
                    "indexName": "search-index",
                    "resultStatus": "UPSERTED",
                    "createdOrUpdated": true,
                    "recordedDuplicate": false,
                    "batchIndex": 0,
                    "tokenCount": 4,
                    "tokens": "search pipeline tokens",
                    "tokensHash": "seed-hash",
                    "vectorHash": "vector-hash-0",
                    "vectorVersion": "vec-v1"
                  }
                ]
                """
                        .formatted(UUID.randomUUID());

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/index-ack/")
                .then()
                .statusCode(200)
                .body("docId", notNullValue())
                .body("indexVersion", notNullValue())
                .body("indexedAt", notNullValue())
                .body("success", notNullValue());
    }

    @Test
    void testIndexAckWithInvalidUUID() {
        String requestBody =
                """
                [
                  {
                    "docId": "invalid-uuid",
                    "externalId": "external-0",
                    "commandId": "cmd-0",
                    "tokens": "search pipeline tokens",
                    "tokensHash": "seed-hash",
                    "vectorHash": "vector-hash-0",
                    "vectorVersion": "vec-v1"
                  }
                ]
                """;

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/index-ack/")
                .then()
                .statusCode(400);
    }

    @Test
    void testIndexAckWithMissingRequiredFields() {
        String requestBody =
                """
                [
                  {
                    "docId": "%s",
                    "externalId": "external-0"
                  }
                ]
                """
                        .formatted(UUID.randomUUID());

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/index-ack/")
                .then()
                .statusCode(400);
    }

    @Test
    void testIndexAckRejectsMixedDocIdsInSingleBatch() {
        String requestBody =
                """
                [
                  {
                    "docId": "%s",
                    "externalId": "external-0",
                    "commandId": "cmd-0",
                    "indexName": "search-index",
                    "resultStatus": "UPSERTED",
                    "createdOrUpdated": true,
                    "recordedDuplicate": false,
                    "batchIndex": 0,
                    "tokenCount": 5,
                    "tokens": "search pipeline tokens one",
                    "tokensHash": "seed-hash-1",
                    "vectorHash": "vector-hash-0",
                    "vectorVersion": "vec-v1"
                  },
                  {
                    "docId": "%s",
                    "externalId": "external-1",
                    "commandId": "cmd-1",
                    "indexName": "search-index",
                    "resultStatus": "UPSERTED",
                    "createdOrUpdated": true,
                    "recordedDuplicate": false,
                    "batchIndex": 1,
                    "tokenCount": 5,
                    "tokens": "search pipeline tokens two",
                    "tokensHash": "seed-hash-2",
                    "vectorHash": "vector-hash-1",
                    "vectorVersion": "vec-v1"
                  }
                ]
                """
                        .formatted(UUID.randomUUID(), UUID.randomUUID());

        given().contentType(ContentType.JSON)
                .body(requestBody)
                .when()
                .post("/api/v1/index-ack/")
                .then()
                .statusCode(400)
                .body(containsString("Invalid request"));
    }
}
