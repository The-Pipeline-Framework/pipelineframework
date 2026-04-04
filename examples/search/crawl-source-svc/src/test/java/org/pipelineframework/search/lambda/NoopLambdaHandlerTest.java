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

package org.pipelineframework.search.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link NoopLambdaHandler}.
 *
 * {@code NoopLambdaHandler} was relocated from {@code common/src/main} to
 * {@code crawl-source-svc/src/test} as part of this PR, scoping it to test-only use
 * in the crawl-source module. These tests verify its passthrough contract.
 */
class NoopLambdaHandlerTest {

    /**
     * The handler must return the exact same object reference that was passed as input.
     */
    @Test
    void handleRequest_returnsInputUnchanged() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Object input = "some-lambda-event-payload";
        Context context = mock(Context.class);

        Object result = handler.handleRequest(input, context);

        assertEquals(input, result, "NoopLambdaHandler must return the input unchanged");
    }

    /**
     * The handler must return null when given a null input, without throwing.
     * Confirms null-safety for Lambda events that may arrive as null.
     */
    @Test
    void handleRequest_returnsNull_whenInputIsNull() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Context context = mock(Context.class);

        Object result = handler.handleRequest(null, context);

        assertNull(result, "NoopLambdaHandler must propagate null input as null output");
    }

    /**
     * The context parameter is not consulted; different context instances must not
     * affect what the handler returns.
     */
    @Test
    void handleRequest_ignoresContext() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Object input = new Object();

        Object result1 = handler.handleRequest(input, mock(Context.class));
        Object result2 = handler.handleRequest(input, mock(Context.class));

        assertEquals(input, result1);
        assertEquals(result1, result2,
            "Handler should return the same result regardless of which context is supplied");
    }

    /**
     * Verifies that the handler implements {@link com.amazonaws.services.lambda.runtime.RequestHandler},
     * satisfying the Lambda SPI contract required for test-scope Lambda wiring.
     */
    @Test
    void noopLambdaHandler_implementsRequestHandlerInterface() {
        NoopLambdaHandler handler = new NoopLambdaHandler();

        // Runtime check: must satisfy the Lambda handler SPI
        assertTrue(handler instanceof com.amazonaws.services.lambda.runtime.RequestHandler,
            "NoopLambdaHandler must implement RequestHandler to satisfy Lambda test wiring");
    }

    private static void assertTrue(boolean condition, String message) {
        org.junit.jupiter.api.Assertions.assertTrue(condition, message);
    }
}