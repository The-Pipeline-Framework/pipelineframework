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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link NoopLambdaHandler} in the index-document-svc module.
 *
 * {@code NoopLambdaHandler} was added to this module's test scope in this PR as a
 * test-only fallback handler for Lambda profile builds. These tests verify the
 * passthrough contract and RequestHandler SPI compliance.
 */
class NoopLambdaHandlerTest {

    /**
     * The handler must echo back the exact object that was passed as input.
     */
    @Test
    void handleRequest_returnsInputUnchanged() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Object input = "index-document-lambda-event";
        Context context = mock(Context.class);

        Object result = handler.handleRequest(input, context);

        assertThat(result).isSameAs(input);
    }

    /**
     * A null input must be returned as null, without any NullPointerException.
     */
    @Test
    void handleRequest_returnsNull_whenInputIsNull() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Context context = mock(Context.class);

        Object result = handler.handleRequest(null, context);

        assertThat(result).isNull();
    }

    /**
     * Multiple invocations with the same input must return the same result each time,
     * confirming the handler is stateless.
     */
    @Test
    void handleRequest_isStateless_returnsConsistentResults() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Object input = new Object();

        Object result1 = handler.handleRequest(input, mock(Context.class));
        Object result2 = handler.handleRequest(input, mock(Context.class));

        assertThat(result1).isSameAs(result2)
            .isSameAs(input);
    }

    /**
     * Verifies that {@link NoopLambdaHandler} satisfies the Lambda SPI by implementing
     * {@link com.amazonaws.services.lambda.runtime.RequestHandler}. This is required for
     * the handler to be injectable in Lambda test profiles.
     */
    @Test
    void noopLambdaHandler_implementsRequestHandlerInterface() {
        NoopLambdaHandler handler = new NoopLambdaHandler();

        assertThat(handler)
            .isInstanceOf(com.amazonaws.services.lambda.runtime.RequestHandler.class);
    }
}