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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class NoopLambdaHandlerTest {

    @Test
    void handleRequest_returnsInputUnchanged() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Object input = "some-lambda-event-payload";
        Context context = mock(Context.class);

        Object result = handler.handleRequest(input, context);

        assertEquals(input, result);
    }

    @Test
    void handleRequest_returnsNull_whenInputIsNull() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Context context = mock(Context.class);

        Object result = handler.handleRequest(null, context);

        assertNull(result);
    }

    @Test
    void handleRequest_ignoresContext() {
        NoopLambdaHandler handler = new NoopLambdaHandler();
        Object input = new Object();

        Object result1 = handler.handleRequest(input, mock(Context.class));
        Object result2 = handler.handleRequest(input, mock(Context.class));

        assertEquals(input, result1);
        assertEquals(result1, result2);
    }

    @Test
    void noopLambdaHandler_implementsRequestHandlerInterface() {
        NoopLambdaHandler handler = new NoopLambdaHandler();

        assertTrue(handler instanceof com.amazonaws.services.lambda.runtime.RequestHandler);
    }
}
