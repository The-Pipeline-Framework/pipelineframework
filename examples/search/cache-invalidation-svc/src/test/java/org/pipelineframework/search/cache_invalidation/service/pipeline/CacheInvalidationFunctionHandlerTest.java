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

package org.pipelineframework.search.cache_invalidation.service.pipeline;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.CDI;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheInvalidationFunctionHandlerTest {
    private static final String DELEGATE_HANDLER_NAME = "CacheRawDocumentSideEffectFunctionHandler";

    private MockedStatic<CDI> cdiMock;

    @AfterEach
    void closeMocks() {
        if (cdiMock != null) {
            cdiMock.close();
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void handleRequest_throwsIllegalStateException_whenDelegateNotResolvable() {
        CDI<Object> cdiInstance = mock(CDI.class);
        Instance<RequestHandler> handlerInstance = mock(Instance.class);

        when(cdiInstance.select(eq(RequestHandler.class), any(NamedLiteral.class)))
            .thenReturn(handlerInstance);
        when(handlerInstance.isResolvable()).thenReturn(false);

        cdiMock = mockStatic(CDI.class);
        cdiMock.when(CDI::current).thenReturn(cdiInstance);

        CacheInvalidationFunctionHandler handler = new CacheInvalidationFunctionHandler();

        IllegalStateException ex = assertThrows(
            IllegalStateException.class,
            () -> handler.handleRequest("test-input", mock(Context.class)));

        assertTrue(ex.getMessage().contains(DELEGATE_HANDLER_NAME));
        assertTrue(ex.getMessage().contains("CacheInvalidationFunctionHandler"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void handleRequest_delegatesToNamedBean_whenResolvable() {
        CDI<Object> cdiInstance = mock(CDI.class);
        Instance<RequestHandler> handlerInstance = mock(Instance.class);
        RequestHandler<Object, Object> delegateHandler = mock(RequestHandler.class);

        Object input = "some-event-payload";
        Object expectedOutput = "delegate-response";
        Context context = mock(Context.class);

        when(cdiInstance.select(eq(RequestHandler.class), any(NamedLiteral.class)))
            .thenReturn(handlerInstance);
        when(handlerInstance.isResolvable()).thenReturn(true);
        when(handlerInstance.get()).thenReturn(delegateHandler);
        when(delegateHandler.handleRequest(input, context)).thenReturn(expectedOutput);

        cdiMock = mockStatic(CDI.class);
        cdiMock.when(CDI::current).thenReturn(cdiInstance);

        CacheInvalidationFunctionHandler handler = new CacheInvalidationFunctionHandler();
        Object result = handler.handleRequest(input, context);

        assertSame(expectedOutput, result);
        verify(delegateHandler).handleRequest(input, context);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void handleRequest_looksUpDelegateByExpectedBeanName() {
        CDI<Object> cdiInstance = mock(CDI.class);
        Instance<RequestHandler> handlerInstance = mock(Instance.class);

        when(cdiInstance.select(eq(RequestHandler.class), any(NamedLiteral.class)))
            .thenReturn(handlerInstance);
        when(handlerInstance.isResolvable()).thenReturn(false);

        cdiMock = mockStatic(CDI.class);
        cdiMock.when(CDI::current).thenReturn(cdiInstance);

        CacheInvalidationFunctionHandler handler = new CacheInvalidationFunctionHandler();

        assertThrows(IllegalStateException.class, () -> handler.handleRequest("input", mock(Context.class)));

        verify(cdiInstance).select(eq(RequestHandler.class), eq(NamedLiteral.of(DELEGATE_HANDLER_NAME)));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void handleRequest_propagatesNullInput_toDelegate() {
        CDI<Object> cdiInstance = mock(CDI.class);
        Instance<RequestHandler> handlerInstance = mock(Instance.class);
        RequestHandler<Object, Object> delegateHandler = mock(RequestHandler.class);
        Context context = mock(Context.class);

        when(cdiInstance.select(eq(RequestHandler.class), any(NamedLiteral.class)))
            .thenReturn(handlerInstance);
        when(handlerInstance.isResolvable()).thenReturn(true);
        when(handlerInstance.get()).thenReturn(delegateHandler);
        when(delegateHandler.handleRequest(null, context)).thenReturn(null);

        cdiMock = mockStatic(CDI.class);
        cdiMock.when(CDI::current).thenReturn(cdiInstance);

        CacheInvalidationFunctionHandler handler = new CacheInvalidationFunctionHandler();
        Object result = handler.handleRequest(null, context);

        assertNull(result);
        verify(delegateHandler).handleRequest(null, context);
    }
}
