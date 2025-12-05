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

package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.*;
import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.pipelineframework.annotation.PipelineStep;

/** Unit tests for PipelineStepProcessor */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PipelineStepProcessorTest {

    @Mock
    private ProcessingEnvironment processingEnv;

    @Mock
    private RoundEnvironment roundEnv;

    @Mock
    private Messager messager;

    private PipelineStepProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new PipelineStepProcessor();
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(processingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(processingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);
    }

    @Test
    void testSupportedAnnotationTypes() {
        // We need to initialize processor for this test
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        localProcessor.init(processingEnv);

        Set<String> supportedAnnotations = localProcessor.getSupportedAnnotationTypes();
        assertTrue(supportedAnnotations.contains("org.pipelineframework.annotation.PipelineStep"));
    }

    @Test
    void testSupportedSourceVersion() {
        // We need to initialize processor for this test
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        localProcessor.init(processingEnv);

        assertEquals(SourceVersion.RELEASE_21, localProcessor.getSupportedSourceVersion());
    }

    @Test
    void testInit() {
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        assertDoesNotThrow(() -> localProcessor.init(processingEnv));
    }

    @Test
    void testProcessWithNoAnnotations() {
        // We need to initialize processor for this test
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        localProcessor.init(processingEnv);

        // Simulate the case where no @PipelineStep annotations are present in this round
        // This should cause the processor to return false immediately
        boolean result = localProcessor.process(Collections.emptySet(), roundEnv);

        assertFalse(result);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testProcessWithPipelineStepAnnotationPresent() {
        // Mock the @PipelineStep TypeElement
        TypeElement pipelineStepAnnotation = mock(TypeElement.class);
        Set<TypeElement> annotations = Collections.singleton(pipelineStepAnnotation);

        // Mock a valid class element annotated with @PipelineStep
        TypeElement mockServiceClass = mock(TypeElement.class);
        when(mockServiceClass.getKind()).thenReturn(ElementKind.CLASS);
        PipelineStep mockAnnotation = mock(PipelineStep.class);
        when(mockServiceClass.getAnnotation(PipelineStep.class)).thenReturn(mockAnnotation);

        // Use raw types to avoid generic issues
        when(roundEnv.getElementsAnnotatedWith(PipelineStep.class))
                .thenReturn((Set) Collections.singleton(mockServiceClass));

        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        ProcessingEnvironment localProcessingEnv = mock(ProcessingEnvironment.class);
        when(localProcessingEnv.getMessager()).thenReturn(mock(Messager.class));
        when(localProcessingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(localProcessingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(localProcessingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);

        localProcessor.init(localProcessingEnv);

        // Use spy to verify that generation methods are called
        PipelineStepProcessor spyProcessor = spy(localProcessor);
        try {
            doNothing()
                    .when(spyProcessor)
                    .generateGrpcServiceAdapter(any(TypeElement.class), any(PipelineStep.class));
            doNothing()
                    .when(spyProcessor)
                    .generateClientStep(any(TypeElement.class), any(PipelineStep.class));
        } catch (IOException e) {
            // This catch block is just to satisfy the compiler - IOException won't actually be
            // thrown by doNothing
            fail("Unexpected IOException during mocking: " + e.getMessage());
        }

        // When annotations are present (i.e., @PipelineStep), processing should occur and return
        // true
        boolean result = spyProcessor.process(annotations, roundEnv);

        assertTrue(result);
        try {
            verify(spyProcessor)
                    .generateGrpcServiceAdapter(eq(mockServiceClass), eq(mockAnnotation));
            verify(spyProcessor).generateClientStep(eq(mockServiceClass), eq(mockAnnotation));
        } catch (IOException e) {
            // This catch is just to satisfy the compiler - verify() won't actually throw
            // IOException
            fail("Unexpected IOException during verification: " + e.getMessage());
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testProcessWithNonClassAnnotatedElement() {
        // Mock the @PipelineStep TypeElement
        TypeElement pipelineStepAnnotation = mock(TypeElement.class);
        Set<TypeElement> annotations = Collections.singleton(pipelineStepAnnotation);

        // Mock an element that is not a class
        Element mockElement = mock(Element.class);
        when(mockElement.getKind()).thenReturn(ElementKind.METHOD);
        when(mockElement.getAnnotation(PipelineStep.class)).thenReturn(mock(PipelineStep.class));

        // Use raw types to avoid generic issues
        when(roundEnv.getElementsAnnotatedWith(PipelineStep.class))
                .thenReturn((Set) Collections.singleton(mockElement));

        // Create a fresh processor for this test that uses our shared mocks
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        ProcessingEnvironment localProcessingEnv = mock(ProcessingEnvironment.class);
        Messager localMessager = mock(Messager.class);
        when(localProcessingEnv.getMessager()).thenReturn(localMessager);
        when(localProcessingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(localProcessingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(localProcessingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);

        localProcessor.init(localProcessingEnv);

        localProcessor.process(annotations, roundEnv);

        // Verify that an error message was printed
        verify(localMessager)
                .printMessage(
                        eq(Diagnostic.Kind.ERROR),
                        eq("@PipelineStep can only be applied to classes"),
                        eq(mockElement));
    }

    @ParameterizedTest
    @CsvSource({
            "ProcessCustomerService, /api/v1/process-customer",
            "ValidateOrderService, /api/v1/validate-order",
            "CustomerService, /api/v1/customer",
            "ProcessPaymentStatus, /api/v1/process-payment-status",
            "SimpleClass, /api/v1/simple-class",
            "UPPERCASE, /api/v1/uppercase",
            "camelCase, /api/v1/camel-case",
            "XMLHttpRequest, /api/v1/xml-http-request"
    })
    void testDeriveResourcePath(String className, String expectedPath) {
        assertEquals(expectedPath, processor.deriveResourcePath(className));
    }

    @Test
    void testDeriveResourcePathWithException() {
        // Test exception handling in deriveResourcePath
        try {
            // Pass null to trigger exception for better diagnostic coverage
            processor.deriveResourcePath(null);
            fail("Expected exception was not thrown");
        } catch (Exception e) {
            // Verify that the exception is handled appropriately
            // In this case, it would be a NullPointerException from calling substring on null
            assertTrue(e instanceof NullPointerException);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "com.example.domain.CustomerInput, com.example.dto.CustomerInputDto",
            "com.example.common.domain.CustomerOutput, com.example.common.dto.CustomerOutputDto",
            "com.example.domain.payment.PaymentRecord, com.example.dto.payment.PaymentRecordDto",
            "com.example.domain.CustomerInputDto, com.example.domain.CustomerInputDto", // Already ends
            // with Dto
            "com.example.service.Customer, com.example.dto.CustomerDto", // Service package
            "com.example.model.Customer, com.example.model.CustomerDto", // Not in domain package
            "com.example.foo.domain.bar.Type, com.example.foo.dto.bar.TypeDto", // Nested domain
            // packages
            "CustomerInput, CustomerInputDto", // No package
            "com.example.domain, com.example.dto.domainDto", // Edge case with class named domain
            "com.example.common.domain, com.example.common.dto.domainDto" // Edge case with class named
    // domain
    })
    void testGetDtoType(String domainType, String expectedDtoType) {
        // Create a proper TypeMirror mock that returns the domainType when toString is called
        javax.lang.model.type.TypeMirror mockTypeMirror = mock(javax.lang.model.type.TypeMirror.class);
        when(mockTypeMirror.toString()).thenReturn(domainType);

        String actualDtoType = processor.getDtoType(mockTypeMirror);
        assertEquals(expectedDtoType, actualDtoType);
    }

    @Test
    void testGetDtoTypeWithNullInput() {
        // Create a TypeMirror mock that returns null for toString
        javax.lang.model.type.TypeMirror mockTypeMirror = mock(javax.lang.model.type.TypeMirror.class);
        when(mockTypeMirror.toString()).thenReturn(null);

        // When toString returns null, the method should return null
        String result = processor.getDtoType(mockTypeMirror);
        assertNull(result);
    }

    @Test
    void testImplementsInterface() {
        // The implementsInterface method requires processingEnv to be properly initialized
        // For this test, we'll test with a processor that has been initialized
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();

        // Set up the processingEnv mock
        ProcessingEnvironment localProcessingEnv = mock(ProcessingEnvironment.class);
        when(localProcessingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(localProcessingEnv.getTypeUtils()).thenReturn(mock(javax.lang.model.util.Types.class));

        localProcessor.init(localProcessingEnv);

        // Create a mock service class and test interface checking
        TypeElement mockServiceClass = mock(TypeElement.class);
        when(mockServiceClass.getInterfaces()).thenReturn(Collections.emptyList());

        // Test with a class that doesn't implement the interface
        boolean result = localProcessor.implementsInterface(
                mockServiceClass, "org.pipelineframework.service.ReactiveService");
        // This should return false since we mocked empty interfaces
        assertFalse(result);
    }

    @Test
    void testImplementsReactiveService() {
        // The implementsReactiveService method requires processingEnv to be properly initialized
        PipelineStepProcessor localProcessor = new PipelineStepProcessor();

        // Set up the processingEnv mock
        ProcessingEnvironment localProcessingEnv = mock(ProcessingEnvironment.class);
        when(localProcessingEnv.getElementUtils())
                .thenReturn(mock(javax.lang.model.util.Elements.class));
        when(localProcessingEnv.getTypeUtils()).thenReturn(mock(javax.lang.model.util.Types.class));

        localProcessor.init(localProcessingEnv);

        // Create a mock service class and test reactive service interface checking
        TypeElement mockServiceClass = mock(TypeElement.class);
        when(mockServiceClass.getInterfaces()).thenReturn(Collections.emptyList());

        boolean result = localProcessor.implementsReactiveService(mockServiceClass);
        // This should return false since the service doesn't implement ReactiveService
        assertFalse(result);
    }
}
