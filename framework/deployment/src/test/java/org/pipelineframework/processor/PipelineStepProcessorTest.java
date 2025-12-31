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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.tools.Diagnostic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.pipelineframework.annotation.PipelineStep;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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

    @BeforeEach
    void setUp() {
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
    void testProcessWithPipelineStepAnnotationPresentGrpcEnabled() {
        // Mock the @PipelineStep TypeElement
        TypeElement pipelineStepAnnotation = mock(TypeElement.class);
        Set<TypeElement> annotations = Collections.singleton(pipelineStepAnnotation);

        // Mock a valid class element annotated with @PipelineStep
        TypeElement mockServiceClass = mock(TypeElement.class);
        when(mockServiceClass.getKind()).thenReturn(ElementKind.CLASS);
        when(mockServiceClass.getNestingKind()).thenReturn(NestingKind.TOP_LEVEL);

        // Set up simple name
        javax.lang.model.element.Name simpleNameMock = mock(javax.lang.model.element.Name.class);
        when(simpleNameMock.toString()).thenReturn("TestService");
        when(mockServiceClass.getSimpleName()).thenReturn(simpleNameMock);

        // Set up qualified name
        javax.lang.model.element.Name qualifiedNameMock = mock(javax.lang.model.element.Name.class);
        when(qualifiedNameMock.toString()).thenReturn("test.TestService");
        when(mockServiceClass.getQualifiedName()).thenReturn(qualifiedNameMock);

        // Mock the enclosing element (package)
        PackageElement packageElement = mock(PackageElement.class);
        when(packageElement.getKind()).thenReturn(ElementKind.PACKAGE);
        when(mockServiceClass.getEnclosingElement()).thenReturn(packageElement);

        // Mock the package element name
        javax.lang.model.element.Name packageNameMock = mock(javax.lang.model.element.Name.class);
        when(packageNameMock.toString()).thenReturn("test");
        when(packageElement.getQualifiedName()).thenReturn(packageNameMock);
        PipelineStep mockAnnotation = mock(PipelineStep.class);
        when(mockServiceClass.getAnnotation(PipelineStep.class)).thenReturn(mockAnnotation);
        // Mock the grpcEnabled property to return true (default value)
        when(mockAnnotation.grpcEnabled()).thenReturn(true);

        // Use raw types to avoid generic issues
        when(roundEnv.getElementsAnnotatedWith(PipelineStep.class))
                .thenReturn((Set) Collections.singleton(mockServiceClass));

        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        ProcessingEnvironment localProcessingEnv = mock(ProcessingEnvironment.class);
        when(localProcessingEnv.getMessager()).thenReturn(mock(Messager.class));
        javax.lang.model.util.Elements elementsUtils = mock(javax.lang.model.util.Elements.class);
        when(localProcessingEnv.getElementUtils()).thenReturn(elementsUtils);
        when(localProcessingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(localProcessingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);

        // Mock the package for the service class
        PackageElement packageElementUtils = mock(PackageElement.class);
        javax.lang.model.element.Name packageName = mock(javax.lang.model.element.Name.class);
        when(packageName.toString()).thenReturn("test");
        when(packageElementUtils.getQualifiedName()).thenReturn(packageName);
        when(elementsUtils.getPackageOf(mockServiceClass)).thenReturn(packageElementUtils);
        when(elementsUtils.getTypeElement(mockServiceClass.getQualifiedName().toString())).thenReturn(mockServiceClass);

        // Mock the annotation mirror for PipelineStep
        AnnotationMirror mockAnnotationMirror = mock(AnnotationMirror.class);
        when(mockServiceClass.getAnnotationMirrors())
                .thenReturn((List)Collections.singletonList(mockAnnotationMirror));
        javax.lang.model.type.DeclaredType declaredType = mock(javax.lang.model.type.DeclaredType.class);
        when(mockAnnotationMirror.getAnnotationType()).thenReturn(declaredType);
        when(declaredType.toString()).thenReturn("org.pipelineframework.annotation.PipelineStep");

        // Mock annotation values to ensure the extractor can access them
        // Removed duplicate: when(elementsUtils.getPackageOf(mockServiceClass)).thenReturn(packageElementUtils);

        // Mock the element values map for the annotation with wildcard types
        java.util.Map elementValuesMap = new java.util.HashMap();

        // Create mock executable elements for the annotation attributes
        ExecutableElement inputTypeElement = createMockExecutableElement("inputType");
        ExecutableElement inputGrpcTypeElement = createMockExecutableElement("inputGrpcType");
        ExecutableElement inboundMapperElement = createMockExecutableElement("inboundMapper");
        ExecutableElement outputTypeElement = createMockExecutableElement("outputType");
        ExecutableElement outputGrpcTypeElement = createMockExecutableElement("outputGrpcType");
        ExecutableElement outboundMapperElement = createMockExecutableElement("outboundMapper");
        ExecutableElement stepTypeElement = createMockExecutableElement("stepType");
        ExecutableElement grpcImplElement = createMockExecutableElement("grpcImpl");
        ExecutableElement grpcStubElement = createMockExecutableElement("grpcStub");
        ExecutableElement grpcClientElement = createMockExecutableElement("grpcClient");
        ExecutableElement sideEffectElement = createMockExecutableElement("sideEffect");
        ExecutableElement pathElement = createMockExecutableElement("path");

        // Create mock TypeElement objects for the types using the helper
        javax.lang.model.type.DeclaredType stringTypeMirror = createMockTypeMirror("java.lang.String");
        javax.lang.model.type.DeclaredType integerTypeMirror = createMockTypeMirror("java.lang.Integer");
        javax.lang.model.type.DeclaredType voidTypeMirror = createMockTypeMirror("java.lang.Void");
        javax.lang.model.type.DeclaredType grpcStringTypeMirror = createMockTypeMirror("test.TestServiceGrpcMessage");
        javax.lang.model.type.DeclaredType grpcIntegerTypeMirror = createMockTypeMirror("test.TestServiceGrpcMessageResponse");

        // Create mock annotation values with proper TypeMirror objects
        AnnotationValue inputTypeValue = mock(AnnotationValue.class);
        when(inputTypeValue.getValue()).thenReturn(stringTypeMirror);
        elementValuesMap.put(inputTypeElement, inputTypeValue);

        AnnotationValue inputGrpcTypeValue = mock(AnnotationValue.class);
        when(inputGrpcTypeValue.getValue()).thenReturn(grpcStringTypeMirror);
        elementValuesMap.put(inputGrpcTypeElement, inputGrpcTypeValue);

        AnnotationValue inboundMapperValue = mock(AnnotationValue.class);
        when(inboundMapperValue.getValue()).thenReturn(voidTypeMirror);
        elementValuesMap.put(inboundMapperElement, inboundMapperValue);

        AnnotationValue outputTypeValue = mock(AnnotationValue.class);
        when(outputTypeValue.getValue()).thenReturn(integerTypeMirror);
        elementValuesMap.put(outputTypeElement, outputTypeValue);

        AnnotationValue outputGrpcTypeValue = mock(AnnotationValue.class);
        when(outputGrpcTypeValue.getValue()).thenReturn(grpcIntegerTypeMirror);
        elementValuesMap.put(outputGrpcTypeElement, outputGrpcTypeValue);

        AnnotationValue outboundMapperValue = mock(AnnotationValue.class);
        when(outboundMapperValue.getValue()).thenReturn(voidTypeMirror);
        elementValuesMap.put(outboundMapperElement, outboundMapperValue);

        AnnotationValue stepTypeValue = mock(AnnotationValue.class);
        when(stepTypeValue.getValue()).thenReturn("org.pipelineframework.step.StepOneToOne");
        elementValuesMap.put(stepTypeElement, stepTypeValue);

        AnnotationValue grpcImplValue = mock(AnnotationValue.class);
        when(grpcImplValue.getValue()).thenReturn(grpcStringTypeMirror);
        elementValuesMap.put(grpcImplElement, grpcImplValue);

        AnnotationValue grpcStubValue = mock(AnnotationValue.class);
        when(grpcStubValue.getValue()).thenReturn(grpcStringTypeMirror);
        elementValuesMap.put(grpcStubElement, grpcStubValue);

        AnnotationValue grpcClientValue = mock(AnnotationValue.class);
        when(grpcClientValue.getValue()).thenReturn("testClient");
        elementValuesMap.put(grpcClientElement, grpcClientValue);

        AnnotationValue sideEffectValue = mock(AnnotationValue.class);
        when(sideEffectValue.getValue()).thenReturn(voidTypeMirror);
        elementValuesMap.put(sideEffectElement, sideEffectValue);

        AnnotationValue pathValue = mock(AnnotationValue.class);
        when(pathValue.getValue()).thenReturn("/test");
        elementValuesMap.put(pathElement, pathValue);

        when(mockAnnotationMirror.getElementValues()).thenReturn(elementValuesMap);

        localProcessor.init(localProcessingEnv);

        // When annotations are present (i.e., @PipelineStep), processing should occur and return
        // true
        boolean result = localProcessor.process(annotations, roundEnv);

        assertTrue(result);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void testProcessWithPipelineStepAnnotationPresentGrpcDisabled() {
        // Mock the @PipelineStep TypeElement
        TypeElement pipelineStepAnnotation = mock(TypeElement.class);
        Set<TypeElement> annotations = Collections.singleton(pipelineStepAnnotation);

        // Mock a valid class element annotated with @PipelineStep
        TypeElement mockServiceClass = mock(TypeElement.class);
        when(mockServiceClass.getKind()).thenReturn(ElementKind.CLASS);
        when(mockServiceClass.getNestingKind()).thenReturn(NestingKind.TOP_LEVEL);

        // Set up simple name
        javax.lang.model.element.Name simpleNameMock = mock(javax.lang.model.element.Name.class);
        when(simpleNameMock.toString()).thenReturn("TestService");
        when(mockServiceClass.getSimpleName()).thenReturn(simpleNameMock);

        // Set up qualified name
        javax.lang.model.element.Name qualifiedNameMock = mock(javax.lang.model.element.Name.class);
        when(qualifiedNameMock.toString()).thenReturn("test.TestService");
        when(mockServiceClass.getQualifiedName()).thenReturn(qualifiedNameMock);

        // Mock the enclosing element (package)
        PackageElement packageElement = mock(PackageElement.class);
        when(packageElement.getKind()).thenReturn(ElementKind.PACKAGE);
        when(mockServiceClass.getEnclosingElement()).thenReturn(packageElement);

        // Mock the package element name
        javax.lang.model.element.Name packageNameMock = mock(javax.lang.model.element.Name.class);
        when(packageNameMock.toString()).thenReturn("test");
        when(packageElement.getQualifiedName()).thenReturn(packageNameMock);
        PipelineStep mockAnnotation = mock(PipelineStep.class);
        when(mockServiceClass.getAnnotation(PipelineStep.class)).thenReturn(mockAnnotation);
        // Mock the grpcEnabled property to return false (disabled)
        when(mockAnnotation.grpcEnabled()).thenReturn(false);

        // Use raw types to avoid generic issues
        when(roundEnv.getElementsAnnotatedWith(PipelineStep.class))
                .thenReturn((Set) Collections.singleton(mockServiceClass));

        PipelineStepProcessor localProcessor = new PipelineStepProcessor();
        ProcessingEnvironment localProcessingEnv = mock(ProcessingEnvironment.class);
        when(localProcessingEnv.getMessager()).thenReturn(mock(Messager.class));
        javax.lang.model.util.Elements elementsUtils = mock(javax.lang.model.util.Elements.class);
        when(localProcessingEnv.getElementUtils()).thenReturn(elementsUtils);
        when(localProcessingEnv.getFiler()).thenReturn(mock(Filer.class));
        when(localProcessingEnv.getSourceVersion()).thenReturn(SourceVersion.RELEASE_21);

        // Mock the package for the service class
        PackageElement packageElementUtils = mock(PackageElement.class);
        javax.lang.model.element.Name packageName = mock(javax.lang.model.element.Name.class);
        when(packageName.toString()).thenReturn("test");
        when(packageElementUtils.getQualifiedName()).thenReturn(packageName);
        when(elementsUtils.getPackageOf(mockServiceClass)).thenReturn(packageElementUtils);
        when(elementsUtils.getTypeElement(mockServiceClass.getQualifiedName().toString())).thenReturn(mockServiceClass);

        // Mock the annotation mirror for PipelineStep
        AnnotationMirror mockAnnotationMirror = mock(AnnotationMirror.class);
        when(mockServiceClass.getAnnotationMirrors())
                .thenReturn((List)Collections.singletonList(mockAnnotationMirror));
        javax.lang.model.type.DeclaredType declaredType = mock(javax.lang.model.type.DeclaredType.class);
        when(mockAnnotationMirror.getAnnotationType()).thenReturn(declaredType);
        when(declaredType.toString()).thenReturn("org.pipelineframework.annotation.PipelineStep");

        // Mock annotation values to ensure the extractor can access them
        // Removed duplicate: when(elementsUtils.getPackageOf(mockServiceClass)).thenReturn(packageElementUtils);

        // Mock the element values map for the annotation with wildcard types
        java.util.Map elementValuesMap = new java.util.HashMap();

        // Create mock executable elements for the annotation attributes
        ExecutableElement inputTypeElement = createMockExecutableElement("inputType");
        ExecutableElement inputGrpcTypeElement = createMockExecutableElement("inputGrpcType");
        ExecutableElement inboundMapperElement = createMockExecutableElement("inboundMapper");
        ExecutableElement outputTypeElement = createMockExecutableElement("outputType");
        ExecutableElement outputGrpcTypeElement = createMockExecutableElement("outputGrpcType");
        ExecutableElement outboundMapperElement = createMockExecutableElement("outboundMapper");
        ExecutableElement stepTypeElement = createMockExecutableElement("stepType");
        ExecutableElement grpcImplElement = createMockExecutableElement("grpcImpl");
        ExecutableElement grpcStubElement = createMockExecutableElement("grpcStub");
        ExecutableElement grpcClientElement = createMockExecutableElement("grpcClient");
        ExecutableElement sideEffectElement = createMockExecutableElement("sideEffect");
        ExecutableElement pathElement = createMockExecutableElement("path");

        // Create mock TypeElement objects for the types using the helper
        javax.lang.model.type.DeclaredType stringTypeMirror = createMockTypeMirror("java.lang.String");
        javax.lang.model.type.DeclaredType integerTypeMirror = createMockTypeMirror("java.lang.Integer");
        javax.lang.model.type.DeclaredType voidTypeMirror = createMockTypeMirror("java.lang.Void");
        javax.lang.model.type.DeclaredType grpcStringTypeMirror = createMockTypeMirror("test.TestServiceGrpcMessage");
        javax.lang.model.type.DeclaredType grpcIntegerTypeMirror = createMockTypeMirror("test.TestServiceGrpcMessageResponse");

        // Create mock annotation values with proper TypeMirror objects
        AnnotationValue inputTypeValue = mock(AnnotationValue.class);
        when(inputTypeValue.getValue()).thenReturn(stringTypeMirror);
        elementValuesMap.put(inputTypeElement, inputTypeValue);

        AnnotationValue inputGrpcTypeValue = mock(AnnotationValue.class);
        when(inputGrpcTypeValue.getValue()).thenReturn(grpcStringTypeMirror);
        elementValuesMap.put(inputGrpcTypeElement, inputGrpcTypeValue);

        AnnotationValue inboundMapperValue = mock(AnnotationValue.class);
        when(inboundMapperValue.getValue()).thenReturn(voidTypeMirror);
        elementValuesMap.put(inboundMapperElement, inboundMapperValue);

        AnnotationValue outputTypeValue = mock(AnnotationValue.class);
        when(outputTypeValue.getValue()).thenReturn(integerTypeMirror);
        elementValuesMap.put(outputTypeElement, outputTypeValue);

        AnnotationValue outputGrpcTypeValue = mock(AnnotationValue.class);
        when(outputGrpcTypeValue.getValue()).thenReturn(grpcIntegerTypeMirror);
        elementValuesMap.put(outputGrpcTypeElement, outputGrpcTypeValue);

        AnnotationValue outboundMapperValue = mock(AnnotationValue.class);
        when(outboundMapperValue.getValue()).thenReturn(voidTypeMirror);
        elementValuesMap.put(outboundMapperElement, outboundMapperValue);

        AnnotationValue stepTypeValue = mock(AnnotationValue.class);
        when(stepTypeValue.getValue()).thenReturn("org.pipelineframework.step.StepOneToOne");
        elementValuesMap.put(stepTypeElement, stepTypeValue);

        AnnotationValue grpcImplValue = mock(AnnotationValue.class);
        when(grpcImplValue.getValue()).thenReturn(grpcStringTypeMirror);
        elementValuesMap.put(grpcImplElement, grpcImplValue);

        AnnotationValue grpcStubValue = mock(AnnotationValue.class);
        when(grpcStubValue.getValue()).thenReturn(grpcStringTypeMirror);
        elementValuesMap.put(grpcStubElement, grpcStubValue);

        AnnotationValue grpcClientValue = mock(AnnotationValue.class);
        when(grpcClientValue.getValue()).thenReturn("testClient");
        elementValuesMap.put(grpcClientElement, grpcClientValue);

        AnnotationValue sideEffectValue = mock(AnnotationValue.class);
        when(sideEffectValue.getValue()).thenReturn(voidTypeMirror);
        elementValuesMap.put(sideEffectElement, sideEffectValue);

        AnnotationValue pathValue = mock(AnnotationValue.class);
        when(pathValue.getValue()).thenReturn("/test");
        elementValuesMap.put(pathElement, pathValue);

        when(mockAnnotationMirror.getElementValues()).thenReturn(elementValuesMap);

        localProcessor.init(localProcessingEnv);

        // When annotations are present (i.e., @PipelineStep), processing should occur and return
        // true
        boolean result = localProcessor.process(annotations, roundEnv);

        assertTrue(result);
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

    // Resource path and DTO type derivation are covered in RestResourceRendererTest.

    @Disabled("TODO(issues/annotation-processor-tests): add IR extraction tests for interface detection.")
    @Test
    void testImplementsInterface() {
    }

    @Disabled("TODO(issues/annotation-processor-tests): add IR extraction tests for reactive service detection.")
    @Test
    void testImplementsReactiveService() {
    }

    private ExecutableElement createMockExecutableElement(String name) {
        ExecutableElement element = mock(ExecutableElement.class);
        javax.lang.model.element.Name elementName = mock(javax.lang.model.element.Name.class);
        when(elementName.toString()).thenReturn(name);
        when(element.getSimpleName()).thenReturn(elementName);
        when(element.getSimpleName().toString()).thenReturn(name);
        return element;
    }

    private javax.lang.model.type.DeclaredType createMockTypeMirror(String qualifiedName) {
        String packageName = qualifiedName.contains(".")
            ? qualifiedName.substring(0, qualifiedName.lastIndexOf('.'))
            : "";
        TypeElement typeElement = mock(TypeElement.class);
        Name nameMock = mock(Name.class);
        when(nameMock.toString()).thenReturn(qualifiedName);
        when(typeElement.getQualifiedName()).thenReturn(nameMock);
        when(typeElement.getKind()).thenReturn(ElementKind.CLASS);
        when(typeElement.getModifiers()).thenReturn(Collections.emptySet());

        Name simpleNameMock = mock(Name.class);
        String simpleName = qualifiedName.contains(".") ? qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1) : qualifiedName;
        when(simpleNameMock.toString()).thenReturn(simpleName);
        when(typeElement.getSimpleName()).thenReturn(simpleNameMock);

        PackageElement packageElement = mock(PackageElement.class);
        Name packageNameMock = mock(Name.class);
        when(packageNameMock.toString()).thenReturn(packageName);
        when(packageElement.getQualifiedName()).thenReturn(packageNameMock);
        when(packageElement.getKind()).thenReturn(ElementKind.PACKAGE);
        when(typeElement.getEnclosingElement()).thenReturn(packageElement);

        javax.lang.model.type.DeclaredType typeMirror = mock(javax.lang.model.type.DeclaredType.class);
        when(typeMirror.asElement()).thenReturn(typeElement);
        when(typeMirror.toString()).thenReturn(qualifiedName);
        when(typeMirror.getKind()).thenReturn(javax.lang.model.type.TypeKind.DECLARED);
        when(typeMirror.getTypeArguments()).thenReturn(java.util.Collections.emptyList());

        javax.lang.model.type.NoType noType = mock(javax.lang.model.type.NoType.class);
        when(noType.getKind()).thenReturn(javax.lang.model.type.TypeKind.NONE);
        when(typeMirror.getEnclosingType()).thenReturn(noType);

        // Mock the accept method for both the declared type and the enclosing NoType
        when(typeMirror.accept(any(), any())).thenAnswer(invocation -> {
            javax.lang.model.type.TypeVisitor visitor = invocation.getArgument(0);
            return visitor.visitDeclared(typeMirror, invocation.getArgument(1));
        });
        when(noType.accept(any(), any())).thenAnswer(invocation -> {
            javax.lang.model.type.TypeVisitor visitor = invocation.getArgument(0);
            return visitor.visitNoType(noType, invocation.getArgument(1));
        });

        return typeMirror;
    }
}
