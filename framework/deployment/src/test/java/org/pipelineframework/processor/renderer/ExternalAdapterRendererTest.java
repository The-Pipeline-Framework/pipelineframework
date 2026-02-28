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

package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.TypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ExternalAdapterRenderer with Jackson fallback functionality.
 */
class ExternalAdapterRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void generatesExternalAdapterWithJacksonFallback() throws IOException {
        ClassName delegateService = ClassName.get("com.example.lib", "ExternalService");
        ClassName appInputType = ClassName.get("com.example.app", "AppInput");
        ClassName appOutputType = ClassName.get("com.example.app", "AppOutput");
        ClassName libInputType = ClassName.get("com.example.lib", "LibInput");
        ClassName libOutputType = ClassName.get("com.example.lib", "LibOutput");

        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("ProcessDataService")
                .generatedName("ProcessDataService")
                .servicePackage("com.example.pipeline")
                .serviceClassName(ClassName.get("com.example.pipeline", "ProcessDataService"))
                .inputMapping(new TypeMapping(appInputType, null, false, libInputType))
                .outputMapping(new TypeMapping(appOutputType, null, false, libOutputType))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.EXTERNAL_ADAPTER))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .delegateService(delegateService)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        ExternalAdapterBinding binding = new ExternalAdapterBinding(
                model,
                "ProcessDataService",
                "com.example.pipeline",
                delegateService.canonicalName(),
                null);

        ExternalAdapterRenderer renderer = new ExternalAdapterRenderer(GenerationTarget.EXTERNAL_ADAPTER);

        javax.annotation.processing.ProcessingEnvironment mockProcessingEnv =
            new TestProcessingEnvironment();

        GenerationContext ctx = new GenerationContext(
                mockProcessingEnv,
                tempDir,
                DeploymentRole.PIPELINE_SERVER,
                Set.of(),
                null,
                null);

        renderer.render(binding, ctx);

        Path generatedFile = tempDir.resolve("com/example/pipeline/pipeline/ProcessDataExternalAdapter.java");
        assertTrue(Files.exists(generatedFile), "Generated file should exist");

        String content = Files.readString(generatedFile);

        // Verify ObjectMapper field is generated
        assertTrue(content.contains("@Inject"), "Should have @Inject annotation");
        assertTrue(content.contains("private com.fasterxml.jackson.databind.ObjectMapper objectMapper"),
                "Should have ObjectMapper field");

        // Verify convertInput method is generated
        assertTrue(content.contains("private " + libInputType.simpleName() + " convertInput"),
                "Should have convertInput method");
        assertTrue(content.contains("objectMapper.convertValue"),
                "convertInput should use ObjectMapper.convertValue");
        assertTrue(content.contains("NonRetryableException"),
                "Should throw NonRetryableException on conversion failure");
        assertTrue(content.contains("Mapper fallback (JACKSON) failed"),
                "Should have descriptive error message");

        // Verify convertOutput method is generated
        assertTrue(content.contains("private " + appOutputType.simpleName() + " convertOutput"),
                "Should have convertOutput method");

        // Verify process method uses conversion
        assertTrue(content.contains("convertInput(input)"),
                "process should call convertInput");
        assertTrue(content.contains(".map(this::convertOutput)"),
                "process should call convertOutput via method reference");
    }

    @Test
    void generatesExternalAdapterWithExplicitMapper() throws IOException {
        ClassName delegateService = ClassName.get("com.example.lib", "ExternalService");
        ClassName externalMapper = ClassName.get("com.example.app", "ExternalMapperImpl");
        ClassName appInputType = ClassName.get("com.example.app", "AppInput");
        ClassName appOutputType = ClassName.get("com.example.app", "AppOutput");

        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("ProcessDataService")
                .generatedName("ProcessDataService")
                .servicePackage("com.example.pipeline")
                .serviceClassName(ClassName.get("com.example.pipeline", "ProcessDataService"))
                .inputMapping(new TypeMapping(appInputType, null, false))
                .outputMapping(new TypeMapping(appOutputType, null, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.EXTERNAL_ADAPTER))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .delegateService(delegateService)
                .externalMapper(externalMapper)
                .mapperFallbackMode(MapperFallbackMode.NONE)
                .build();

        ExternalAdapterBinding binding = new ExternalAdapterBinding(
                model,
                "ProcessDataService",
                "com.example.pipeline",
                delegateService.canonicalName(),
                externalMapper.canonicalName());

        ExternalAdapterRenderer renderer = new ExternalAdapterRenderer(GenerationTarget.EXTERNAL_ADAPTER);

        javax.annotation.processing.ProcessingEnvironment mockProcessingEnv =
            new TestProcessingEnvironment();

        GenerationContext ctx = new GenerationContext(
                mockProcessingEnv,
                tempDir,
                DeploymentRole.PIPELINE_SERVER,
                Set.of(),
                null,
                null);

        renderer.render(binding, ctx);

        Path generatedFile = tempDir.resolve("com/example/pipeline/pipeline/ProcessDataExternalAdapter.java");
        assertTrue(Files.exists(generatedFile));

        String content = Files.readString(generatedFile);

        // Verify externalMapper field is generated
        assertTrue(content.contains("private " + externalMapper.simpleName() + " externalMapper"),
                "Should have externalMapper field");

        // Verify ObjectMapper is NOT generated when explicit mapper is present
        assertFalse(content.contains("private com.fasterxml.jackson.databind.ObjectMapper objectMapper"),
                "Should not have ObjectMapper field when explicit mapper is provided");

        // Verify process uses externalMapper
        assertTrue(content.contains("externalMapper.toOperatorInput"),
                "Should use externalMapper for input conversion");
        assertTrue(content.contains("externalMapper.toApplicationOutput"),
                "Should use externalMapper for output conversion");

        // Verify convertInput/convertOutput methods are NOT generated
        assertFalse(content.contains("private " + appInputType.simpleName() + " convertInput"),
                "Should not have convertInput method when explicit mapper is provided");
        assertFalse(content.contains("private " + appOutputType.simpleName() + " convertOutput"),
                "Should not have convertOutput method when explicit mapper is provided");
    }

    @Test
    void generatesExternalAdapterClassName() {
        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("ProcessDataService")
                .generatedName("ProcessDataService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "ProcessDataService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.EXTERNAL_ADAPTER))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .build();

        String className = ExternalAdapterRenderer.getExternalAdapterClassName(model);
        assertEquals("ProcessDataExternalAdapter", className);
    }

    @Test
    void generatesExternalAdapterClassNameStripsServiceSuffix() {
        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("TestService")
                .generatedName("TestService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "TestService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.EXTERNAL_ADAPTER))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .build();

        String className = ExternalAdapterRenderer.getExternalAdapterClassName(model);
        assertEquals("TestExternalAdapter", className);
    }

    @Test
    void generatesStreamingExternalAdapterWithJacksonFallback() throws IOException {
        ClassName delegateService = ClassName.get("com.example.lib", "StreamingService");
        ClassName appInputType = ClassName.get("com.example.app", "AppInput");
        ClassName appOutputType = ClassName.get("com.example.app", "AppOutput");
        ClassName libInputType = ClassName.get("com.example.lib", "LibInput");
        ClassName libOutputType = ClassName.get("com.example.lib", "LibOutput");

        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("StreamingService")
                .generatedName("StreamingService")
                .servicePackage("com.example.pipeline")
                .serviceClassName(ClassName.get("com.example.pipeline", "StreamingService"))
                .inputMapping(new TypeMapping(appInputType, null, false, libInputType))
                .outputMapping(new TypeMapping(appOutputType, null, false, libOutputType))
                .streamingShape(StreamingShape.STREAMING_STREAMING)
                .enabledTargets(Set.of(GenerationTarget.EXTERNAL_ADAPTER))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .delegateService(delegateService)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        ExternalAdapterBinding binding = new ExternalAdapterBinding(
                model,
                "StreamingService",
                "com.example.pipeline",
                delegateService.canonicalName(),
                null);

        ExternalAdapterRenderer renderer = new ExternalAdapterRenderer(GenerationTarget.EXTERNAL_ADAPTER);

        javax.annotation.processing.ProcessingEnvironment mockProcessingEnv =
            new TestProcessingEnvironment();

        GenerationContext ctx = new GenerationContext(
                mockProcessingEnv,
                tempDir,
                DeploymentRole.PIPELINE_SERVER,
                Set.of(),
                null,
                null);

        renderer.render(binding, ctx);

        Path generatedFile = tempDir.resolve("com/example/pipeline/pipeline/StreamingExternalAdapter.java");
        assertTrue(Files.exists(generatedFile));

        String content = Files.readString(generatedFile);

        // Verify streaming operations use conversion
        assertTrue(content.contains("input.map(this::convertInput)"),
                "Streaming input should be mapped with convertInput");
        assertTrue(content.contains(".map(this::convertOutput)"),
                "Streaming output should be mapped with convertOutput");
    }

    private static class TestProcessingEnvironment implements javax.annotation.processing.ProcessingEnvironment {
        @Override
        public java.util.Map<String, String> getOptions() {
            return java.util.Map.of();
        }

        @Override
        public javax.annotation.processing.Messager getMessager() {
            return new javax.annotation.processing.Messager() {
                @Override
                public void printMessage(javax.tools.Diagnostic.Kind kind, CharSequence msg) {
                }

                @Override
                public void printMessage(javax.tools.Diagnostic.Kind kind, CharSequence msg, javax.lang.model.element.Element e) {
                }

                @Override
                public void printMessage(javax.tools.Diagnostic.Kind kind, CharSequence msg, javax.lang.model.element.Element e, javax.lang.model.element.AnnotationMirror a) {
                }

                @Override
                public void printMessage(javax.tools.Diagnostic.Kind kind, CharSequence msg, javax.lang.model.element.Element e, javax.lang.model.element.AnnotationMirror a, javax.lang.model.element.AnnotationValue v) {
                }
            };
        }

        @Override
        public javax.annotation.processing.Filer getFiler() {
            return null;
        }

        @Override
        public javax.lang.model.util.Elements getElementUtils() {
            return null;
        }

        @Override
        public javax.lang.model.util.Types getTypeUtils() {
            return null;
        }

        @Override
        public javax.lang.model.SourceVersion getSourceVersion() {
            return javax.lang.model.SourceVersion.RELEASE_21;
        }

        @Override
        public java.util.Locale getLocale() {
            return java.util.Locale.getDefault();
        }
    }
}
