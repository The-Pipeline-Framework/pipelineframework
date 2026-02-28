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

package org.pipelineframework.processor.ir;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PipelineStepModel with mapper fallback functionality.
 */
class PipelineStepModelTest {

    @Test
    void builderCreatesModelWithMapperFallbackMode() {
        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("TestService")
                .generatedName("TestService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "TestService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        assertEquals(MapperFallbackMode.JACKSON, model.mapperFallbackMode());
    }

    @Test
    void builderDefaultsMapperFallbackModeToNoneWhenNotSet() {
        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("TestService")
                .generatedName("TestService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "TestService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .build();

        assertEquals(MapperFallbackMode.NONE, model.mapperFallbackMode());
    }

    @Test
    void constructorDefaultsMapperFallbackModeToNoneWhenNull() {
        PipelineStepModel model = new PipelineStepModel(
                "TestService",
                "TestService",
                "com.example",
                ClassName.get("com.example", "TestService"),
                new TypeMapping(TypeName.INT, TypeName.INT, false),
                new TypeMapping(TypeName.INT, TypeName.INT, false),
                StreamingShape.UNARY_UNARY,
                Set.of(GenerationTarget.CLIENT_STEP),
                ExecutionMode.DEFAULT,
                DeploymentRole.PIPELINE_SERVER,
                false,
                null,
                OrderingRequirement.RELAXED,
                ThreadSafety.SAFE,
                null,
                null,
                null);

        assertEquals(MapperFallbackMode.NONE, model.mapperFallbackMode());
    }

    @Test
    void constructorAcceptsJacksonMapperFallbackMode() {
        PipelineStepModel model = new PipelineStepModel(
                "TestService",
                "TestService",
                "com.example",
                ClassName.get("com.example", "TestService"),
                new TypeMapping(TypeName.INT, TypeName.INT, false),
                new TypeMapping(TypeName.INT, TypeName.INT, false),
                StreamingShape.UNARY_UNARY,
                Set.of(GenerationTarget.CLIENT_STEP),
                ExecutionMode.DEFAULT,
                DeploymentRole.PIPELINE_SERVER,
                false,
                null,
                OrderingRequirement.RELAXED,
                ThreadSafety.SAFE,
                null,
                null,
                MapperFallbackMode.JACKSON);

        assertEquals(MapperFallbackMode.JACKSON, model.mapperFallbackMode());
    }

    @Test
    void toBuilderPreservesMapperFallbackMode() {
        PipelineStepModel original = new PipelineStepModel.Builder()
                .serviceName("TestService")
                .generatedName("TestService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "TestService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        PipelineStepModel rebuilt = original.toBuilder().build();

        assertEquals(MapperFallbackMode.JACKSON, rebuilt.mapperFallbackMode());
    }

    @Test
    void toBuilderAllowsChangingMapperFallbackMode() {
        PipelineStepModel original = new PipelineStepModel.Builder()
                .serviceName("TestService")
                .generatedName("TestService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "TestService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .mapperFallbackMode(MapperFallbackMode.NONE)
                .build();

        PipelineStepModel modified = original.toBuilder()
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        assertEquals(MapperFallbackMode.NONE, original.mapperFallbackMode());
        assertEquals(MapperFallbackMode.JACKSON, modified.mapperFallbackMode());
    }

    @Test
    void withDeploymentRolePreservesMapperFallbackMode() {
        PipelineStepModel original = new PipelineStepModel.Builder()
                .serviceName("TestService")
                .generatedName("TestService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "TestService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        PipelineStepModel modified = original.withDeploymentRole(DeploymentRole.ORCHESTRATOR_SERVER);

        assertEquals(MapperFallbackMode.JACKSON, modified.mapperFallbackMode());
        assertEquals(DeploymentRole.ORCHESTRATOR_SERVER, modified.deploymentRole());
    }

    @Test
    void modelWithDelegateServiceAndJacksonFallback() {
        ClassName delegateService = ClassName.get("com.example.lib", "ExternalService");

        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("DelegatedService")
                .generatedName("DelegatedService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "DelegatedService"))
                .inputMapping(new TypeMapping(
                        ClassName.get("com.example.app", "AppInput"),
                        TypeName.INT,
                        false,
                        ClassName.get("com.example.lib", "LibInput")))
                .outputMapping(new TypeMapping(
                        ClassName.get("com.example.app", "AppOutput"),
                        TypeName.INT,
                        false,
                        ClassName.get("com.example.lib", "LibOutput")))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .delegateService(delegateService)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        assertNotNull(model.delegateService());
        assertEquals(delegateService, model.delegateService());
        assertNull(model.externalMapper());
        assertEquals(MapperFallbackMode.JACKSON, model.mapperFallbackMode());
    }

    @Test
    void modelCanHaveBothExternalMapperAndMapperFallback() {
        ClassName externalMapper = ClassName.get("com.example.app", "ExternalMapperImpl");

        PipelineStepModel model = new PipelineStepModel.Builder()
                .serviceName("DelegatedService")
                .generatedName("DelegatedService")
                .servicePackage("com.example")
                .serviceClassName(ClassName.get("com.example", "DelegatedService"))
                .inputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .outputMapping(new TypeMapping(TypeName.INT, TypeName.INT, false))
                .streamingShape(StreamingShape.UNARY_UNARY)
                .enabledTargets(Set.of(GenerationTarget.CLIENT_STEP))
                .executionMode(ExecutionMode.DEFAULT)
                .deploymentRole(DeploymentRole.PIPELINE_SERVER)
                .externalMapper(externalMapper)
                .mapperFallbackMode(MapperFallbackMode.JACKSON)
                .build();

        assertNotNull(model.externalMapper());
        assertEquals(externalMapper, model.externalMapper());
        assertEquals(MapperFallbackMode.JACKSON, model.mapperFallbackMode());
    }

    @Test
    void legacyConstructorWithoutMapperFallbackDefaultsToNone() {
        PipelineStepModel model = new PipelineStepModel(
                "TestService",
                "TestService",
                "com.example",
                ClassName.get("com.example", "TestService"),
                new TypeMapping(TypeName.INT, TypeName.INT, false),
                new TypeMapping(TypeName.INT, TypeName.INT, false),
                StreamingShape.UNARY_UNARY,
                Set.of(GenerationTarget.CLIENT_STEP),
                ExecutionMode.DEFAULT,
                DeploymentRole.PIPELINE_SERVER,
                false,
                null,
                OrderingRequirement.RELAXED,
                ThreadSafety.SAFE,
                null,
                null);

        assertEquals(MapperFallbackMode.NONE, model.mapperFallbackMode());
    }
}