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

package org.pipelineframework.processor.renderer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.LocalBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.ServiceApiKind;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpringLocalClientStepRendererTest {

    @TempDir
    Path tempDir;

    @Test
    void rendersSpringNativeUnaryLocalClientStep() throws Exception {
        SpringLocalClientStepRenderer renderer = new SpringLocalClientStepRenderer();

        renderer.render(new LocalBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.REACTIVE)),
            new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null));

        Path clientStep = tempDir.resolve("com/example/service/pipeline/PaymentLocalClientStep.java");
        String source = Files.readString(clientStep);
        assertTrue(source.contains("import org.pipelineframework.runtime.core.PipelineUnaryStep;"));
        assertTrue(source.contains("import org.springframework.stereotype.Component;"));
        assertTrue(source.contains("public class PaymentLocalClientStep implements PipelineUnaryStep<PaymentRecord, PaymentStatus>"));
        assertTrue(source.contains("private final PaymentService paymentService;"));
        assertTrue(source.contains("public PaymentLocalClientStep(PaymentService paymentService)"));
        assertTrue(source.contains("return this.paymentService.process(input).subscribeAsCompletionStage();"));
        assertFalse(source.contains("io.quarkus."));
        assertFalse(source.contains("jakarta.enterprise."));
        assertFalse(source.contains("jakarta.inject."));
        assertFalse(source.contains("io.vertx."));
        assertFalse(source.contains("org.jboss.resteasy."));
        assertFalse(source.contains("jakarta.ws.rs."));
    }

    @Test
    void rejectsNonUnaryShape() {
        SpringLocalClientStepRenderer renderer = new SpringLocalClientStepRenderer();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new LocalBinding(model(StreamingShape.UNARY_STREAMING, ServiceApiKind.REACTIVE)),
                new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null)));
    }

    @Test
    void rejectsBlockingAuthoredService() {
        SpringLocalClientStepRenderer renderer = new SpringLocalClientStepRenderer();

        assertThrows(IllegalArgumentException.class,
            () -> renderer.render(new LocalBinding(model(StreamingShape.UNARY_UNARY, ServiceApiKind.BLOCKING)),
                new GenerationContext(null, tempDir, DeploymentRole.ORCHESTRATOR_CLIENT, Set.of(), null, null)));
    }

    private PipelineStepModel model(StreamingShape shape, ServiceApiKind apiKind) {
        return new PipelineStepModel.Builder()
            .serviceName("PaymentService")
            .generatedName("PaymentService")
            .servicePackage("com.example.service")
            .serviceClassName(ClassName.get("com.example.service", "PaymentService"))
            .inputMapping(new TypeMapping(ClassName.get("com.example.service", "PaymentRecord"), null, false))
            .outputMapping(new TypeMapping(ClassName.get("com.example.service", "PaymentStatus"), null, false))
            .streamingShape(shape)
            .enabledTargets(Set.of(GenerationTarget.LOCAL_CLIENT_STEP))
            .executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.PIPELINE_SERVER)
            .serviceApiKind(apiKind)
            .build();
    }
}
