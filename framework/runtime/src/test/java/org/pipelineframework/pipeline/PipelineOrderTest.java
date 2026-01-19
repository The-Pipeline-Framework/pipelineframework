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

package org.pipelineframework.pipeline;

import java.lang.reflect.Method;
import java.util.List;
import jakarta.inject.Inject;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.pipeline.order.OrderedStepA;
import org.pipelineframework.pipeline.order.OrderedStepB;

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
class PipelineOrderTest {

    @Inject
    PipelineExecutionService pipelineExecutionService;

    @Test
    void loadsStepsInPipelineOrder() throws Exception {
        Method method = PipelineExecutionService.class.getDeclaredMethod("loadPipelineSteps");
        method.setAccessible(true);

        @SuppressWarnings("unchecked")
        List<Object> steps = (List<Object>) method.invoke(pipelineExecutionService);

        List<Class<?>> stepTypes = steps.stream()
            .map(step -> {
                Class<?> type = step.getClass();
                return type.getName().endsWith("_ClientProxy") ? type.getSuperclass() : type;
            })
            .toList();

        assertEquals(
            List.of(OrderedStepA.class, OrderedStepB.class, OrderedStepA.class),
            stepTypes);
    }

}
