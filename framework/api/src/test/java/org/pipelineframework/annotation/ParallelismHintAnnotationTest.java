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

package org.pipelineframework.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ParallelismHintAnnotationTest {

    @Test
    void parallelismHintHasRuntimeRetentionAndTypeTarget() {
        assertEquals(RetentionPolicy.RUNTIME,
            ParallelismHint.class.getAnnotation(Retention.class).value());
        assertEquals(Set.of(ElementType.TYPE),
            Set.of(ParallelismHint.class.getAnnotation(Target.class).value()));
    }

    @Test
    void parallelismHintExposesPortablePolicyAttributesAndDefaults() throws NoSuchMethodException {
        Set<String> attributeNames = Arrays.stream(ParallelismHint.class.getDeclaredMethods())
            .map(Method::getName)
            .collect(Collectors.toSet());

        assertEquals(Set.of("ordering", "threadSafety"), attributeNames);
        assertEquals(OrderingRequirement.class,
            ParallelismHint.class.getDeclaredMethod("ordering").getReturnType());
        assertEquals(ThreadSafety.class,
            ParallelismHint.class.getDeclaredMethod("threadSafety").getReturnType());
        assertEquals(OrderingRequirement.RELAXED,
            ParallelismHint.class.getDeclaredMethod("ordering").getDefaultValue());
        assertEquals(ThreadSafety.SAFE,
            ParallelismHint.class.getDeclaredMethod("threadSafety").getDefaultValue());
    }
}
