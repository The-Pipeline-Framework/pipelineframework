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

package org.pipelineframework.config.pipeline;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineOrderExpanderTest {

    @Test
    void expandsOrderWithSideEffectClientSteps() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "GRPC",
            List.of(
                new PipelineYamlStep("Step A", "InFoo", "Foo"),
                new PipelineYamlStep("Step B", "InBar", "Bar"),
                new PipelineYamlStep("Step A Stream", "InFoo", "Foo")
            ),
            List.of(
                new PipelineYamlAspect("persistence", true, "GLOBAL", "AFTER_STEP", List.of()),
                new PipelineYamlAspect("cache", true, "GLOBAL", "AFTER_STEP", List.of()),
                new PipelineYamlAspect("cache-invalidate-all", true, "STEPS", "BEFORE_STEP", List.of("StepBService"))
            )
        );

        List<String> expanded = PipelineOrderExpander.expand(
            List.of("StepA", "StepB", "StepC"),
            config,
            null
        );

        assertEquals(
            List.of(
                "StepA",
                "org.example.service.pipeline.PersistenceFooSideEffectGrpcClientStep",
                "org.example.service.pipeline.CacheFooSideEffectGrpcClientStep",
                "org.example.service.pipeline.CacheInvalidateAllInBarSideEffectGrpcClientStep",
                "StepB",
                "org.example.service.pipeline.PersistenceBarSideEffectGrpcClientStep",
                "org.example.service.pipeline.CacheBarSideEffectGrpcClientStep",
                "StepC"
            ),
            expanded
        );
    }

    @Test
    void skipsExpansionWhenOrderAlreadyIncludesSideEffects() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "GRPC",
            List.of(new PipelineYamlStep("Step A", "InFoo", "Foo")),
            List.of(new PipelineYamlAspect("persistence", true, "GLOBAL", "AFTER_STEP", List.of()))
        );

        List<String> baseOrder = List.of(
            "StepA",
            "org.example.service.pipeline.PersistenceFooSideEffectGrpcClientStep"
        );

        assertEquals(baseOrder, PipelineOrderExpander.expand(baseOrder, config, null));
    }

    @Test
    void usesStepPackageWhenBaseOrderContainsFullClassNames() {
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.example",
            "REST",
            List.of(new PipelineYamlStep("Process Crawl", "InFoo", "Foo")),
            List.of(new PipelineYamlAspect("persistence", true, "GLOBAL", "AFTER_STEP", List.of()))
        );

        List<String> expanded = PipelineOrderExpander.expand(
            List.of("org.acme.search.crawl.service.pipeline.ProcessCrawlRestClientStep"),
            config,
            null
        );

        assertEquals(
            List.of(
                "org.acme.search.crawl.service.pipeline.ProcessCrawlRestClientStep",
                "org.acme.search.crawl.service.pipeline.PersistenceFooSideEffectRestClientStep"
            ),
            expanded
        );
    }
}
