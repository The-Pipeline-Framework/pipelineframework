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

package org.pipelineframework.cache;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachePolicyEnforcerTest {

    @AfterEach
    void cleanup() {
        PipelineCacheStatusHolder.clear();
        PipelineContextHolder.clear();
    }

    @Test
    void enforce_ReturnsItemWhenNoStatus() {
        String input = "item";

        Uni<String> result = CachePolicyEnforcer.enforce(input);
        UniAssertSubscriber<String> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(input, subscriber.getItem());
    }

    @Test
    void enforce_RequireCacheFailsOnMiss() {
        String input = "item";
        PipelineContextHolder.set(new PipelineContext(null, null, "require-cache"));
        PipelineCacheStatusHolder.set(CacheStatus.MISS);

        Uni<String> result = CachePolicyEnforcer.enforce(input);
        UniAssertSubscriber<String> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitFailure();

        assertTrue(subscriber.getFailure() instanceof CachePolicyViolation);
    }

    @Test
    void enforce_RequireCacheAcceptsHit() {
        String input = "item";
        PipelineContextHolder.set(new PipelineContext(null, null, "require-cache"));
        PipelineCacheStatusHolder.set(CacheStatus.HIT);

        Uni<String> result = CachePolicyEnforcer.enforce(input);
        UniAssertSubscriber<String> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(input, subscriber.getItem());
    }

    @Test
    void enforce_PreferCacheIgnoresMiss() {
        String input = "item";
        PipelineContextHolder.set(new PipelineContext(null, null, "prefer-cache"));
        PipelineCacheStatusHolder.set(CacheStatus.MISS);

        Uni<String> result = CachePolicyEnforcer.enforce(input);
        UniAssertSubscriber<String> subscriber = result.subscribe().withSubscriber(UniAssertSubscriber.create());
        subscriber.awaitItem();

        assertSame(input, subscriber.getItem());
    }
}
