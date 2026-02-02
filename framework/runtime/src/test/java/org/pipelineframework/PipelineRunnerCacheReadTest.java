package org.pipelineframework;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.*;
import org.pipelineframework.context.PipelineCacheStatusHolder;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.*;

class PipelineRunnerCacheReadTest {

    @Test
    void cacheHitReturnsCachedValueAndSkipsStep() {
        CountingStep step = new CountingStep();
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new FixedReader(Map.of("v1:key", "cached-value")),
            List.of(new FixedKeyStrategy()),
            "prefer-cache");

        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Object result = PipelineRunner.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item("input"),
            false,
            128,
            null,
            null,
            support,
            context);

        String value = ((Uni<String>) result).await().indefinitely();
        assertEquals("cached-value", value);
        assertEquals(0, step.calls.get());
    }

    @Test
    void requireCacheFailsWhenMissing() {
        CountingStep step = new CountingStep();
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new FixedReader(Map.of()),
            List.of(new FixedKeyStrategy()),
            "require-cache");

        PipelineContext context = new PipelineContext("v1", null, "require-cache");

        Object result = PipelineRunner.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item("input"),
            false,
            128,
            null,
            null,
            support,
            context);

        assertThrows(CachePolicyViolation.class, () -> ((Uni<String>) result).await().indefinitely());
        assertEquals(0, step.calls.get());
    }

    @Test
    void cacheMissFallsThroughToStep() {
        CountingStep step = new CountingStep();
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new FixedReader(Map.of()),
            List.of(new FixedKeyStrategy()),
            "prefer-cache");

        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Object result = PipelineRunner.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item("input"),
            false,
            128,
            null,
            null,
            support,
            context);

        String value = ((Uni<String>) result).await().indefinitely();
        assertEquals("computed-input", value);
        assertEquals(1, step.calls.get());
    }

    @Test
    void targetedStrategyPreferredWhenStepProvidesTargetType() {
        CountingTargetStep step = new CountingTargetStep();
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new FixedReader(Map.of("v1:target", "cached-target")),
            List.of(new FallbackKeyStrategy(), new TargetedKeyStrategy()),
            "prefer-cache");

        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Object result = PipelineRunner.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item("input"),
            false,
            128,
            null,
            null,
            support,
            context);

        String value = ((Uni<String>) result).await().indefinitely();
        assertEquals("cached-target", value);
        assertEquals(0, step.calls.get());
    }

    @Test
    void cacheReadBypassIgnoresCacheHit() {
        CountingBypassStep step = new CountingBypassStep();
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new FixedReader(Map.of("v1:key", "cached-value")),
            List.of(new FixedKeyStrategy()),
            "prefer-cache");

        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Object result = PipelineRunner.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item("input"),
            false,
            128,
            null,
            null,
            support,
            context);

        String value = ((Uni<String>) result).await().indefinitely();
        assertEquals("computed-input", value);
        assertEquals(1, step.calls.get());
    }

    @Test
    void targetedStrategyReturningBlankSkipsFallbackResolution() {
        PipelineRunner.CacheReadSupport support = new PipelineRunner.CacheReadSupport(
            new FixedReader(Map.of("v1:fallback", "cached-fallback")),
            List.of(new EmptyTargetKeyStrategy(), new FallbackKeyStrategy()),
            "prefer-cache");

        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Optional<String> resolved = support.resolveKey("input", context, String.class);
        assertTrue(resolved.isEmpty());
    }

    @Test
    void selectsHighestPriorityCacheReader() throws Exception {
        PipelineRunner runner = new PipelineRunner();
        runner.cacheReaders = new SimpleInstance<>(List.of(
            new LowPriorityReader(),
            new HighPriorityReader()));
        runner.cacheKeyStrategies = new SimpleInstance<>(List.of(new FixedKeyStrategy()));
        runner.cachePolicyDefault = "prefer-cache";

        java.lang.reflect.Method method = PipelineRunner.class.getDeclaredMethod("buildCacheReadSupport");
        method.setAccessible(true);
        PipelineRunner.CacheReadSupport support = (PipelineRunner.CacheReadSupport) method.invoke(runner);

        CountingStep step = new CountingStep();
        PipelineContext context = new PipelineContext("v1", null, "prefer-cache");

        Object result = PipelineRunner.applyOneToOneUnchecked(
            step,
            Uni.createFrom().item("input"),
            false,
            128,
            null,
            null,
            support,
            context);

        String value = ((Uni<String>) result).await().indefinitely();
        assertEquals("cached-high", value);
        assertEquals(0, step.calls.get());
    }

    @Test
    void cachePolicyEnforcementUsesContextSnapshot() {
        CountingStep step = new CountingStep();
        PipelineContext context = new PipelineContext("v1", null, "require-cache");
        PipelineCacheStatusHolder.set(CacheStatus.MISS);
        try {
            Object result = PipelineRunner.applyOneToOneUnchecked(
                step,
                Uni.createFrom().item("input"),
                false,
                128,
                null,
                null,
                null,
                context);

            assertThrows(CachePolicyViolation.class, () -> ((Uni<String>) result).await().indefinitely());
        } finally {
            PipelineCacheStatusHolder.clear();
        }
    }

    static class CountingStep extends ConfigurableStep implements StepOneToOne<String, String> {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public Uni<String> applyOneToOne(String input) {
            calls.incrementAndGet();
            return Uni.createFrom().item("computed-" + input);
        }
    }

    static final class CountingTargetStep extends CountingStep implements CacheKeyTarget {
        @Override
        public Class<?> cacheKeyTargetType() {
            return String.class;
        }
    }

    static final class CountingBypassStep extends CountingStep implements CacheReadBypass {
    }

    static final class FixedKeyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("key");
        }

        @Override
        public int priority() {
            return 100;
        }
    }

    static final class TargetedKeyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("target");
        }

        @Override
        public boolean supportsTarget(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public int priority() {
            return 10;
        }
    }

    static final class FallbackKeyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("fallback");
        }

        @Override
        public int priority() {
            return 100;
        }
    }

    static final class EmptyTargetKeyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of(" ");
        }

        @Override
        public boolean supportsTarget(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public int priority() {
            return 200;
        }
    }

    static final class HighPriorityReader implements PipelineCacheReader {
        @Override
        public Uni<Optional<Object>> get(String key) {
            Object value = "v1:key".equals(key) ? "cached-high" : null;
            return Uni.createFrom().item(Optional.ofNullable(value));
        }

        @Override
        public Uni<Boolean> exists(String key) {
            return Uni.createFrom().item("v1:key".equals(key));
        }

        @Override
        public int priority() {
            return 10;
        }
    }

    static final class LowPriorityReader implements PipelineCacheReader {
        @Override
        public Uni<Optional<Object>> get(String key) {
            Object value = "v1:key".equals(key) ? "cached-low" : null;
            return Uni.createFrom().item(Optional.ofNullable(value));
        }

        @Override
        public Uni<Boolean> exists(String key) {
            return Uni.createFrom().item("v1:key".equals(key));
        }

        @Override
        public int priority() {
            return 1;
        }
    }

    static final class SimpleInstance<T> implements Instance<T> {
        private final List<T> items;

        SimpleInstance(List<T> items) {
            this.items = items;
        }

        @Override
        public Instance<T> select(Annotation... qualifiers) {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public boolean isUnsatisfied() {
            return items.isEmpty();
        }

        @Override
        public boolean isAmbiguous() {
            return items.size() > 1;
        }

        @Override
        public void destroy(T instance) {
        }

        @Override
        public Handle<T> getHandle() {
            throw new UnsupportedOperationException("Not implemented for test");
        }

        @Override
        public Iterable<? extends Handle<T>> handles() {
            return List.of();
        }

        @Override
        public java.util.Iterator<T> iterator() {
            return items.iterator();
        }

        @Override
        public T get() {
            return items.isEmpty() ? null : items.get(0);
        }

        @Override
        public java.util.stream.Stream<T> stream() {
            return items.stream();
        }
    }

    static final class FixedReader implements PipelineCacheReader {
        private final Map<String, Object> cache;

        FixedReader(Map<String, Object> cache) {
            this.cache = cache;
        }

        @Override
        public Uni<Optional<Object>> get(String key) {
            return Uni.createFrom().item(Optional.ofNullable(cache.get(key)));
        }

        @Override
        public Uni<Boolean> exists(String key) {
            return Uni.createFrom().item(cache.containsKey(key));
        }
    }
}
