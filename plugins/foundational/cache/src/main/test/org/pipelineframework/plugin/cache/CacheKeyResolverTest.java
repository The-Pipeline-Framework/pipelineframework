package org.pipelineframework.plugin.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;

class CacheKeyResolverTest {

    @Test
    void resolvesTargetedStrategyBeforeFallback() {
        CacheKeyResolver resolver = new CacheKeyResolver();
        resolver.strategies = new FixedInstance<>(List.of(
            new FallbackStrategy(),
            new TargetedStrategy()
        ));

        Optional<String> resolved = resolver.resolveKey("input", new PipelineContext("v1", null, null), String.class);

        assertTrue(resolved.isPresent());
        assertEquals("target", resolved.get());
    }

    @Test
    void fallsBackWhenNoTargetedStrategyMatches() {
        CacheKeyResolver resolver = new CacheKeyResolver();
        resolver.strategies = new FixedInstance<>(List.of(
            new FallbackStrategy(),
            new NonTargetedStrategy()
        ));

        Optional<String> resolved = resolver.resolveKey("input", new PipelineContext("v1", null, null), Integer.class);

        assertTrue(resolved.isPresent());
        assertEquals("fallback", resolved.get());
    }

    static final class TargetedStrategy implements CacheKeyStrategy {
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

    static final class NonTargetedStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("nontarget");
        }

        @Override
        public int priority() {
            return 5;
        }
    }

    static final class FallbackStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("fallback");
        }

        @Override
        public int priority() {
            return 100;
        }
    }

    static final class FixedInstance<T> implements Instance<T> {
        private final List<T> values;

        FixedInstance(List<T> values) {
            this.values = values;
        }

        @Override
        public Instance<T> select(Annotation... qualifiers) {
            return this;
        }

        @Override
        public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) {
            return cast();
        }

        @Override
        public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) {
            return cast();
        }

        @Override
        public boolean isUnsatisfied() {
            return values.isEmpty();
        }

        @Override
        public boolean isAmbiguous() {
            return values.size() > 1;
        }

        @Override
        public void destroy(T instance) {
        }

        @Override
        public Handle<T> getHandle() {
            return new FixedHandle<>(values.isEmpty() ? null : values.get(0));
        }

        @Override
        public Iterable<? extends Handle<T>> handles() {
            return values.stream().map(FixedHandle::new).toList();
        }

        @Override
        public java.util.Iterator<T> iterator() {
            return values.iterator();
        }

        @Override
        public T get() {
            return values.get(0);
        }

        @Override
        public Stream<T> stream() {
            return values.stream();
        }

        @SuppressWarnings("unchecked")
        private <U extends T> Instance<U> cast() {
            return (Instance<U>) this;
        }
    }

    static final class FixedHandle<T> implements Instance.Handle<T> {
        private final T value;

        FixedHandle(T value) {
            this.value = value;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public jakarta.enterprise.inject.spi.Bean<T> getBean() {
            return null;
        }

        @Override
        public void destroy() {
        }

        @Override
        public void close() {
        }
    }
}
