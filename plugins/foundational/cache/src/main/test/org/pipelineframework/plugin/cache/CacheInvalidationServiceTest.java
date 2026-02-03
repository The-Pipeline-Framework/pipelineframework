package org.pipelineframework.plugin.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.util.TypeLiteral;
import org.junit.jupiter.api.Test;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

import io.smallrye.mutiny.Uni;

class CacheInvalidationServiceTest {

    @Test
    void invalidatesWhenReplayEnabled() {
        CacheInvalidationService<String> service = new CacheInvalidationService<>();
        TestCacheManager cacheManager = new TestCacheManager();
        service.cacheManager = cacheManager;
        service.cacheKeyResolver = resolverWith(new FixedStrategy());

        PipelineContextHolder.set(new PipelineContext("v1", "true", null));
        try {
            String result = service.process("item").await().indefinitely();
            assertEquals("item", result);
            assertEquals("v1:key", cacheManager.invalidatedKey);
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void skipsWhenReplayDisabled() {
        CacheInvalidationService<String> service = new CacheInvalidationService<>();
        TestCacheManager cacheManager = new TestCacheManager();
        service.cacheManager = cacheManager;
        service.cacheKeyResolver = resolverWith(new FixedStrategy());

        PipelineContextHolder.set(new PipelineContext("v1", "false", null));
        try {
            service.process("item").await().indefinitely();
            assertNull(cacheManager.invalidatedKey);
        } finally {
            PipelineContextHolder.clear();
        }
    }

    @Test
    void skipsWhenNoCacheKeyResolved() {
        CacheInvalidationService<String> service = new CacheInvalidationService<>();
        TestCacheManager cacheManager = new TestCacheManager();
        service.cacheManager = cacheManager;
        service.cacheKeyResolver = resolverWith(new EmptyStrategy());

        PipelineContextHolder.set(new PipelineContext("v1", "true", null));
        try {
            service.process("item").await().indefinitely();
            assertNull(cacheManager.invalidatedKey);
        } finally {
            PipelineContextHolder.clear();
        }
    }

    private CacheKeyResolver resolverWith(CacheKeyStrategy strategy) {
        CacheKeyResolver resolver = new CacheKeyResolver();
        resolver.strategies = new FixedInstance<>(List.of(strategy));
        return resolver;
    }

    static final class FixedStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("key");
        }

        @Override
        public int priority() {
            return 10;
        }
    }

    static final class EmptyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.empty();
        }
    }

    static final class TestCacheManager extends CacheManager {
        String invalidatedKey;

        @Override
        public Uni<Boolean> invalidate(String key) {
            invalidatedKey = key;
            return Uni.createFrom().item(true);
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
