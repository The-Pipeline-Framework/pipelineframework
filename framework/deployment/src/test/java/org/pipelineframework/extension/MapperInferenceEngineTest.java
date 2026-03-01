package org.pipelineframework.extension;

import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.Test;
import org.pipelineframework.mapper.Mapper;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapperInferenceEngineTest {

    @Test
    void buildsRegistryForUniqueMapperPairs() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class, MapperA.class,
            GrpcB.class, DtoB.class, DomainB.class, MapperB.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        DotName domainB = DotName.createSimple(DomainB.class.getName());
        DotName grpcA = DotName.createSimple(GrpcA.class.getName());
        DotName grpcB = DotName.createSimple(GrpcB.class.getName());

        assertEquals(2, registry.pairToMapper().size());
        assertNotNull(registry.pairToMapper().get(new MapperInferenceEngine.MapperPairKey(domainA, grpcA)));
        assertNotNull(registry.pairToMapper().get(new MapperInferenceEngine.MapperPairKey(domainB, grpcB)));
        assertEquals(MapperA.class.getName(),
                registry.pairToMapper().get(new MapperInferenceEngine.MapperPairKey(domainA, grpcA)).name().toString());
        assertEquals(MapperB.class.getName(),
                registry.pairToMapper().get(new MapperInferenceEngine.MapperPairKey(domainB, grpcB)).name().toString());
    }

    @Test
    void failsWhenPairHasDuplicateMappers() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class, MapperA.class,
            GrpcA2.class, DtoA2.class, MapperADuplicate.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);

        IllegalStateException error = assertThrows(IllegalStateException.class, engine::buildRegistry);
        assertTrue(error.getMessage().contains("Duplicate mapper found for pair"));
    }

    @Test
    void failsWhenMapperHasErasedTypeVariable() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, GenericMapper.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);

        IllegalStateException error = assertThrows(IllegalStateException.class, engine::buildRegistry);
        assertTrue(error.getMessage().contains("erased"));
    }

    @Test
    void buildsEmptyRegistryWhenNoMappersFound() throws Exception {
        Index index = indexOf(Mapper.class);

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        assertEquals(0, registry.pairToMapper().size());
        assertEquals(0, registry.mapperToPair().size());
    }

    @Test
    void failsWhenEngineConstructedWithNullIndex() {
        assertThrows(NullPointerException.class, () -> new MapperInferenceEngine(null));
    }

    @Test
    void inferenceResultRequiresMapperClassWhenSuccess() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(null, true, null));
        assertTrue(error.getMessage().contains("requires non-null mapperClass"));
    }

    @Test
    void inferenceResultRequiresNullErrorMessageWhenSuccess() throws Exception {
        Index index = indexOf(Mapper.class, GrpcA.class, DomainA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();
        var mapperClass = registry.pairToMapper().values().iterator().next();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(mapperClass, true, "error"));
        assertTrue(error.getMessage().contains("requires null errorMessage"));
    }

    @Test
    void inferenceResultRequiresNullMapperClassWhenFailure() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(null, false, null));
        assertTrue(error.getMessage().contains("requires non-null, non-blank errorMessage"));
    }

    @Test
    void inferenceResultRequiresErrorMessageWhenFailure() throws Exception {
        Index index = indexOf(Mapper.class, GrpcA.class, DomainA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();
        var mapperClass = registry.pairToMapper().values().iterator().next();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(mapperClass, false, "error"));
        assertTrue(error.getMessage().contains("requires mapperClass==null"));
    }

    @Test
    void inferenceResultAcceptsValidSuccessState() throws Exception {
        Index index = indexOf(Mapper.class, GrpcA.class, DomainA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();
        var mapperClass = registry.pairToMapper().values().iterator().next();

        MapperInferenceEngine.InferenceResult result =
            new MapperInferenceEngine.InferenceResult(mapperClass, true, null);
        assertTrue(result.success());
        assertNotNull(result.mapperClass());
    }

    @Test
    void inferenceResultAcceptsValidFailureState() {
        MapperInferenceEngine.InferenceResult result =
            new MapperInferenceEngine.InferenceResult(null, false, "Test error");
        assertFalse(result.success());
        assertEquals("Test error", result.errorMessage());
    }

    @Test
    void mapperPairKeyEquality() {
        DotName domainA = DotName.createSimple(DomainA.class.getName());
        DotName grpcA = DotName.createSimple(GrpcA.class.getName());
        DotName domainB = DotName.createSimple(DomainB.class.getName());

        MapperInferenceEngine.MapperPairKey key1 = new MapperInferenceEngine.MapperPairKey(domainA, grpcA);
        MapperInferenceEngine.MapperPairKey key2 = new MapperInferenceEngine.MapperPairKey(domainA, grpcA);
        MapperInferenceEngine.MapperPairKey key3 = new MapperInferenceEngine.MapperPairKey(domainB, grpcA);

        assertEquals(key1, key2);
        assertNotEquals(key1, key3);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    private static Index indexOf(Class<?>... classes) throws IOException {
        Indexer indexer = new Indexer();
        for (Class<?> clazz : classes) {
            String resourceName = clazz.getName().replace('.', '/') + ".class";
            try (InputStream stream = clazz.getClassLoader().getResourceAsStream(resourceName)) {
                if (stream == null) {
                    throw new IOException("Could not load class bytes for " + clazz.getName());
                }
                indexer.index(stream);
            }
        }
        return indexer.complete();
    }

    static final class GrpcA {}
    static final class DtoA {}
    static final class DomainA {}
    static final class GrpcB {}
    static final class DtoB {}
    static final class DomainB {}
    static final class GrpcA2 {}
    static final class DtoA2 {}

    static final class MapperA implements Mapper<DomainA, GrpcA> {
        @Override
        public DomainA fromExternal(GrpcA grpc) {
            return null;
        }

        @Override
        public GrpcA toExternal(DomainA domain) {
            return null;
        }
    }

    static final class MapperB implements Mapper<DomainB, GrpcB> {
        @Override
        public DomainB fromExternal(GrpcB grpc) {
            return null;
        }

        @Override
        public GrpcB toExternal(DomainB domain) {
            return null;
        }
    }

    static final class MapperADuplicate implements Mapper<DomainA, GrpcA> {
        @Override
        public DomainA fromExternal(GrpcA grpc) {
            return null;
        }

        @Override
        public GrpcA toExternal(DomainA domain) {
            return null;
        }
    }

    static final class GenericMapper<T> implements Mapper<T, GrpcA> {
        @Override
        public T fromExternal(GrpcA grpc) {
            return null;
        }

        @Override
        public GrpcA toExternal(T domain) {
            return null;
        }
    }
}
