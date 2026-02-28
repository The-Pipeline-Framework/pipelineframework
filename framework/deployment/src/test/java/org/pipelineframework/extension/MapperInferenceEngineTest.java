package org.pipelineframework.extension;

import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.Test;
import org.pipelineframework.mapper.Mapper;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapperInferenceEngineTest {

    @Test
    void buildsRegistryForUniqueDomainMappers() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class, MapperA.class,
            GrpcB.class, DtoB.class, DomainB.class, MapperB.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        DotName domainB = DotName.createSimple(DomainB.class.getName());

        assertEquals(2, registry.domainToMapper().size());
        assertNotNull(registry.domainToMapper().get(domainA));
        assertNotNull(registry.domainToMapper().get(domainB));
        assertEquals(MapperA.class.getName(), registry.domainToMapper().get(domainA).name().toString());
        assertEquals(MapperB.class.getName(), registry.domainToMapper().get(domainB).name().toString());
    }

    @Test
    void failsWhenDomainHasDuplicateMappers() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class, MapperA.class,
            GrpcA2.class, DtoA2.class, MapperADuplicate.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);

        IllegalStateException error = assertThrows(IllegalStateException.class, engine::buildRegistry);
        assertTrue(error.getMessage().contains("Duplicate mapper found for domain type"));
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
        Index index = indexOf(GrpcA.class, DtoA.class, DomainA.class);

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        assertEquals(0, registry.domainToMapper().size());
        assertEquals(0, registry.mapperToDomain().size());
    }

    @Test
    void resolvesMapperViaInheritance() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class,
            AbstractMapperBase.class, ConcreteMapperViaInheritance.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        assertNotNull(registry.domainToMapper().get(domainA));
        assertEquals(ConcreteMapperViaInheritance.class.getName(),
            registry.domainToMapper().get(domainA).name().toString());
    }

    @Test
    void resolvesMapperImplementingInterfaceDirectly() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class,
            MapperInterface.class, MapperViaInterface.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        assertNotNull(registry.domainToMapper().get(domainA));
        assertEquals(MapperViaInterface.class.getName(),
            registry.domainToMapper().get(domainA).name().toString());
    }

    @Test
    void handlesMultipleInterfaceInheritancePaths() throws Exception {
        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class,
            MapperInterfaceA.class, MapperInterfaceB.class,
            MultipleInterfaceMapper.class
        );

        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        assertNotNull(registry.domainToMapper().get(domainA));
    }

    @Test
    void validatesInferenceResultSuccessRequiresMapperClass() {
        ClassInfo mapperClass = null;
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(mapperClass, true, null));
        assertTrue(error.getMessage().contains("success=true requires non-null mapperClass"));
    }

    @Test
    void validatesInferenceResultSuccessRequiresNullError() throws Exception {
        Index index = indexOf(Mapper.class, MapperA.class, GrpcA.class, DomainA.class);
        ClassInfo mapperClass = index.getClassByName(DotName.createSimple(MapperA.class.getName()));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(mapperClass, true, "error"));
        assertTrue(error.getMessage().contains("success=true requires null errorMessage"));
    }

    @Test
    void validatesInferenceResultFailureRequiresNullMapperClass() throws Exception {
        Index index = indexOf(Mapper.class, MapperA.class, GrpcA.class, DomainA.class);
        ClassInfo mapperClass = index.getClassByName(DotName.createSimple(MapperA.class.getName()));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(mapperClass, false, "error"));
        assertTrue(error.getMessage().contains("success=false requires mapperClass==null"));
    }

    @Test
    void validatesInferenceResultFailureRequiresErrorMessage() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(null, false, null));
        assertTrue(error.getMessage().contains("success=false requires non-null, non-blank errorMessage"));
    }

    @Test
    void validatesInferenceResultFailureRequiresNonBlankErrorMessage() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperInferenceEngine.InferenceResult(null, false, "   "));
        assertTrue(error.getMessage().contains("success=false requires non-null, non-blank errorMessage"));
    }

    @Test
    void createsValidSuccessInferenceResult() throws Exception {
        Index index = indexOf(Mapper.class, MapperA.class, GrpcA.class, DomainA.class);
        ClassInfo mapperClass = index.getClassByName(DotName.createSimple(MapperA.class.getName()));

        MapperInferenceEngine.InferenceResult result =
            new MapperInferenceEngine.InferenceResult(mapperClass, true, null);

        assertTrue(result.success());
        assertNotNull(result.mapperClass());
        assertEquals(null, result.errorMessage());
    }

    @Test
    void createsValidFailureInferenceResult() {
        MapperInferenceEngine.InferenceResult result =
            new MapperInferenceEngine.InferenceResult(null, false, "Test error message");

        assertEquals(false, result.success());
        assertEquals(null, result.mapperClass());
        assertEquals("Test error message", result.errorMessage());
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

    static final class MapperADuplicate implements Mapper<DomainA, GrpcA2> {
        @Override
        public DomainA fromExternal(GrpcA2 grpc) {
            return null;
        }

        @Override
        public GrpcA2 toExternal(DomainA domain) {
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

    static abstract class AbstractMapperBase<D, E> implements Mapper<D, E> {
        // Abstract base class that implements Mapper
    }

    static final class ConcreteMapperViaInheritance extends AbstractMapperBase<DomainA, GrpcA> {
        @Override
        public DomainA fromExternal(GrpcA grpc) {
            return null;
        }

        @Override
        public GrpcA toExternal(DomainA domain) {
            return null;
        }
    }

    interface MapperInterface extends Mapper<DomainA, GrpcA> {
        // Interface extending Mapper
    }

    static final class MapperViaInterface implements MapperInterface {
        @Override
        public DomainA fromExternal(GrpcA grpc) {
            return null;
        }

        @Override
        public GrpcA toExternal(DomainA domain) {
            return null;
        }
    }

    interface MapperInterfaceA extends Mapper<DomainA, GrpcA> {
    }

    interface MapperInterfaceB {
        // Non-Mapper interface
    }

    static final class MultipleInterfaceMapper implements MapperInterfaceA, MapperInterfaceB {
        @Override
        public DomainA fromExternal(GrpcA grpc) {
            return null;
        }

        @Override
        public GrpcA toExternal(DomainA domain) {
            return null;
        }
    }
}