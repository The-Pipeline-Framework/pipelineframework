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
}
