package org.pipelineframework.extension;

import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
<<<<<<< HEAD
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.Test;
import org.pipelineframework.extension.MapperInferenceEngine.MapperPairKey;
import org.pipelineframework.mapper.Mapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MapperRegistryBuildItemTest {

    @Test
    void constructsValidBuildItem() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        assertEquals(1, buildItem.size());
        assertNotNull(buildItem.getAllPairs());
        assertNotNull(buildItem.getAllMappers());
    }

    @Test
    void failsWhenPairToMapperIsNull() {
        Map<ClassInfo, MapperPairKey> mapperToPair = new HashMap<>();
        assertThrows(NullPointerException.class,
            () -> new MapperRegistryBuildItem(null, mapperToPair));
    }

    @Test
    void failsWhenMapperToPairIsNull() {
        Map<MapperPairKey, ClassInfo> pairToMapper = new HashMap<>();
        assertThrows(NullPointerException.class,
            () -> new MapperRegistryBuildItem(pairToMapper, null));
    }

    @Test
    void failsWhenMapSizesDoNotMatch() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        Map<MapperPairKey, ClassInfo> pairToMapper = new HashMap<>(registry.pairToMapper());
        Map<ClassInfo, MapperPairKey> mapperToPair = new HashMap<>();

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperRegistryBuildItem(pairToMapper, mapperToPair));
        assertTrue(error.getMessage().contains("Inconsistent mapper registry"));
        assertTrue(error.getMessage().contains("pairToMapper has 1 entries"));
    }

    @Test
    void failsWhenBidirectionalMappingIsInconsistent() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, DomainB.class, GrpcA.class, GrpcB.class, MapperA.class, MapperB.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        Map<MapperPairKey, ClassInfo> pairToMapper = new HashMap<>(registry.pairToMapper());
        Map<ClassInfo, MapperPairKey> mapperToPair = new HashMap<>(registry.mapperToPair());

        var pairKeys = pairToMapper.keySet().iterator();
        MapperPairKey firstKey = pairKeys.next();
        MapperPairKey secondKey = pairKeys.next();
        ClassInfo firstMapper = pairToMapper.get(firstKey);

        mapperToPair.put(firstMapper, secondKey);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
            () -> new MapperRegistryBuildItem(pairToMapper, mapperToPair));
        assertTrue(error.getMessage().contains("Inconsistent mapper registry"));
    }

    @Test
    void getMapperForPairReturnsCorrectMapper() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        DotName grpcA = DotName.createSimple(GrpcA.class.getName());

        ClassInfo mapper = buildItem.getMapperForPair(domainA, grpcA);
        assertNotNull(mapper);
        assertEquals(MapperA.class.getName(), mapper.name().toString());
    }

    @Test
    void getMapperForPairReturnsNullWhenNotFound() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        DotName domainB = DotName.createSimple(DomainB.class.getName());
        DotName grpcA = DotName.createSimple(GrpcA.class.getName());

        ClassInfo mapper = buildItem.getMapperForPair(domainB, grpcA);
        assertNull(mapper);
    }

    @Test
    void hasMapperForPairReturnsTrueWhenFound() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        DotName domainA = DotName.createSimple(DomainA.class.getName());
        DotName grpcA = DotName.createSimple(GrpcA.class.getName());

        assertTrue(buildItem.hasMapperForPair(domainA, grpcA));
    }

    @Test
    void hasMapperForPairReturnsFalseWhenNotFound() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        DotName domainB = DotName.createSimple(DomainB.class.getName());
        DotName grpcA = DotName.createSimple(GrpcA.class.getName());

        assertFalse(buildItem.hasMapperForPair(domainB, grpcA));
    }

    @Test
    void getPairForMapperReturnsCorrectPair() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        ClassInfo mapperClass = buildItem.getAllMappers().iterator().next();
        MapperPairKey pair = buildItem.getPairForMapper(mapperClass);

        assertNotNull(pair);
        assertEquals(DomainA.class.getName(), pair.domainType().toString());
        assertEquals(GrpcA.class.getName(), pair.externalType().toString());
    }

    @Test
    void getPairForMapperReturnsNullWhenNotFound() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class, MapperB.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        Index indexB = indexOf(Mapper.class, DomainB.class, GrpcB.class, MapperB.class);
        ClassInfo unknownMapper = indexB.getClassByName(DotName.createSimple(MapperB.class.getName()));

        MapperPairKey pair = buildItem.getPairForMapper(unknownMapper);
        assertNull(pair);
    }

    @Test
    void getAllPairsReturnsUnmodifiableSet() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        var pairs = buildItem.getAllPairs();
        assertNotNull(pairs);
        assertEquals(1, pairs.size());
        assertThrows(UnsupportedOperationException.class,
            () -> pairs.add(new MapperPairKey(
                DotName.createSimple("test.Domain"),
                DotName.createSimple("test.External"))));
    }

    @Test
    void getAllMappersReturnsUnmodifiableCollection() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        var mappers = buildItem.getAllMappers();
        assertNotNull(mappers);
        assertEquals(1, mappers.size());
    }

    @Test
    void toStringReturnsInformativeString() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        String str = buildItem.toString();
        assertTrue(str.contains("MapperRegistryBuildItem"));
        assertTrue(str.contains("size=1"));
    }

    @Test
    void getPairToMapperMapReturnsUnmodifiableMap() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        var map = buildItem.getPairToMapperMap();
        assertNotNull(map);
        assertThrows(UnsupportedOperationException.class,
            () -> map.put(new MapperPairKey(
                DotName.createSimple("test.Domain"),
                DotName.createSimple("test.External")), null));
    }

    @Test
    void getMapperToPairMapReturnsUnmodifiableMap() throws Exception {
        Index index = indexOf(Mapper.class, DomainA.class, GrpcA.class, MapperA.class);
        MapperInferenceEngine engine = new MapperInferenceEngine(index);
        MapperInferenceEngine.MapperRegistry registry = engine.buildRegistry();

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(
            registry.pairToMapper(),
            registry.mapperToPair());

        var map = buildItem.getMapperToPairMap();
        assertNotNull(map);
        assertThrows(UnsupportedOperationException.class,
            () -> map.put(null, new MapperPairKey(
                DotName.createSimple("test.Domain"),
                DotName.createSimple("test.External"))));
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

    static final class DomainA {}
    static final class DomainB {}
    static final class GrpcA {}
    static final class GrpcB {}

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
=======
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MapperRegistryBuildItem.
 */
class MapperRegistryBuildItemTest {

    @Test
    void constructorAcceptsValidRegistry() {
        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);

        assertNotNull(buildItem);
    }

    @Test
    void getMapperForPairReturnsMapperWhenPresent() {
        DotName domainType = DotName.createSimple("com.example.Domain");
        DotName externalType = DotName.createSimple("com.example.External");
        ClassInfo mockMapper = mock(ClassInfo.class);

        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        when(mockRegistry.pairToMapper()).thenReturn(
            java.util.Map.of(
                new MapperInferenceEngine.MapperPairKey(domainType, externalType),
                mockMapper
            )
        );

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);
        ClassInfo result = buildItem.getMapperForPair(domainType, externalType);

        assertEquals(mockMapper, result);
    }

    @Test
    void getMapperForPairReturnsNullWhenNotPresent() {
        DotName domainType = DotName.createSimple("com.example.Domain");
        DotName externalType = DotName.createSimple("com.example.External");

        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        when(mockRegistry.pairToMapper()).thenReturn(java.util.Map.of());

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);
        ClassInfo result = buildItem.getMapperForPair(domainType, externalType);

        assertNull(result);
    }

    @Test
    void getMapperForPairHandlesNullParameters() {
        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        when(mockRegistry.pairToMapper()).thenReturn(java.util.Map.of());

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);

        // Should handle null parameters gracefully
        assertNull(buildItem.getMapperForPair(null, null));
        assertNull(buildItem.getMapperForPair(
            DotName.createSimple("com.example.Domain"),
            null
        ));
        assertNull(buildItem.getMapperForPair(
            null,
            DotName.createSimple("com.example.External")
        ));
    }

    @Test
    void buildItemSupportsMultipleMappers() {
        DotName domain1 = DotName.createSimple("com.example.Domain1");
        DotName external1 = DotName.createSimple("com.example.External1");
        DotName domain2 = DotName.createSimple("com.example.Domain2");
        DotName external2 = DotName.createSimple("com.example.External2");

        ClassInfo mapper1 = mock(ClassInfo.class);
        ClassInfo mapper2 = mock(ClassInfo.class);

        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        when(mockRegistry.pairToMapper()).thenReturn(
            java.util.Map.of(
                new MapperInferenceEngine.MapperPairKey(domain1, external1), mapper1,
                new MapperInferenceEngine.MapperPairKey(domain2, external2), mapper2
            )
        );

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);

        assertEquals(mapper1, buildItem.getMapperForPair(domain1, external1));
        assertEquals(mapper2, buildItem.getMapperForPair(domain2, external2));
    }

    @Test
    void buildItemSupportsEmptyRegistry() {
        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        when(mockRegistry.pairToMapper()).thenReturn(java.util.Map.of());

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);

        assertNotNull(buildItem);
        assertNull(buildItem.getMapperForPair(
            DotName.createSimple("com.example.Domain"),
            DotName.createSimple("com.example.External")
        ));
    }

    @Test
    void getMapperForPairUsesCorrectKeyOrdering() {
        DotName domainType = DotName.createSimple("com.example.Domain");
        DotName externalType = DotName.createSimple("com.example.External");
        ClassInfo mockMapper = mock(ClassInfo.class);

        MapperInferenceEngine.MapperRegistry mockRegistry = mock(MapperInferenceEngine.MapperRegistry.class);
        when(mockRegistry.pairToMapper()).thenReturn(
            java.util.Map.of(
                new MapperInferenceEngine.MapperPairKey(domainType, externalType),
                mockMapper
            )
        );

        MapperRegistryBuildItem buildItem = new MapperRegistryBuildItem(mockRegistry);

        // Should find with correct order
        assertEquals(mockMapper, buildItem.getMapperForPair(domainType, externalType));

        // Should not find with reversed order (MapperPairKey is order-sensitive)
        assertNull(buildItem.getMapperForPair(externalType, domainType));
>>>>>>> 2be9e022 (📝 CodeRabbit Chat: Add unit tests for PR changes)
    }
}