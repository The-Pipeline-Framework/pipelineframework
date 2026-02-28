package org.pipelineframework.extension;

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.jboss.jandex.IndexView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.mapper.Mapper;

import java.lang.reflect.Method;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapperInferenceBuildStepsTest {

    @TempDir
    Path tempDir;

    @Test
    void producesMapperRegistryWhenStepDefinitionsAreValid() throws Exception {
        String content = "com.example.ProcessA|"
            + DomainA.class.getName()
            + "|"
            + DomainA.class.getName()
            + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class, MapperA.class
        );
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
        MapperRegistryBuildItem item = produced.get(0);
        assertNotNull(item.getMapperForDomain(DotName.createSimple(DomainA.class.getName())));
    }

    @Test
    void failsWhenStepDomainTypeIsMissingFromIndex() throws Exception {
        String missingDomain = "com.example.MissingDomain";
        String content = "com.example.ProcessA|"
            + missingDomain
            + "|"
            + missingDomain
            + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        IllegalStateException error = withContextClassLoader(root.toUri().toURL(),
            () -> assertThrows(IllegalStateException.class,
                () -> steps.buildMapperRegistry(combinedIndex, item -> {
                })));

        assertTrue(error.getMessage().contains("not in the Jandex index"));
    }

    @Test
    void failsWhenStepDomainHasNoMapperInRegistry() throws Exception {
        String content = "com.example.ProcessA|"
            + DomainB.class.getName()
            + "|"
            + DomainB.class.getName()
            + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class, DomainB.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        IllegalStateException error = withContextClassLoader(root.toUri().toURL(),
            () -> assertThrows(IllegalStateException.class,
                () -> steps.buildMapperRegistry(combinedIndex, item -> {
                })));

        assertTrue(error.getMessage().contains("has no outbound mapper"));
    }

    @Test
    void failsWhenDuplicateStepDefinitionsConflict() throws Exception {
        String content = "com.example.ProcessA|"
                + DomainA.class.getName()
                + "|"
                + DomainA.class.getName()
                + "|ONE_TO_ONE\n"
                + "com.example.ProcessA|"
                + DomainB.class.getName()
                + "|"
                + DomainB.class.getName()
                + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class, DomainB.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        IllegalStateException error = withContextClassLoader(root.toUri().toURL(),
                () -> assertThrows(IllegalStateException.class,
                        () -> steps.buildMapperRegistry(combinedIndex, item -> {
                        })));

        assertTrue(error.getMessage().contains("Conflicting step definitions for stepName 'com.example.ProcessA'"));
    }

    @Test
    void isMapLikeDetectsIndirectMapInterfaces() throws Exception {
        Index index = indexOf(IndirectMap.class, BaseMap.class);
        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        Method method = MapperInferenceBuildSteps.class.getDeclaredMethod("isMapLike", DotName.class, IndexView.class);
        method.setAccessible(true);

        boolean mapLike = (boolean) method.invoke(steps, DotName.createSimple(IndirectMap.class.getName()), index);
        assertTrue(mapLike);
    }

    @Test
    void allowsEmptyStepDefinitionFile() throws Exception {
        String content = "# This file is empty except for comments\n\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
        // Registry should still be built even with empty step definitions
        assertNotNull(produced.get(0));
    }

    @Test
    void failsOnMalformedStepDefinitionMissingFields() throws Exception {
        String content = "com.example.ProcessA|" + DomainA.class.getName() + "\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        IllegalStateException error = withContextClassLoader(root.toUri().toURL(),
            () -> assertThrows(IllegalStateException.class,
                () -> steps.buildMapperRegistry(combinedIndex, item -> {})));

        assertTrue(error.getMessage().contains("Malformed step definition") ||
                   error.getMessage().contains("Expected format"));
    }

    @Test
    void ignoresCommentsAndBlankLinesInStepDefinitions() throws Exception {
        String content = "# Comment line\n" +
            "\n" +
            "   \n" +
            "com.example.ProcessA|" + DomainA.class.getName() + "|" + DomainA.class.getName() + "|ONE_TO_ONE\n" +
            "# Another comment\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
        assertNotNull(produced.get(0).getMapperForDomain(DotName.createSimple(DomainA.class.getName())));
    }

    @Test
    void allowsEmptyDomainFieldsInStepDefinitions() throws Exception {
        String content = "com.example.ProcessSource|||ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        // Should succeed - steps with no domain types are valid (e.g., source/sink steps)
        assertEquals(1, produced.size());
    }

    @Test
    void deduplicatesIdenticalStepDefinitions() throws Exception {
        String content = "com.example.ProcessA|" + DomainA.class.getName() + "|" + DomainA.class.getName() + "|ONE_TO_ONE\n" +
                        "com.example.ProcessA|" + DomainA.class.getName() + "|" + DomainA.class.getName() + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        // Should deduplicate identical definitions
        assertEquals(1, produced.size());
        assertNotNull(produced.get(0).getMapperForDomain(DotName.createSimple(DomainA.class.getName())));
    }

    @Test
    void failsWhenOutputDomainTypeIsMissingFromIndex() throws Exception {
        String content = "com.example.ProcessA|" + DomainA.class.getName() + "|com.example.MissingOutput|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        IllegalStateException error = withContextClassLoader(root.toUri().toURL(),
            () -> assertThrows(IllegalStateException.class,
                () -> steps.buildMapperRegistry(combinedIndex, item -> {})));

        assertTrue(error.getMessage().contains("not in the Jandex index"));
    }

    @Test
    void failsWhenOutputDomainHasNoMapper() throws Exception {
        String content = "com.example.ProcessA|" + DomainA.class.getName() + "|" + DomainB.class.getName() + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class, DomainB.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        IllegalStateException error = withContextClassLoader(root.toUri().toURL(),
            () -> assertThrows(IllegalStateException.class,
                () -> steps.buildMapperRegistry(combinedIndex, item -> {})));

        assertTrue(error.getMessage().contains("has no inbound mapper") ||
                   error.getMessage().contains("has no outbound mapper"));
    }

    @Test
    void handlesMultipleValidStepsWithDifferentDomains() throws Exception {
        String content = "com.example.ProcessA|" + DomainA.class.getName() + "|" + DomainA.class.getName() + "|ONE_TO_ONE\n" +
                        "com.example.ProcessB|" + DomainB.class.getName() + "|" + DomainB.class.getName() + "|ONE_TO_MANY\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(
            Mapper.class,
            GrpcA.class, DtoA.class, DomainA.class, MapperA.class,
            GrpcB.class, DtoB.class, DomainB.class, MapperB.class
        );
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
        MapperRegistryBuildItem item = produced.get(0);
        assertNotNull(item.getMapperForDomain(DotName.createSimple(DomainA.class.getName())));
        assertNotNull(item.getMapperForDomain(DotName.createSimple(DomainB.class.getName())));
    }

    private Path writeStepDefinitions(String content) throws IOException {
        Path metaInf = tempDir.resolve("META-INF").resolve("pipeline");
        Files.createDirectories(metaInf);
        Path file = metaInf.resolve("step-definitions.txt");
        Files.writeString(file, content);
        return tempDir;
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

    private static <T> T withContextClassLoader(URL url, ThrowingSupplier<T> action) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[]{url}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            return action.get();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static void withContextClassLoader(URL url, ThrowingRunnable action) throws Exception {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[]{url}, previous)) {
            Thread.currentThread().setContextClassLoader(loader);
            action.run();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    static final class GrpcA {}
    static final class DtoA {}
    static final class DomainA {}
    static final class GrpcB {}
    static final class DtoB {}
    static final class DomainB {}
    interface BaseMap<K, V> extends java.util.Map<K, V> {}
    interface IndirectMap<K, V> extends BaseMap<K, V> {}

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
}