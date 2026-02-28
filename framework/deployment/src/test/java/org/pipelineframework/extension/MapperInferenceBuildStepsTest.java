package org.pipelineframework.extension;

import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.mapper.Mapper;

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
        assertNotNull(item.getMapperForPair(
                DotName.createSimple(DomainA.class.getName()),
                DotName.createSimple(GrpcA.class.getName())));
    }

    @Test
    void doesNotFailWhenStepDomainTypeIsMissingFromIndex() throws Exception {
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
        withContextClassLoader(root.toUri().toURL(),
            () -> steps.buildMapperRegistry(combinedIndex, item -> {
            }));
    }

    @Test
    void doesNotFailWhenStepDomainHasNoMapperInRegistry() throws Exception {
        String content = "com.example.ProcessA|"
            + DomainB.class.getName()
            + "|"
            + DomainB.class.getName()
            + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class, DomainB.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(),
            () -> steps.buildMapperRegistry(combinedIndex, item -> {
            }));
    }

    @Test
    void doesNotValidateStepDefinitionsAtBuildTime() throws Exception {
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
        withContextClassLoader(root.toUri().toURL(),
                () -> steps.buildMapperRegistry(combinedIndex, item -> {
                }));
    }

<<<<<<< HEAD
=======
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
    void handlesEmptyStepDefinitionsGracefully() throws Exception {
        Path root = writeStepDefinitions("");

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        // Should still produce a registry, just empty
        assertEquals(1, produced.size());
    }

    @Test
    void handlesMalformedStepDefinitionLineGracefully() throws Exception {
        // Malformed line with missing fields
        String content = "com.example.ProcessA|incomplete\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(),
            () -> steps.buildMapperRegistry(combinedIndex, item -> {}));

        // Should not throw exception, just skip malformed lines
    }

    @Test
    void handlesMultipleStepsWithSameDomain() throws Exception {
        String content = "com.example.ProcessA|"
                + DomainA.class.getName()
                + "|"
                + DomainA.class.getName()
                + "|ONE_TO_ONE\n"
                + "com.example.ProcessB|"
                + DomainA.class.getName()
                + "|"
                + DomainA.class.getName()
                + "|ONE_TO_ONE\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
        assertNotNull(produced.get(0));
    }

    @Test
    void handlesStepDefinitionWithTrailingWhitespace() throws Exception {
        String content = "com.example.ProcessA|"
                + DomainA.class.getName()
                + "|"
                + DomainA.class.getName()
                + "|ONE_TO_ONE   \n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
    }

    @Test
    void isMapLikeReturnsFalseForNonMapTypes() throws Exception {
        Index index = indexOf(DomainA.class);
        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        Method method = MapperInferenceBuildSteps.class.getDeclaredMethod("isMapLike", DotName.class, IndexView.class);
        method.setAccessible(true);

        boolean mapLike = (boolean) method.invoke(steps, DotName.createSimple(DomainA.class.getName()), index);
        assertTrue(!mapLike);
    }

    @Test
    void handlesStepDefinitionWithEmptyLines() throws Exception {
        String content = "\n"
                + "com.example.ProcessA|"
                + DomainA.class.getName()
                + "|"
                + DomainA.class.getName()
                + "|ONE_TO_ONE\n"
                + "\n"
                + "\n";
        Path root = writeStepDefinitions(content);

        Index index = indexOf(Mapper.class, GrpcA.class, DtoA.class, DomainA.class, MapperA.class);
        CombinedIndexBuildItem combinedIndex = new CombinedIndexBuildItem(index, index);

        List<MapperRegistryBuildItem> produced = new ArrayList<>();
        BuildProducer<MapperRegistryBuildItem> producer = produced::add;

        MapperInferenceBuildSteps steps = new MapperInferenceBuildSteps();
        withContextClassLoader(root.toUri().toURL(), () -> steps.buildMapperRegistry(combinedIndex, producer));

        assertEquals(1, produced.size());
    }

>>>>>>> 2be9e022 (📝 CodeRabbit Chat: Add unit tests for PR changes)
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
    static final class DomainB {}
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
}