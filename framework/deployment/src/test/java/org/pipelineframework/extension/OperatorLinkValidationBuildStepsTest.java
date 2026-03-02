package org.pipelineframework.extension;

import jakarta.enterprise.inject.spi.DeploymentException;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.Indexer;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.ParameterizedType;
import org.jboss.jandex.Type;
import org.junit.jupiter.api.Test;
import org.pipelineframework.extension.MapperInferenceEngine.MapperPairKey;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class OperatorLinkValidationBuildStepsTest {

    private static final DotName UNI = DotName.createSimple("io.smallrye.mutiny.Uni");
    private static final DotName MULTI = DotName.createSimple("io.smallrye.mutiny.Multi");

    private final OperatorLinkValidationBuildSteps buildSteps = new OperatorLinkValidationBuildSteps();

    @Test
    void passesForUniToUniWithAssignableTypes() throws Exception {
        Index index = indexOf(Source.class, Sink.class, MapperOne.class, MapperTwo.class);
        OperatorBuildItem source = operator(index, Source.class, "Source", "java.lang.String", uniType(String.class));
        OperatorBuildItem sink = operator(index, Sink.class, "Sink", "java.lang.String", uniType(String.class));

        assertDoesNotThrow(() -> buildSteps.validateOperatorLinks(
                List.of(source, sink),
                emptyRegistry(),
                index));
    }

    @Test
    void failsForMultiToUniCardinality() throws Exception {
        Index index = indexOf(Source.class, Sink.class, MapperOne.class, MapperTwo.class);
        OperatorBuildItem source = operator(index, Source.class, "Source", "java.lang.String", multiType(String.class));
        OperatorBuildItem sink = operator(index, Sink.class, "Sink", "java.lang.String", uniType(String.class));

        try {
            buildSteps.validateOperatorLinks(List.of(source, sink), emptyRegistry(), index);
            fail("Expected DeploymentException");
        } catch (DeploymentException e) {
            assertTrue(e.getMessage().contains("Step 'Source' produces"));
            assertTrue(e.getMessage().contains("step 'Sink' expects"));
        }
    }

    @Test
    void failsWhenNoMapperExistsForIncompatibleAdjacentTypes() throws Exception {
        Index index = indexOf(Source.class, Sink.class, MapperOne.class, MapperTwo.class);
        OperatorBuildItem source = operator(index, Source.class, "Source", "java.lang.String", uniType(String.class));
        OperatorBuildItem sink = operator(index, Sink.class, "Sink", "java.lang.Integer", uniType(Integer.class));

        try {
            buildSteps.validateOperatorLinks(List.of(source, sink), emptyRegistry(), index);
            fail("Expected DeploymentException");
        } catch (DeploymentException e) {
            assertTrue(e.getMessage().contains("Step 'Source' produces"));
            assertTrue(e.getMessage().contains("step 'Sink' expects"));
            assertTrue(e.getMessage().contains("java.lang.String"));
            assertTrue(e.getMessage().contains("java.lang.Integer"));
        }
    }

    @Test
    void passesWhenSingleMapperExistsForProducedDomain() throws Exception {
        Index index = indexOf(Source.class, Sink.class, MapperOne.class, MapperTwo.class);
        OperatorBuildItem source = operator(index, Source.class, "Source", "java.lang.String", uniType(String.class));
        OperatorBuildItem sink = operator(index, Sink.class, "Sink", "java.lang.Integer", uniType(Integer.class));

        ClassInfo mapperOne = classInfo(index, MapperOne.class);
        MapperRegistryBuildItem registry = registry(Map.of(
                new MapperPairKey(
                        DotName.createSimple(String.class.getName()),
                        DotName.createSimple(Long.class.getName())),
                mapperOne));

        assertDoesNotThrow(() -> buildSteps.validateOperatorLinks(List.of(source, sink), registry, index));
    }

    @Test
    void failsWhenMultipleMappersExistForProducedDomain() throws Exception {
        Index index = indexOf(Source.class, Sink.class, MapperOne.class, MapperTwo.class);
        OperatorBuildItem source = operator(index, Source.class, "Source", "java.lang.String", uniType(String.class));
        OperatorBuildItem sink = operator(index, Sink.class, "Sink", "java.lang.Integer", uniType(Integer.class));

        ClassInfo mapperOne = classInfo(index, MapperOne.class);
        ClassInfo mapperTwo = classInfo(index, MapperTwo.class);
        MapperRegistryBuildItem registry = registry(Map.of(
                new MapperPairKey(
                        DotName.createSimple(String.class.getName()),
                        DotName.createSimple(Long.class.getName())),
                mapperOne,
                new MapperPairKey(
                        DotName.createSimple(String.class.getName()),
                        DotName.createSimple(Double.class.getName())),
                mapperTwo));

        try {
            buildSteps.validateOperatorLinks(List.of(source, sink), registry, index);
            fail("Expected DeploymentException");
        } catch (DeploymentException e) {
            assertTrue(e.getMessage().contains("multiple mappers found"));
            assertTrue(e.getMessage().contains(MapperOne.class.getName()));
            assertTrue(e.getMessage().contains(MapperTwo.class.getName()));
        }
    }

    private OperatorBuildItem operator(
            Index index,
            Class<?> ownerClass,
            String stepName,
            String inputTypeName,
            Type normalizedReturnType) {
        ClassInfo ownerInfo = classInfo(index, ownerClass);
        MethodInfo method = ownerInfo.methods().stream()
                .filter(m -> "map".equals(m.name()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Expected map method on " + ownerInfo.name()));
        return new OperatorBuildItem(
                new PipelineConfigBuildItem.StepConfig(stepName, "com.example." + stepName + "::map"),
                ownerInfo,
                method,
                Type.create(DotName.createSimple(inputTypeName), Type.Kind.CLASS),
                normalizedReturnType,
                OperatorCategory.REACTIVE);
    }

    private MapperRegistryBuildItem emptyRegistry() {
        return registry(Map.of());
    }

    private MapperRegistryBuildItem registry(Map<MapperPairKey, ClassInfo> pairToMapper) {
        Map<ClassInfo, MapperPairKey> mapperToPair = new HashMap<>();
        for (Map.Entry<MapperPairKey, ClassInfo> entry : pairToMapper.entrySet()) {
            mapperToPair.put(entry.getValue(), entry.getKey());
        }
        return new MapperRegistryBuildItem(pairToMapper, mapperToPair);
    }

    private static Type uniType(Class<?> argument) {
        return ParameterizedType.create(UNI, new Type[]{Type.create(DotName.createSimple(argument.getName()), Type.Kind.CLASS)}, null);
    }

    private static Type multiType(Class<?> argument) {
        return ParameterizedType.create(MULTI, new Type[]{Type.create(DotName.createSimple(argument.getName()), Type.Kind.CLASS)}, null);
    }

    private static ClassInfo classInfo(Index index, Class<?> type) {
        ClassInfo info = index.getClassByName(DotName.createSimple(type.getName()));
        if (info == null) {
            throw new IllegalStateException("Missing class in index: " + type.getName());
        }
        return info;
    }

    private static Index indexOf(Class<?>... classesToIndex) throws IOException {
        Indexer indexer = new Indexer();
        for (Class<?> c : classesToIndex) {
            String resource = c.getName().replace('.', '/') + ".class";
            try (InputStream in = c.getClassLoader().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IOException("Could not locate class resource: " + resource);
                }
                indexer.index(in);
            }
        }
        return indexer.complete();
    }

    static final class Source {
        public String map(String input) {
            return input;
        }
    }

    static final class Sink {
        public String map(String input) {
            return input;
        }
    }

    static final class MapperOne {
    }

    static final class MapperTwo {
    }
}
