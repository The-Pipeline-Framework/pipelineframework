package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;

class QueryPortabilityBoundaryTest {
    private static final String MUTINY_UNI = "io.smallrye.mutiny.Uni";

    @Test
    void queryConnectorAndCaptureStoreContractsDoNotExposeMutiny() {
        for (Class<?> contract : List.of(FrameworkQueryConnector.class, QueryCaptureStore.class)) {
            for (Method method : contract.getMethods()) {
                if (method.getDeclaringClass().equals(Object.class)) {
                    continue;
                }
                assertNotEquals(MUTINY_UNI, method.getReturnType().getName(),
                    contract.getSimpleName() + "." + method.getName() + " must use a neutral async type");
            }
        }
    }
}
