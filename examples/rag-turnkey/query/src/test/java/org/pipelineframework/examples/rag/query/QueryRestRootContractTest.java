package org.pipelineframework.examples.rag.query;

import java.lang.reflect.Method;
import java.util.Arrays;

import jakarta.ws.rs.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryRestRootContractTest {

    @Test
    void exposesThePublicPipelineRunEndpoint() throws ClassNotFoundException {
        Class<?> resource = Class.forName(
            "org.pipelineframework.examples.rag.query.orchestrator.service.PipelineRunResource");

        assertEquals("/pipeline", resource.getAnnotation(Path.class).value());

        Method run = Arrays.stream(resource.getDeclaredMethods())
            .filter(method -> method.getName().equals("run"))
            .findFirst()
            .orElseThrow();
        assertEquals("/run", run.getAnnotation(Path.class).value());
    }
}
