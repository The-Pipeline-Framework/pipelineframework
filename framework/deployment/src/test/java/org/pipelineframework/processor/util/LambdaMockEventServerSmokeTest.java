package org.pipelineframework.processor.util;

import java.io.IOException;

import io.quarkus.amazon.lambda.runtime.MockHttpEventServer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LambdaMockEventServerSmokeTest {

    @Test
    void startsAndStopsOnEphemeralPort() throws IOException {
        MockHttpEventServer server = new MockHttpEventServer();
        try {
            server.start(0);
            int port = server.getPort();
            assertTrue(port > 0, "Expected mock lambda event server to bind to an ephemeral port");
        } finally {
            server.close();
        }
    }
}
