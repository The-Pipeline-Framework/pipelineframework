package org.pipelineframework.orchestrator;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LoggingDeadLetterPublisherTest {

    @Test
    void exposesExpectedProviderContract() {
        LoggingDeadLetterPublisher publisher = new LoggingDeadLetterPublisher();

        assertEquals("log", publisher.providerName());
        assertEquals(-100, publisher.priority());
    }
}
