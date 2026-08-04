package org.pipelineframework.awaitable.v3fixture.domain;

public sealed interface AwaitOutput permits AwaitOutput.Approved {
    record Approved(String value) implements AwaitOutput {
    }
}
