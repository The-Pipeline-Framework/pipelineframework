package org.pipelineframework.awaitable.fixture.service.pipeline;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.awaitable.AwaitStreamOneToOneStep;

/** Test-only stand-in for a generated v2 await client. */
public final class ProcessAwaitPaymentProviderAwaitClientStep
    implements AwaitStreamOneToOneStep<LegacyAwaitInput, LegacyAwaitOutput> {

    @Override
    public Multi<LegacyAwaitOutput> applyAwaitPerItem(Multi<LegacyAwaitInput> input) {
        return Multi.createFrom().empty();
    }
}

record LegacyAwaitInput(String reference) {
}

record LegacyAwaitOutput(String decision) {
}
