package org.pipelineframework.localcommandproof;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.command.CommandConnector;
import org.pipelineframework.command.CommandRequest;

@ApplicationScoped
public class ObservedOperationCommandConnector
    implements CommandConnector<ObservedOperationCommand, ObservedOperationResult> {
    static final String COMMAND = "observed-operation";

    @Inject
    FakeSerializedOperationManager manager;

    @Override
    public String command() {
        return COMMAND;
    }

    @Override
    public Uni<ObservedOperationResult> execute(CommandRequest<ObservedOperationCommand> request) {
        return Uni.createFrom()
            .item(() -> manager.executeBlocking(request.input(), request.config()))
            .runSubscriptionOn(manager.workerExecutor());
    }
}
