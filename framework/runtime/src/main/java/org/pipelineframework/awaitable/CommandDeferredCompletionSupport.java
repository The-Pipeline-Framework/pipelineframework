package org.pipelineframework.awaitable;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.CommandOutcomeException;
import org.pipelineframework.command.CommandRetryableEffectException;
import org.pipelineframework.connector.ConnectorCallbackContext;
import org.pipelineframework.connector.ProviderCallbackEndpointResolver;
import org.pipelineframework.connector.ProviderCallbackRequest;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

/** Orders durable completion registration around the existing Command execution path. */
@ApplicationScoped
public class CommandDeferredCompletionSupport {
    public static final String CALLBACK_PATH = "pipeline/callbacks/";
    @Inject AwaitCoordinator coordinator;
    @Inject AwaitResumeTokenService tokens;
    @Inject PipelineOrchestratorConfig config;
    @ConfigProperty(name = "pipeline.callback.allow-http", defaultValue = "false")
    boolean allowHttp;

    public <I, O> Uni<O> execute(AwaitCompletionDescriptor descriptor, I input,
        ProviderCallbackEndpointResolver endpointResolver, Function<ConnectorCallbackContext, Uni<?>> command) {
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(input, "input");
        Objects.requireNonNull(endpointResolver, "endpointResolver");
        Objects.requireNonNull(command, "command");
        if (config.mode() != OrchestratorMode.QUEUE_ASYNC) {
            return Uni.createFrom().failure(new IllegalStateException("Command callback requires QUEUE_ASYNC"));
        }
        PipelineExecutionContext execution = PipelineExecutionContextHolder.get()
            .orElseThrow(() -> new IllegalStateException("Command callback requires execution context"));
        AwaitExecutionContext context = Optional.ofNullable(AwaitExecutionContextHolder.get())
            .map(current -> new AwaitExecutionContext(current.tenantId(), current.executionId(),
                current.currentStepIndex(), current.continuationMode(), current.terminalOutputOwnership(),
                current.traceMetadata()))
            .orElseGet(() -> new AwaitExecutionContext(execution.tenantId(), execution.executionId(),
                execution.currentStepIndex()));
        return coordinator.registerCommandCompletion(descriptor, context, input)
            .chain(created -> {
                AwaitInteractionRecord record = created.record();
                if (record.status().terminal() || record.status() == AwaitInteractionStatus.DISPATCHED) {
                    return finish(new CommandCompletionSettlementResult(record,
                        record.status() == AwaitInteractionStatus.COMPLETED
                            && "dispatch".equals(record.transportMetadata().get("completionDelivery"))), context);
                }
                ConnectorCallbackContext callback;
                try {
                    String token = tokens.sign(record, record.createdAtEpochMs());
                    var selected = descriptor.callback().orElseThrow();
                    var policy = allowHttp ? ConnectorCallbackContext.UriPolicy.LOCAL_HTTP
                        : ConnectorCallbackContext.UriPolicy.HTTPS_ONLY;
                    var base = new ConnectorCallbackContext(selected.callback().id(), endpointResolver.resolve(
                        new ProviderCallbackRequest(record.tenantId(), record.interactionId(),
                            selected.operation(), selected.callback().id())), policy).callbackUri();
                    if (base.getRawQuery() != null) {
                        throw new IllegalArgumentException("callback base URI must not contain a query");
                    }
                    String prefix = base.toASCIIString();
                    callback = new ConnectorCallbackContext(selected.callback().id(), java.net.URI.create(
                        prefix + (prefix.endsWith("/") ? "" : "/") + CALLBACK_PATH + token), policy);
                } catch (RuntimeException failure) {
                    return Uni.createFrom().failure(new IllegalStateException("Unable to resolve Command callback endpoint"));
                }
                Uni<Optional<AwaitInteractionRecord>> claimed = record.status() == AwaitInteractionStatus.WAITING
                    ? coordinator.claimCommandDispatch(record) : Uni.createFrom().item(Optional.of(record));
                return claimed.chain(claim -> {
                    if (claim.isEmpty()) {
                        return suspended(record, context);
                    }
                    return invoke(execution, command, callback)
                        .onItem().transform(ignored -> new DispatchResult(CommandDispatchSettlement.SUCCEEDED, Optional.empty()))
                        .onFailure(failure -> !(failure instanceof org.pipelineframework.command.CommandInProgressException))
                        .recoverWithItem(failure -> new DispatchResult(classify(failure), Optional.of(failure)))
                        .chain(result -> coordinator.settleCommandDispatch(claim.orElseThrow(), result.settlement())
                            .chain(settled -> {
                                if (settled.record().status() == AwaitInteractionStatus.WAITING) {
                                    return Uni.createFrom().<O>failure(result.failure().orElseThrow());
                                }
                                return this.<O>finish(settled, context);
                            }))
                        .onFailure(org.pipelineframework.command.CommandInProgressException.class)
                        .recoverWithUni(ignored -> this.<O>suspended(claim.orElseThrow(), context));
                });
            });
    }

    private Uni<?> invoke(PipelineExecutionContext execution, Function<ConnectorCallbackContext, Uni<?>> command,
        ConnectorCallbackContext callback) {
        Optional<PipelineExecutionContext> previous = PipelineExecutionContextHolder.get();
        PipelineExecutionContextHolder.set(execution);
        try {
            return Objects.requireNonNull(command.apply(callback), "Command returned no asynchronous result");
        } catch (RuntimeException failure) {
            return Uni.createFrom().failure(failure);
        } finally {
            previous.ifPresentOrElse(PipelineExecutionContextHolder::set, PipelineExecutionContextHolder::clear);
        }
    }

    private CommandDispatchSettlement classify(Throwable failure) {
        if (failure instanceof CommandOutcomeException outcome && outcome.status() == CommandEffectStatus.AMBIGUOUS) {
            return CommandDispatchSettlement.AMBIGUOUS;
        }
        if (failure instanceof CommandOutcomeException outcome && outcome.status() == CommandEffectStatus.USER_ACTION_REQUIRED) {
            return CommandDispatchSettlement.USER_ACTION_REQUIRED;
        }
        if (CommandRetryableEffectException.find(failure).isPresent()
            || failure instanceof org.pipelineframework.command.CommandRetryableOutcomeException) {
            return CommandDispatchSettlement.RETRYABLE;
        }
        return CommandDispatchSettlement.TERMINAL;
    }

    @SuppressWarnings("unchecked")
    private <O> Uni<O> finish(CommandCompletionSettlementResult result, AwaitExecutionContext context) {
        AwaitInteractionRecord record = result.record();
        if (record.status().terminal() && record.status() != AwaitInteractionStatus.COMPLETED) {
            return coordinator.recordCompletion(record, System.currentTimeMillis())
                .chain(() -> coordinator.releaseAdmission(record))
                .chain(() -> Uni.createFrom().failure(new AwaitInteractionTerminalException(
                    "Command completion " + record.status() + ": " + record.transportMetadata().getOrDefault("completionFailure", "effect-failed"))));
        }
        if (result.completedByDispatch()) {
            return coordinator.recordCompletion(record, System.currentTimeMillis())
                .chain(() -> coordinator.releaseAdmission(record))
                .replaceWith(() -> (O) coordinator.resumePayload(record));
        }
        return suspended(record, context);
    }

    private <O> Uni<O> suspended(AwaitInteractionRecord record, AwaitExecutionContext context) {
        return Uni.createFrom().failure(new AwaitSuspendedException(context.tenantId(), context.executionId(),
            record.unitId(), context.currentStepIndex()));
    }

    private record DispatchResult(CommandDispatchSettlement settlement, Optional<Throwable> failure) { }
}
