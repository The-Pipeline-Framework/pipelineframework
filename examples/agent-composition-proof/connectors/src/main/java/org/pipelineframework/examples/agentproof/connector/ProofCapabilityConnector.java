package org.pipelineframework.examples.agentproof.connector;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.pipelineframework.connector.CommandConfirmation;
import org.pipelineframework.connector.CommandInvocation;
import org.pipelineframework.connector.CommandOperation;
import org.pipelineframework.connector.CommandOutcome;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.examples.agentproof.domain.LookupArguments;
import org.pipelineframework.examples.agentproof.domain.LookupResult;
import org.pipelineframework.examples.agentproof.domain.RecordArguments;
import org.pipelineframework.examples.agentproof.domain.RecordResult;

/** One native Query and one native Command exposed through a named binding. */
@ApplicationScoped
public class ProofCapabilityConnector implements ConnectorProvider<ProofCapabilityConnector.ProviderConfiguration> {
    private static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("proof.capabilities");

    @Inject
    ProofInvocationRecorder recorder;

    public ProofCapabilityConnector() {
    }

    @Override
    public ConnectorProviderId id() {
        return PROVIDER_ID;
    }

    @Override
    public ConnectorProviderVersion version() {
        return new ConnectorProviderVersion(1, 0);
    }

    @Override
    public Optional<ConnectorConfigSchema<ProviderConfiguration>> configurationSchema() {
        return Optional.of(ConnectorConfigSchema.record(
            ProviderConfiguration.class, "proof.capabilities.provider", 1));
    }

    @Override
    public Collection<? extends ConnectorOperation> operations() {
        return List.of(new LookupOperation(this), new RecordOperation(this));
    }

    public record ProviderConfiguration(String fixture) {
    }

    public record RecordConfiguration(String label) {
    }

    private static final class LookupOperation
        implements QueryOperation<LookupArguments, org.pipelineframework.connector.ConnectorConfigurationDocument, LookupResult> {
        private final ProofCapabilityConnector provider;

        private LookupOperation(ProofCapabilityConnector provider) {
            this.provider = provider;
        }

        @Override
        public String id() {
            return "evidence.lookup";
        }

        @Override
        public CompletionStage<QueryOutcome<LookupResult>> query(
            QueryInvocation<LookupArguments, org.pipelineframework.connector.ConnectorConfigurationDocument, LookupResult> invocation
        ) {
            provider.requireRecorder().recordQuery(invocation.executionContext());
            return CompletableFuture.completedFuture(new QueryOutcome.NotFound<>("proof-subject-missing"));
        }
    }

    private static final class RecordOperation
        implements CommandOperation<RecordArguments, RecordConfiguration, RecordResult> {
        private static final ConnectorConfigSchema<RecordConfiguration> CONFIGURATION = ConnectorConfigSchema.record(
            RecordConfiguration.class, "proof.capabilities.evidence.record", 1);
        private final ProofCapabilityConnector provider;

        private RecordOperation(ProofCapabilityConnector provider) {
            this.provider = provider;
        }

        @Override
        public String id() {
            return "evidence.record";
        }

        @Override
        public Optional<ConnectorConfigSchema<RecordConfiguration>> configurationSchema() {
            return Optional.of(CONFIGURATION);
        }

        @Override
        public CompletionStage<CommandOutcome<RecordResult>> dispatch(
            CommandInvocation<RecordArguments, RecordConfiguration> invocation
        ) {
            provider.requireRecorder().recordCommand(invocation.executionContext());
            RecordResult result = new RecordResult(
                invocation.configuration().label() + ":" + invocation.input().action());
            return CompletableFuture.completedFuture(
                new CommandOutcome.Succeeded<>(result, CommandConfirmation.none(), List.of()));
        }
    }

    private ProofInvocationRecorder requireRecorder() {
        return java.util.Objects.requireNonNull(recorder, "proof invocation recorder must be injected");
    }
}
