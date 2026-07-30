package org.pipelineframework.orchestrator.release;

import java.time.Duration;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineReleaseIdentityResolver;

/**
 * Activates the generated contract as the pinned release for an in-memory local runtime.
 *
 * <p>Hosted runtimes register release artifacts through their control plane. A direct local runtime has no
 * control-plane registration, but its generated contract is the deployed release artifact. Registering that
 * exact contract at startup lets durable payload resolution remain pinned rather than falling back to an
 * ambient active release.</p>
 */
@ApplicationScoped
public class LocalPipelineReleaseActivation {

    private static final Duration ACTIVATION_TIMEOUT = Duration.ofSeconds(10);

    @Inject
    PipelineReleaseRegistry releaseRegistry;

    @Inject
    PipelineReleaseIdentityResolver releaseIdentity;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    void activate(@Observes StartupEvent ignored) {
        if (!usesInMemoryRegistry()) {
            return;
        }
        PipelineContractDescriptor contract = releaseIdentity.contract();
        String pipelineId = releaseIdentity.pipelineId(orchestratorConfig);
        String contractVersion = releaseIdentity.contractVersion();
        String releaseVersion = releaseIdentity.releaseVersion(orchestratorConfig);
        if (!pipelineId.equals(contract.pipelineId()) || !contractVersion.equals(contract.contractVersion())) {
            throw new IllegalStateException(
                "Local in-memory release identity does not match generated contract: pipelineId="
                    + pipelineId + ", contractVersion=" + contractVersion);
        }

        long now = System.currentTimeMillis();
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            pipelineId,
            contractVersion,
            releaseVersion,
            java.util.List.of());
        PipelineReleaseRecord record = new PipelineReleaseRecord(
            "default",
            pipelineId,
            contractVersion,
            releaseVersion,
            PipelineReleaseStatus.ACTIVE,
            descriptor,
            "",
            "",
            "",
            0L,
            "",
            contract,
            now,
            now,
            now);
        releaseRegistry.register(record)
            .onItem().transformToUni(registered -> releaseRegistry.activate(
                registered.tenantId(), registered.pipelineId(), registered.releaseVersion(), now))
            .await().atMost(ACTIVATION_TIMEOUT)
            .orElseThrow(() -> new IllegalStateException(
                "Local in-memory release activation did not retain " + pipelineId + "@" + releaseVersion));
    }

    private boolean usesInMemoryRegistry() {
        if (orchestratorConfig == null
            || orchestratorConfig.releases() == null
            || orchestratorConfig.releases().registry() == null) {
            return true;
        }
        String provider = orchestratorConfig.releases().registry().provider();
        return provider == null || provider.isBlank() || "memory".equalsIgnoreCase(provider);
    }
}
