package org.pipelineframework.orchestrator.release;

import java.time.Duration;
import java.util.Objects;

import io.smallrye.mutiny.Uni;
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
        activateForCurrentRelease("default", releaseIdentity.pipelineId(orchestratorConfig),
            releaseIdentity.contractVersion(), releaseIdentity.releaseVersion(orchestratorConfig))
            .await().atMost(ACTIVATION_TIMEOUT);
    }

    /**
     * Activates the locally generated release for the tenant submitting work to a direct in-memory runtime.
     *
     * <p>Release registry records are tenant-scoped. Startup can only establish the default tenant; queue
     * submission must establish the same generated release for an explicitly selected tenant before a durable
     * execution can pin it. Hosted registries retain their control-plane-owned lifecycle.</p>
     */
    public Uni<Void> activateForCurrentRelease(
        String tenantId,
        String pipelineId,
        String contractVersion,
        String releaseVersion
    ) {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(contractVersion, "contractVersion");
        Objects.requireNonNull(releaseVersion, "releaseVersion");
        if (!usesInMemoryRegistry() || !isCurrentRelease(pipelineId, contractVersion, releaseVersion)) {
            return Uni.createFrom().voidItem();
        }
        PipelineContractDescriptor contract = releaseIdentity.contract();
        long now = System.currentTimeMillis();
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            pipelineId,
            contractVersion,
            releaseVersion,
            java.util.List.of());
        PipelineReleaseRecord record = new PipelineReleaseRecord(
            tenantId,
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
        return releaseRegistry.register(record)
            .onItem().transformToUni(registered -> releaseRegistry.activate(
                registered.tenantId(), registered.pipelineId(), registered.releaseVersion(), now))
            .onItem().transformToUni(activated -> activated
                .map(ignored -> Uni.createFrom().voidItem())
                .orElseGet(() -> Uni.createFrom().failure(new IllegalStateException(
                    "Local in-memory release activation did not retain " + pipelineId + "@" + releaseVersion))));
    }

    private boolean isCurrentRelease(String pipelineId, String contractVersion, String releaseVersion) {
        PipelineContractDescriptor contract = releaseIdentity.contract();
        String currentPipelineId = releaseIdentity.pipelineId(orchestratorConfig);
        String currentContractVersion = releaseIdentity.contractVersion();
        String currentReleaseVersion = releaseIdentity.releaseVersion(orchestratorConfig);
        if (!currentPipelineId.equals(contract.pipelineId()) || !currentContractVersion.equals(contract.contractVersion())) {
            throw new IllegalStateException(
                "Local in-memory release identity does not match generated contract: pipelineId="
                    + currentPipelineId + ", contractVersion=" + currentContractVersion);
        }
        return currentPipelineId.equals(pipelineId)
            && currentContractVersion.equals(contractVersion)
            && currentReleaseVersion.equals(releaseVersion);
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
