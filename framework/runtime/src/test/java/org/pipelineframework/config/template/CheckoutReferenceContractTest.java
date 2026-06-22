package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.composition.PipelineCompositionHandoff;
import org.pipelineframework.config.composition.PipelineCompositionIr;
import org.pipelineframework.config.composition.PipelineCompositionConfigLoader;

import static org.junit.jupiter.api.Assertions.*;

class CheckoutReferenceContractTest {

    private static final String CHECKOUT_COMPOSITION =
        "examples/checkout/config/canonical/pipeline-composition.yaml";

    @Test
    void canonicalCheckoutCompositionResolvesTypedHandoffs() {
        PipelineCompositionIr ir = new PipelineCompositionConfigLoader()
            .loadIr(resolveFromWorkspaceRoot(CHECKOUT_COMPOSITION));

        assertEquals("tpfgo-checkout", ir.config().name());
        assertEquals(8, ir.nodes().size());
        assertEquals(7, ir.handoffs().size());
        assertEquals(List.of("checkout"), ir.entrypointPipelineIds());
        assertEquals(List.of("tpfgo.compensation.terminal-state.v1"), ir.terminalPublications());
        assertTrue(ir.nodes().stream().allMatch(node -> "GRPC".equals(node.config().transport())),
            "Canonical checkout pipelines should use GRPC transport.");
        assertEquals(
            List.of(
                "checkout->consumer-validation:tpfgo.checkout.order-pending.v1",
                "consumer-validation->restaurant-acceptance:tpfgo.consumer.order-approved.v1",
                "restaurant-acceptance->kitchen-preparation:tpfgo.restaurant.order-accepted.v1",
                "kitchen-preparation->dispatch:tpfgo.kitchen.order-ready.v1",
                "dispatch->delivery-execution:tpfgo.dispatch.delivery-assigned.v1",
                "delivery-execution->payment-capture:tpfgo.delivery.order-delivered.v1",
                "payment-capture->compensation-failure:tpfgo.payment.capture-result.v1"),
            ir.handoffs().stream().map(this::describe).toList());
        assertEquals("MANY_TO_ONE",
            ir.nodes().stream()
                .filter(node -> "kitchen-preparation".equals(node.id()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected kitchen-preparation node in checkout composition"))
                .terminalStep()
                .cardinality());
    }

    private String describe(PipelineCompositionHandoff handoff) {
        return handoff.producerPipelineId() + "->" + handoff.consumerPipelineId() + ":" + handoff.publication();
    }

    private Path resolveFromWorkspaceRoot(String relativePath) {
        String workspaceRoot = System.getProperty("workspace.root");
        if (workspaceRoot == null || workspaceRoot.isBlank()) {
            workspaceRoot = System.getenv("WORKSPACE_ROOT");
        }
        if (workspaceRoot != null && !workspaceRoot.isBlank()) {
            Path explicitRoot = Path.of(workspaceRoot).normalize();
            Path explicitCandidate = explicitRoot.resolve(relativePath).normalize();
            if (!Files.exists(explicitCandidate)) {
                throw new AssertionError("workspace.root/WORKSPACE_ROOT is set but file was not found: " + explicitCandidate);
            }
            if (Files.exists(explicitCandidate)) {
                return explicitCandidate;
            }
        }

        var classpathResource = CheckoutReferenceContractTest.class.getClassLoader().getResource(relativePath);
        if (classpathResource != null && "file".equalsIgnoreCase(classpathResource.getProtocol())) {
            try {
                return Path.of(classpathResource.toURI()).normalize();
            } catch (java.net.URISyntaxException e) {
                throw new AssertionError("Could not convert classpath resource URI: " + classpathResource, e);
            }
        }

        Path userDir = Path.of(System.getProperty("user.dir")).normalize();
        List<Path> candidates = List.of(
            userDir.resolve(relativePath).normalize(),
            userDir.resolve("..").resolve(relativePath).normalize(),
            userDir.resolve("..").resolve("..").resolve(relativePath).normalize());

        Optional<Path> resolved = candidates.stream().filter(Files::exists).findFirst();
        if (resolved.isPresent()) {
            return resolved.get();
        }

        throw new AssertionError("Could not resolve checkout reference config file for '" + relativePath +
            "'. Set system property 'workspace.root' (or env WORKSPACE_ROOT), or ensure file is available in test resources. " +
            "user.dir=" + userDir + ", tried: " + candidates);
    }
}
