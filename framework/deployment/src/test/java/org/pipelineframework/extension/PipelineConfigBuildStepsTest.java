package org.pipelineframework.extension;

import jakarta.enterprise.inject.spi.DeploymentException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class PipelineConfigBuildStepsTest {

    @TempDir
    Path tempDir;
    private String prevConfig;
    private String prevUserDir;

    @BeforeEach
    void saveProperties() {
        prevConfig = System.getProperty("pipeline.config");
        prevUserDir = System.getProperty("user.dir");
    }

    @AfterEach
    void restoreProperties() {
        restoreProperty("pipeline.config", prevConfig);
        restoreProperty("user.dir", prevUserDir);
    }

    @Test
    void loadsDelegatedStepsFromExplicitPipelineConfig() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            steps:
              - name: "Delegated A"
                operator: "com.acme.operators.Foo::run"
              - name: "Internal B"
                service: "com.acme.service.InternalStep"
            """);

        System.setProperty("pipeline.config", config.toString());
        PipelineConfigBuildItem item = new PipelineConfigBuildSteps().loadPipelineConfig();

        assertEquals(1, item.steps().size());
        PipelineConfigBuildItem.StepConfig step = item.steps().get(0);
        assertEquals("Delegated A", step.name());
        assertEquals("com.acme.operators.Foo::run", step.operator());
    }

    @Test
    void failsWhenOperatorAndDelegateConflict() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            steps:
              - name: "Delegated A"
                operator: "com.acme.operators.Foo::run"
                delegate: "com.acme.operators.Bar::run"
            """);

        System.setProperty("pipeline.config", config.toString());
        DeploymentException ex = assertThrows(
                DeploymentException.class,
                () -> new PipelineConfigBuildSteps().loadPipelineConfig());
        assertTrue(ex.getMessage().contains("defines both operator and delegate"));
    }

    @Test
    void detectsBranchAwareRoutingOnlyForUnionTypedInputsOrExplicitMarkers() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            unions:
              PaymentOutcome:
                variants:
                  captured:
                    type: PaymentCaptured
                    number: 1
                  rejected:
                    type: PaymentRejected
                    number: 2
            steps:
              - name: "Linear Step"
                inputTypeName: "PaymentRecord"
                outputTypeName: "PaymentOutcome"
              - name: "Terminal Step"
                inputTypeName: "PaymentOutcome"
                outputTypeName: "TerminalOrderState"
                terminal: true
            """);

        System.setProperty("pipeline.config", config.toString());
        PipelineConfigBuildItem item = new PipelineConfigBuildSteps().loadPipelineConfig();

        assertTrue(item.branchAware());
    }

    @Test
    void detectsV3DeclaredUnionInputThroughCanonicalInputSyntax() throws Exception {
        Path config = tempDir.resolve("pipeline-v3.yaml");
        Files.writeString(config, """
            version: 3
            types:
              PaymentApproved:
                fields: [[id, string]]
              PaymentDeclined:
                fields: [[id, string]]
              PaymentOutcome:
                variants:
                  approved: PaymentApproved
                  declined: PaymentDeclined
            steps:
              - name: Handle Outcome
                input: PaymentOutcome
                output: PaymentApproved
            """);

        System.setProperty("pipeline.config", config.toString());

        assertTrue(new PipelineConfigBuildSteps().loadPipelineConfig().branchAware());
    }

    @Test
    void doesNotTreatNonIntegralVersionAsV3ForRawBranchPreflight() throws Exception {
        Path config = tempDir.resolve("pipeline-non-integral.yaml");
        Files.writeString(config, """
            version: 3.5
            types:
              Approved:
                fields: [[id, string]]
              Outcome:
                variants:
                  approved: Approved
            steps:
              - name: Handle Outcome
                input: Outcome
                output: Approved
            """);

        System.setProperty("pipeline.config", config.toString());

        assertFalse(new PipelineConfigBuildSteps().loadPipelineConfig().branchAware());
    }

    @Test
    void doesNotTreatEveryInputTypeNameAsBranchAware() throws Exception {
        Path config = tempDir.resolve("pipeline.yaml");
        Files.writeString(config, """
            steps:
              - name: "Linear Step"
                inputTypeName: "PaymentRecord"
                outputTypeName: "PaymentDecision"
              - name: "Finalize"
                inputTypeName: "PaymentDecision"
                outputTypeName: "Receipt"
            """);

        System.setProperty("pipeline.config", config.toString());
        PipelineConfigBuildItem item = new PipelineConfigBuildSteps().loadPipelineConfig();

        assertTrue(item.steps().isEmpty());
        assertFalse(item.branchAware());
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
