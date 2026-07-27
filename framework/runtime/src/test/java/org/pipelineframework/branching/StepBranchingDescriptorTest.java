package org.pipelineframework.branching;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.junit.jupiter.api.Test;

class StepBranchingDescriptorTest {

    @Test
    void unwrapsGeneratedV3RecordUnionPayloadForItsBranch() {
        ApprovedPaymentStatus payload = new ApprovedPaymentStatus("approved");
        PaymentStatus approved = new PaymentStatus.Approved(payload);
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            1,
            "process-approved",
            "ProcessApproved",
            ApprovedPaymentStatus.class.getName(),
            ApprovedPaymentStatus.class,
            List.of(ApprovedPaymentStatus.class.getName()),
            List.of(ApprovedPaymentStatus.class.getName()),
            List.of(ApprovedPaymentStatus.class),
            List.of(new BranchVariantIdentity("PaymentStatus", "approved", ApprovedPaymentStatus.class.getName())),
            List.of(new BranchVariantIdentity("PaymentStatus", "approved", ApprovedPaymentStatus.class.getName())),
            List.of(),
            false);

        assertSame(payload, descriptor.applicableItem(approved));
        assertEquals("approved", descriptor.variantIdentity(approved).orElseThrow().discriminator());
    }

    @Test
    void wrapsCanonicalBranchPayloadForGeneratedV3TerminalUnionInput() {
        ApprovedPaymentOutput payload = new ApprovedPaymentOutput("approved");
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            2,
            "finalize",
            "Finalize",
            PaymentOutputBranch.class.getName(),
            PaymentOutputBranch.class,
            List.of(ApprovedPaymentOutput.class.getName()),
            List.of(ApprovedPaymentOutput.class.getName()),
            List.of(ApprovedPaymentOutput.class),
            List.of(
                new BranchVariantIdentity("PaymentOutputBranch", "approved", ApprovedPaymentOutput.class.getName()),
                new BranchVariantIdentity("PaymentOutputBranch", "approvedRetry", ApprovedPaymentOutput.class.getName())),
            List.of(new BranchVariantIdentity(
                "PaymentOutputBranch", "approvedRetry", ApprovedPaymentOutput.class.getName())),
            List.of(),
            true);

        Object applicable = descriptor.applicableItem(payload);

        assertSame(payload, assertInstanceOf(PaymentOutputBranch.ApprovedRetry.class, applicable).value());
        assertNull(descriptor.applicableItem(new RejectedPaymentOutput("rejected")));
    }

    private sealed interface PaymentStatus permits PaymentStatus.Approved {
        record Approved(ApprovedPaymentStatus value) implements PaymentStatus {
        }
    }

    private record ApprovedPaymentStatus(String reference) {
    }

    private sealed interface PaymentOutputBranch permits PaymentOutputBranch.Approved, PaymentOutputBranch.ApprovedRetry {
        record Approved(ApprovedPaymentOutput value) implements PaymentOutputBranch {
        }

        record ApprovedRetry(ApprovedPaymentOutput value) implements PaymentOutputBranch {
        }
    }

    private record ApprovedPaymentOutput(String reference) {
    }

    private record RejectedPaymentOutput(String reference) {
    }
}
