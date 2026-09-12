package org.pipelineframework.branching;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BranchVariantIdentityTest {

    @Test
    void rejectsNullComponents() {
        assertThrows(NullPointerException.class,
            () -> new BranchVariantIdentity(null, "approved", "ApprovedPaymentStatus"));
        assertThrows(NullPointerException.class,
            () -> new BranchVariantIdentity("PaymentStatus", null, "ApprovedPaymentStatus"));
        assertThrows(NullPointerException.class,
            () -> new BranchVariantIdentity("PaymentStatus", "approved", null));
    }

    @Test
    void rejectsBlankComponents() {
        assertThrows(IllegalArgumentException.class,
            () -> new BranchVariantIdentity("", "approved", "ApprovedPaymentStatus"));
        assertThrows(IllegalArgumentException.class,
            () -> new BranchVariantIdentity("PaymentStatus", " ", "ApprovedPaymentStatus"));
        assertThrows(IllegalArgumentException.class,
            () -> new BranchVariantIdentity("PaymentStatus", "approved", "\t"));
    }

    @Test
    void preservesProvidedValues() {
        BranchVariantIdentity identity = new BranchVariantIdentity(
            " PaymentStatus ",
            " approved ",
            " ApprovedPaymentStatus ");

        assertEquals(" PaymentStatus ", identity.unionName());
        assertEquals(" approved ", identity.discriminator());
        assertEquals(" ApprovedPaymentStatus ", identity.payloadContract());
    }
}
