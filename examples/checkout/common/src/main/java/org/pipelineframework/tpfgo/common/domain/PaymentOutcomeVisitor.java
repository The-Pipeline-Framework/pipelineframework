package org.pipelineframework.tpfgo.common.domain;

public interface PaymentOutcomeVisitor<R> {
    R captured(PaymentCaptured captured);

    R rejected(PaymentRejected rejected);

    R requiresReview(PaymentRequiresReview review);
}
