package org.pipelineframework.csv.process_ack_payment_sent.service;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.pipelineframework.csv.service.PollAckPaymentSentService;

@Dependent
public class ProcessAckPaymentSentService extends org.pipelineframework.csv.service.ProcessAckPaymentSentService {
  /**
   * Creates a ProcessAckPaymentSentService using the provided poll service.
   *
   * @param pollAckPaymentSentService the PollAckPaymentSentService used by this instance to poll acknowledgement-of-payment-sent events
   */
  @Inject
  public ProcessAckPaymentSentService(PollAckPaymentSentService pollAckPaymentSentService) {
    super(pollAckPaymentSentService);
  }
}