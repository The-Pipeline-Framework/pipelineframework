/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.csv.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;
import org.pipelineframework.grpc.GrpcReactiveServiceAdapter;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = AckPaymentSent.class,
    outputType = PaymentStatus.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = GrpcReactiveServiceAdapter.class,
    inboundMapper = AckPaymentSentMapper.class,
    outboundMapper = PaymentStatusMapper.class,
    restEnabled = true
)
@ApplicationScoped
@Getter
public class ProcessAckPaymentSentReactiveService
    implements ReactiveService<AckPaymentSent, PaymentStatus> {
    
  private static final Logger LOG = Logger.getLogger(ProcessAckPaymentSentReactiveService.class);

  PollAckPaymentSentService pollAckPaymentSentService;

  /**
   * Create a ProcessAckPaymentSentReactiveService and wire its dependency.
   *
   * @param pollAckPaymentSentService the service used to process AckPaymentSent messages into PaymentStatus
   */
  @Inject
  public ProcessAckPaymentSentReactiveService(
          PollAckPaymentSentService pollAckPaymentSentService) {
    this.pollAckPaymentSentService = pollAckPaymentSentService;
    LOG.debug("ProcessAckPaymentSentReactiveService initialized");
  }

  @Override
  public Uni<PaymentStatus> process(AckPaymentSent ackPaymentSent) {
    LOG.debugf("Processing AckPaymentSent in ProcessAckPaymentSentReactiveService: id=%s, conversationId=%s, paymentRecordId=%s", 
        ackPaymentSent.getId(), ackPaymentSent.getConversationId(), ackPaymentSent.getPaymentRecordId());
    
    // Call the service with the new unified method
    Uni<PaymentStatus> result = pollAckPaymentSentService.process(ackPaymentSent);
    
    LOG.debug("Returning Uni from ProcessAckPaymentSentReactiveService");
    return result
        .onItem()
        .invoke(paymentStatus -> 
            LOG.debugf("Successfully processed AckPaymentSent, resulting PaymentStatus: id=%s, reference=%s, status=%s", 
                paymentStatus.getId(), paymentStatus.getReference(), paymentStatus.getStatus()))
        .onFailure()
        .invoke(failure -> 
            LOG.errorf(failure, "Failed to process AckPaymentSent in ProcessAckPaymentSentReactiveService"));
  }
}