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
import io.vertx.mutiny.core.Vertx;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentRecordMapper;
import org.pipelineframework.csv.common.mapper.SendPaymentRequestMapper;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
  inputType = PaymentRecord.class,
  outputType = AckPaymentSent.class,
  stepType = org.pipelineframework.step.StepOneToOne.class,
  backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
  inboundMapper = PaymentRecordMapper.class,
  outboundMapper = AckPaymentSentMapper.class
)
@ApplicationScoped
@Getter
public class ProcessSendPaymentRecordReactiveService
    implements ReactiveService<PaymentRecord, AckPaymentSent> {
  private final PaymentProviderServiceMock paymentProviderServiceMock;

  @Inject
  Vertx vertx;

  @Inject
  public ProcessSendPaymentRecordReactiveService(PaymentProviderServiceMock paymentProviderServiceMock, Vertx vertx) {
    this.paymentProviderServiceMock = paymentProviderServiceMock;
    this.vertx = vertx;
  }

  @Override
  public Uni<AckPaymentSent> process(PaymentRecord paymentRecord) {
    SendPaymentRequestMapper.SendPaymentRequest request =
        new SendPaymentRequestMapper.SendPaymentRequest()
            .setAmount(paymentRecord.getAmount())
            .setReference(paymentRecord.getRecipient())
            .setCurrency(paymentRecord.getCurrency())
            .setPaymentRecord(paymentRecord)
            .setPaymentRecordId(paymentRecord.getId());

    // Execute blocking call while staying in the same Vert.x context
    Uni<AckPaymentSent> result = vertx.executeBlocking(
        () -> {
          // Blocking network call
          return paymentProviderServiceMock.sendPayment(request);
        }
    );    
    
    
    String serviceId = this.getClass().toString();
    MDC.put("serviceId", serviceId);
    Logger logger = Logger.getLogger(this.getClass());
    logger.infof("Executed command on %s --> %s", paymentRecord, result);
    MDC.remove("serviceId");

    return result;
  }
}
