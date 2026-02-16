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
import org.pipelineframework.csv.common.domain.SendPaymentRequest;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
  inputType = PaymentRecord.class,
  outputType = AckPaymentSent.class,
  stepType = org.pipelineframework.step.StepOneToOne.class,
  backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class
)
@ApplicationScoped
@Getter
public class ProcessSendPaymentRecordService
    implements ReactiveService<PaymentRecord, AckPaymentSent> {
  private final PaymentProviderServiceMock paymentProviderServiceMock;

  @Inject
  Vertx vertx;

  /**
   * Create a ProcessSendPaymentRecordService with its required dependencies.
   *
   * @param paymentProviderServiceMock the payment provider mock used to perform payment operations
   * @param vertx the Vert.x instance used to offload blocking work to the Vert.x context
   */
  @Inject
  public ProcessSendPaymentRecordService(PaymentProviderServiceMock paymentProviderServiceMock, Vertx vertx) {
    this.paymentProviderServiceMock = paymentProviderServiceMock;
    this.vertx = vertx;
  }

  @Override
  public Uni<AckPaymentSent> process(PaymentRecord paymentRecord) {
    SendPaymentRequest request =
        new SendPaymentRequest()
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
    
    
    Logger logger = Logger.getLogger(this.getClass());

    return result
        .invoke(ackPaymentSent -> {
          String serviceId = this.getClass().toString();
          MDC.put("serviceId", serviceId);
          logger.infof("Executed command on %s --> %s", paymentRecord, ackPaymentSent);
          MDC.remove("serviceId");
        });
  }
}
