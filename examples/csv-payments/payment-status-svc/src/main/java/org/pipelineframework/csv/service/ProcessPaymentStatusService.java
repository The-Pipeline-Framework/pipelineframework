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

import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.mapper.PaymentOutputMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;
import org.pipelineframework.service.ReactiveService;

@PipelineStep(
    inputType = PaymentStatus.class,
    outputType = PaymentOutput.class,
    stepType = org.pipelineframework.step.StepOneToOne.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = PaymentStatusMapper.class,
    outboundMapper = PaymentOutputMapper.class
)
@ApplicationScoped
@Getter
public class ProcessPaymentStatusService
    implements ReactiveService<PaymentStatus, PaymentOutput> {

  private static final Logger LOGGER = Logger.getLogger(ProcessPaymentStatusService.class);

  @Override
  public Uni<PaymentOutput> process(PaymentStatus paymentStatus) {
      AckPaymentSent ackPaymentSent = paymentStatus.getAckPaymentSent();
      assert ackPaymentSent != null;
      PaymentRecord paymentRecord = ackPaymentSent.getPaymentRecord();
      assert paymentRecord != null;

      PaymentOutput output = new PaymentOutput();
      output.setPaymentStatus(paymentStatus);
      output.setCsvId(paymentRecord.getCsvId());
      output.setRecipient(paymentRecord.getRecipient());
      output.setAmount(paymentRecord.getAmount());
      output.setCurrency(paymentRecord.getCurrency());
      output.setConversationId(ackPaymentSent.getConversationId());
      output.setStatus(ackPaymentSent.getStatus());
      output.setMessage(paymentStatus.getMessage());
      output.setFee(paymentStatus.getFee());

    return Uni.createFrom()
            .item(output)
            .invoke(
                result -> {
                  String serviceId = this.getClass().toString();
                  MDC.put("serviceId", serviceId);
                  LOGGER.infof("Executed command on %s --> %s", paymentStatus, result);
                  MDC.remove("serviceId");
                });
  }
}
