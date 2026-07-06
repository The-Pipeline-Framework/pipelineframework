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

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentOutputBranch;
import org.pipelineframework.service.ReactiveService;

@PipelineStep
@ApplicationScoped
public class ProcessFinalizePaymentOutputService
    implements ReactiveService<PaymentOutputBranch, PaymentOutput> {

  private static final Logger LOGGER = Logger.getLogger(ProcessFinalizePaymentOutputService.class);

  @Override
  public Uni<PaymentOutput> process(PaymentOutputBranch branchOutput) {
    PaymentOutput output = new PaymentOutput();
    output.setCsvPaymentsOutputFilename(branchOutput.getCsvPaymentsOutputFilename());
    output.setCsvPaymentsInputFilePath(branchOutput.getCsvPaymentsInputFilePath());
    output.setCsvId(branchOutput.getCsvId());
    output.setRecipient(branchOutput.getRecipient());
    output.setAmount(branchOutput.getAmount());
    output.setCurrency(branchOutput.getCurrency());
    output.setConversationId(branchOutput.getConversationId());
    output.setStatus(branchOutput.getStatus());
    output.setMessage(branchOutput.getMessage());
    output.setFee(branchOutput.getFee());

    return Uni.createFrom()
        .item(output)
        .invoke(
            result -> {
              MDC.put("serviceId", getClass().toString());
              LOGGER.infof("Executed command on %s --> %s", branchOutput, result);
              MDC.remove("serviceId");
            });
  }
}
