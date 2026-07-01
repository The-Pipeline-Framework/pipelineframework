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
import java.nio.file.Path;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.csv.common.domain.PaymentOutputBranch;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.step.NonRetryableException;

abstract class AbstractProcessPaymentStatusService<I extends PaymentStatus, O extends PaymentOutputBranch>
    implements ReactiveService<I, O> {

  @Override
  public Uni<O> process(I paymentStatus) {
    PaymentRecord paymentRecord = paymentStatus.getPaymentRecord();
    if (paymentRecord == null) {
      return Uni.createFrom()
          .failure(
              new NonRetryableException(
                  paymentStatus.getClass().getSimpleName()
                      + " must include paymentRecord for CSV output mapping"));
    }

    Path inputFile = paymentRecord.getCsvPaymentsInputFilePath();
    if (inputFile == null || inputFile.getFileName() == null) {
      return Uni.createFrom()
          .failure(
              new NonRetryableException(
                  paymentStatus.getClass().getSimpleName()
                      + " must include csvPaymentsInputFilePath for CSV output naming"));
    }

    O output = newOutput();
    output.setCsvPaymentsInputFilePath(inputFile);
    output.setCsvPaymentsOutputFilename(inputFile.getFileName().toString());
    output.setCsvId(paymentRecord.getCsvId());
    output.setRecipient(paymentRecord.getRecipient());
    output.setAmount(paymentRecord.getAmount());
    output.setCurrency(paymentRecord.getCurrency());
    output.setConversationId(paymentStatus.getConversationId());
    output.setStatus(paymentStatus.getStatusCode());
    output.setMessage(paymentStatus.getMessage());
    output.setFee(paymentStatus.getFee());

    return Uni.createFrom()
        .item(output)
        .invoke(
            result -> {
              Logger logger = Logger.getLogger(getClass());
              MDC.put("serviceId", getClass().toString());
              logger.infof("Executed command on %s --> %s", paymentStatus, result);
              MDC.remove("serviceId");
            });
  }

  protected abstract O newOutput();
}
