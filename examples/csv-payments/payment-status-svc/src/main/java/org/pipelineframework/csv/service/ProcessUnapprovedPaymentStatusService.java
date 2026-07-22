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
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.domain.UnapprovedPaymentOutput;
import org.pipelineframework.csv.domain.UnapprovedPaymentStatus;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.step.NonRetryableException;

@PipelineStep
@ApplicationScoped
public class ProcessUnapprovedPaymentStatusService
    implements ReactiveService<UnapprovedPaymentStatus, UnapprovedPaymentOutput> {

  @Override
  public Uni<UnapprovedPaymentOutput> process(UnapprovedPaymentStatus status) {
    if (status.paymentRecord() == null || status.paymentRecord().csvPaymentsInputFilePath() == null) {
      return Uni.createFrom().failure(new NonRetryableException(
          "UnapprovedPaymentStatus must include paymentRecord and csvPaymentsInputFilePath"));
    }
    var record = status.paymentRecord();
    return Uni.createFrom().item(new UnapprovedPaymentOutput(
        record.csvPaymentsInputFilePath().getFileName().toString(),
        record.csvPaymentsInputFilePath(),
        record.csvId(),
        record.recipient(),
        record.amount(),
        record.currency(),
        status.conversationId(),
        status.statusCode(),
        status.message(),
        status.fee()));
  }
}
