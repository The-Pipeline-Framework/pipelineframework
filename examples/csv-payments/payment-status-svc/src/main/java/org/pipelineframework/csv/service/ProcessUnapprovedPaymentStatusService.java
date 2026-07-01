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
import lombok.Getter;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentOutput;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;

@PipelineStep
@ApplicationScoped
@Getter
public class ProcessUnapprovedPaymentStatusService
    extends AbstractProcessPaymentStatusService<
        UnapprovedPaymentStatus, UnapprovedPaymentOutput> {

  @Override
  protected UnapprovedPaymentOutput newOutput() {
    return new UnapprovedPaymentOutput();
  }
}
