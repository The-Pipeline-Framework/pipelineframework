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

package org.pipelineframework.csv.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;
import org.pipelineframework.csv.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class PaymentStatusMapper implements Mapper<PaymentStatus, PipelineTypes.PaymentStatus> {

  @Inject ApprovedPaymentStatusMapper approvedMapper;

  @Inject UnapprovedPaymentStatusMapper unapprovedMapper;

  @Override
  public PaymentStatus fromExternal(PipelineTypes.PaymentStatus external) {
    if (external == null) {
      return null;
    }
    return switch (external.getOutcomeCase()) {
      case APPROVED -> approvedMapper.fromExternal(external.getApproved());
      case UNAPPROVED -> unapprovedMapper.fromExternal(external.getUnapproved());
      case OUTCOME_NOT_SET -> throw new IllegalArgumentException("PaymentStatus outcome is not set");
    };
  }

  @Override
  public PipelineTypes.PaymentStatus toExternal(PaymentStatus domain) {
    if (domain == null) {
      return null;
    }
    PipelineTypes.PaymentStatus.Builder builder = PipelineTypes.PaymentStatus.newBuilder();
    if (domain instanceof ApprovedPaymentStatus approved) {
      return builder.setApproved(approvedMapper.toExternal(approved)).build();
    }
    if (domain instanceof UnapprovedPaymentStatus unapproved) {
      return builder.setUnapproved(unapprovedMapper.toExternal(unapproved)).build();
    }
    throw new IllegalArgumentException(
        "Unsupported PaymentStatus type: " + domain.getClass().getName());
  }
}
