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
import org.pipelineframework.csv.common.domain.ApprovedPaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentOutputBranch;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentOutput;
import org.pipelineframework.csv.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class PaymentOutputBranchMapper
    implements Mapper<PaymentOutputBranch, PipelineTypes.PaymentOutputBranch> {

  @Inject ApprovedPaymentOutputMapper approvedMapper;

  @Inject UnapprovedPaymentOutputMapper unapprovedMapper;

  @Override
  public PaymentOutputBranch fromExternal(PipelineTypes.PaymentOutputBranch external) {
    if (external == null) {
      return null;
    }
    return switch (external.getOutcomeCase()) {
      case APPROVED -> approvedMapper.fromExternal(external.getApproved());
      case UNAPPROVED -> unapprovedMapper.fromExternal(external.getUnapproved());
      case OUTCOME_NOT_SET -> throw new IllegalArgumentException("PaymentOutputBranch outcome is not set");
    };
  }

  @Override
  public PipelineTypes.PaymentOutputBranch toExternal(PaymentOutputBranch domain) {
    if (domain == null) {
      return null;
    }
    PipelineTypes.PaymentOutputBranch.Builder builder = PipelineTypes.PaymentOutputBranch.newBuilder();
    if (domain instanceof ApprovedPaymentOutput approved) {
      return builder.setApproved(approvedMapper.toExternal(approved)).build();
    }
    if (domain instanceof UnapprovedPaymentOutput unapproved) {
      return builder.setUnapproved(unapprovedMapper.toExternal(unapproved)).build();
    }
    throw new IllegalArgumentException(
        "Unsupported PaymentOutputBranch type: " + domain.getClass().getName());
  }
}
