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

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentOutput;
import org.pipelineframework.csv.grpc.PipelineTypes;

@Mapper(
    componentModel = "jakarta",
    uses = CommonConverters.class,
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface UnapprovedPaymentOutputMapper
    extends org.pipelineframework.mapper.Mapper<
        UnapprovedPaymentOutput, PipelineTypes.UnapprovedPaymentOutput> {

  @BeanMapping(ignoreByDefault = true)
  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "csvPaymentsOutputFilename", source = "csvPaymentsOutputFilename")
  @Mapping(target = "csvPaymentsInputFilePath", qualifiedByName = "pathToString")
  @Mapping(target = "csvId", source = "csvId")
  @Mapping(target = "recipient", source = "recipient")
  @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
  @Mapping(target = "currency", qualifiedByName = "currencyToString")
  @Mapping(target = "conversationId", qualifiedByName = "uuidToString")
  @Mapping(target = "status", source = "status")
  @Mapping(target = "message", source = "message")
  @Mapping(target = "fee", qualifiedByName = "bigDecimalToString")
  PipelineTypes.UnapprovedPaymentOutput toGrpc(UnapprovedPaymentOutput domain);

  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "csvPaymentsInputFilePath", qualifiedByName = "stringToPath")
  @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
  @Mapping(target = "currency", qualifiedByName = "stringToCurrency")
  @Mapping(target = "conversationId", qualifiedByName = "stringToUUID")
  @Mapping(target = "fee", qualifiedByName = "stringToBigDecimal")
  UnapprovedPaymentOutput fromGrpc(PipelineTypes.UnapprovedPaymentOutput grpc);

  @Override
  default UnapprovedPaymentOutput fromExternal(PipelineTypes.UnapprovedPaymentOutput external) {
    return fromGrpc(external);
  }

  @Override
  default PipelineTypes.UnapprovedPaymentOutput toExternal(UnapprovedPaymentOutput domain) {
    return toGrpc(domain);
  }
}
