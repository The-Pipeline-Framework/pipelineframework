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

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.dto.PaymentOutputDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class, PaymentStatusMapper.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PaymentOutputMapper extends org.pipelineframework.mapper.Mapper<PaymentOutput, org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput> {

  PaymentOutputMapper INSTANCE = Mappers.getMapper( PaymentOutputMapper.class );

  PaymentOutputDto toDto(PaymentOutput entity);

  PaymentOutput fromDto(PaymentOutputDto dto);

  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
  @Mapping(target = "currency", qualifiedByName = "currencyToString")
  @Mapping(target = "fee", qualifiedByName = "bigDecimalToString")
  @Mapping(target = "paymentStatus")
  org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput toGrpc(PaymentOutputDto dto);

  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
  @Mapping(target = "currency", qualifiedByName = "stringToCurrency")
  @Mapping(target = "fee", qualifiedByName = "stringToBigDecimal")
  @Mapping(target = "paymentStatus")
  PaymentOutputDto fromGrpc(org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput grpc);

  @Override
  default PaymentOutput fromExternal(org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput external) {
    return fromDto(fromGrpc(external));
  }

  @Override
  default org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput toExternal(PaymentOutput domain) {
    return toGrpc(toDto(domain));
  }

  @Deprecated(since = "26.4.3", forRemoval = true)
  default org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput toDtoToGrpc(PaymentOutput domain) {
    return toExternal(domain);
  }

  @Deprecated(since = "26.4.3", forRemoval = true)
  default PaymentOutput fromGrpcFromDto(org.pipelineframework.csv.grpc.PipelineTypes.PaymentOutput grpc) {
    return fromExternal(grpc);
  }
}
