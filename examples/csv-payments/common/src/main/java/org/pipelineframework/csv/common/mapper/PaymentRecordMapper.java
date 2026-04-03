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
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.dto.PaymentRecordDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PaymentRecordMapper extends org.pipelineframework.mapper.Mapper<PaymentRecord, org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord> {

  PaymentRecordMapper INSTANCE = Mappers.getMapper( PaymentRecordMapper.class );

  // Domain ↔ DTO
  PaymentRecordDto toDto(PaymentRecord entity);

  PaymentRecord fromDto(PaymentRecordDto dto);

  // DTO ↔ gRPC
  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
  @Mapping(target = "currency", qualifiedByName = "currencyToString")
  @Mapping(target = "csvPaymentsInputFilePath", qualifiedByName = "pathToString")
  org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord toGrpc(PaymentRecordDto dto);

  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
  @Mapping(target = "currency", qualifiedByName = "stringToCurrency")
  @Mapping(target = "csvPaymentsInputFilePath", qualifiedByName = "stringToPath")
  PaymentRecordDto fromGrpc(org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord grpc);

  @Override
  default PaymentRecord fromExternal(org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord external) {
    return fromDto(fromGrpc(external));
  }

  @Override
  default org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord toExternal(PaymentRecord domain) {
    return toGrpc(toDto(domain));
  }

  default org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord toDtoToGrpc(PaymentRecord domain) {
    return toExternal(domain);
  }

  default PaymentRecord fromGrpcFromDto(org.pipelineframework.csv.grpc.PipelineTypes.PaymentRecord grpc) {
    return fromExternal(grpc);
  }
}
