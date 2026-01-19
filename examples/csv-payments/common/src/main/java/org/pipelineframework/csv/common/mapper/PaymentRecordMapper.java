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
import org.pipelineframework.csv.grpc.ProcessCsvPaymentsInputSvc;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface PaymentRecordMapper extends org.pipelineframework.mapper.Mapper<ProcessCsvPaymentsInputSvc.PaymentRecord, PaymentRecordDto, PaymentRecord> {

  PaymentRecordMapper INSTANCE = Mappers.getMapper( PaymentRecordMapper.class );

  // Domain ↔ DTO
  @Override
  PaymentRecordDto toDto(PaymentRecord entity);

  @Override
  PaymentRecord fromDto(PaymentRecordDto dto);

  // DTO ↔ gRPC
  @Override
  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "amount", qualifiedByName = "bigDecimalToString")
  @Mapping(target = "currency", qualifiedByName = "currencyToString")
  @Mapping(target = "csvPaymentsInputFilePath", qualifiedByName = "pathToString")
  ProcessCsvPaymentsInputSvc.PaymentRecord toGrpc(PaymentRecordDto dto);

  @Override
  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "amount", qualifiedByName = "stringToBigDecimal")
  @Mapping(target = "currency", qualifiedByName = "stringToCurrency")
  @Mapping(target = "csvPaymentsInputFilePath", qualifiedByName = "stringToPath")
  PaymentRecordDto fromGrpc(ProcessCsvPaymentsInputSvc.PaymentRecord grpc);
}
