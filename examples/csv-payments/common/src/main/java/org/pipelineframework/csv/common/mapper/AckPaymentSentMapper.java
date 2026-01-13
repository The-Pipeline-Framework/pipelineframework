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
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.dto.AckPaymentSentDto;
import org.pipelineframework.csv.grpc.ProcessSendPaymentRecordSvc;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class, PaymentRecordMapper.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface AckPaymentSentMapper extends org.pipelineframework.mapper.Mapper<ProcessSendPaymentRecordSvc.AckPaymentSent, AckPaymentSentDto, AckPaymentSent> {

  AckPaymentSentMapper INSTANCE = Mappers.getMapper( AckPaymentSentMapper.class );

  // Domain ↔ DTO
  @Override
  @Mapping(target = "id")
  @Mapping(target = "conversationId")
  @Mapping(target = "paymentRecordId")
  @Mapping(target = "status")
  @Mapping(target = "message")
  @Mapping(target = "paymentRecord")
  AckPaymentSentDto toDto(AckPaymentSent domain);

  @Override
  @Mapping(target = "id")
  @Mapping(target = "conversationId")
  @Mapping(target = "paymentRecordId")
  @Mapping(target = "status")
  @Mapping(target = "message")
  @Mapping(target = "paymentRecord")
  AckPaymentSent fromDto(AckPaymentSentDto dto);

  // DTO ↔ gRPC
  @Override
  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "conversationId", qualifiedByName = "uuidToString")
  @Mapping(target = "paymentRecordId", qualifiedByName = "uuidToString")
  @Mapping(target = "status", qualifiedByName = "longToString")
  @Mapping(target = "message")
  @Mapping(target = "paymentRecord")
  ProcessSendPaymentRecordSvc.AckPaymentSent toGrpc(AckPaymentSentDto dto);

  @Override
  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "conversationId", qualifiedByName = "stringToUUID")
  @Mapping(target = "paymentRecordId", qualifiedByName = "stringToUUID")
  @Mapping(target = "status", qualifiedByName = "stringToLong")
  @Mapping(target = "message")
  @Mapping(target = "paymentRecord")
  AckPaymentSentDto fromGrpc(ProcessSendPaymentRecordSvc.AckPaymentSent grpc);
}
