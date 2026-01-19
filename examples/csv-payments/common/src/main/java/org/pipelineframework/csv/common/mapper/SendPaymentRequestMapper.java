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
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.csv.common.domain.SendPaymentRequest;
import org.pipelineframework.csv.common.dto.SendPaymentRequestDto;

@SuppressWarnings("unused")
@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class, PaymentRecordMapper.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface SendPaymentRequestMapper {

  SendPaymentRequestMapper INSTANCE = Mappers.getMapper( SendPaymentRequestMapper.class );

  /**
   * Map a SendPaymentRequest DTO to its domain representation.
   *
   * @param dto the DTO containing payment details to convert
   * @return a SendPaymentRequest domain object
   */
  SendPaymentRequest fromDto(SendPaymentRequestDto dto);

  /**
   * Convert a SendPaymentRequest domain object into the DTO representation.
   *
   * @param domain the SendPaymentRequest domain object to convert
   * @return a SendPaymentRequest DTO populated from the gRPC request
   */
  SendPaymentRequestDto toDto(SendPaymentRequest domain);
}
