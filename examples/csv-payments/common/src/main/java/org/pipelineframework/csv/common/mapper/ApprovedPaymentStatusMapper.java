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
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.grpc.PipelineTypes;

@Mapper(
    componentModel = "jakarta",
    uses = CommonConverters.class,
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface ApprovedPaymentStatusMapper
    extends org.pipelineframework.mapper.Mapper<
        ApprovedPaymentStatus, PipelineTypes.ApprovedPaymentStatus> {

  @BeanMapping(ignoreByDefault = true)
  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "reference", source = "reference")
  @Mapping(target = "status", source = "status")
  @Mapping(target = "message", source = "message")
  @Mapping(target = "fee", qualifiedByName = "bigDecimalToString")
  @Mapping(target = "conversationId", qualifiedByName = "uuidToString")
  @Mapping(target = "statusCode", qualifiedByName = "longToString")
  @Mapping(target = "paymentRecordId", qualifiedByName = "uuidToString")
  @Mapping(target = "paymentRecord", ignore = true)
  PipelineTypes.ApprovedPaymentStatus toGrpcPayload(ApprovedPaymentStatus domain);

  @Mapping(target = "customerReference", ignore = true)
  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "fee", qualifiedByName = "stringToBigDecimal")
  @Mapping(target = "conversationId", qualifiedByName = "stringToUUID")
  @Mapping(target = "statusCode", qualifiedByName = "stringToLong")
  @Mapping(target = "paymentRecordId", qualifiedByName = "stringToUUID")
  @Mapping(target = "paymentRecord", ignore = true)
  ApprovedPaymentStatus fromGrpcPayload(PipelineTypes.ApprovedPaymentStatus grpc);

  @Override
  default ApprovedPaymentStatus fromExternal(PipelineTypes.ApprovedPaymentStatus external) {
    ApprovedPaymentStatus mapped = fromGrpcPayload(external);
    mapped.setPaymentRecord(fromPaymentRecord(external.getPaymentRecord()));
    return mapped;
  }

  @Override
  default PipelineTypes.ApprovedPaymentStatus toExternal(ApprovedPaymentStatus domain) {
    return toGrpcPayload(domain).toBuilder()
        .setPaymentRecord(toPaymentRecord(domain.getPaymentRecord()))
        .build();
  }

  private static PipelineTypes.PaymentRecord toPaymentRecord(PaymentRecord record) {
    CommonConverters converters = new CommonConverters();
    return PipelineTypes.PaymentRecord.newBuilder()
        .setId(converters.toString(record.getId()))
        .setCsvId(record.getCsvId())
        .setRecipient(record.getRecipient())
        .setAmount(converters.bigDecimalToString(record.getAmount()))
        .setCurrency(converters.currencyToString(record.getCurrency()))
        .setCsvPaymentsInputFilePath(converters.pathToString(record.getCsvPaymentsInputFilePath()))
        .build();
  }

  private static PaymentRecord fromPaymentRecord(PipelineTypes.PaymentRecord record) {
    CommonConverters converters = new CommonConverters();
    PaymentRecord paymentRecord = new PaymentRecord();
    paymentRecord.setId(converters.toUUID(record.getId()));
    paymentRecord.setCsvId(record.getCsvId());
    paymentRecord.setRecipient(record.getRecipient());
    paymentRecord.setAmount(converters.stringToBigDecimal(record.getAmount()));
    paymentRecord.setCurrency(converters.stringToCurrency(record.getCurrency()));
    paymentRecord.setCsvPaymentsInputFilePath(converters.stringToPath(record.getCsvPaymentsInputFilePath()));
    return paymentRecord;
  }
}
