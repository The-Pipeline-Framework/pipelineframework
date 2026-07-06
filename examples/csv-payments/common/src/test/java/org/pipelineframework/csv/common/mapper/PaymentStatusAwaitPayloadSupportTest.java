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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitPayloadSupport;
import org.pipelineframework.awaitable.kafka.KafkaAwaitCompletionEnvelope;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus;
import org.pipelineframework.csv.grpc.PipelineTypes;

class PaymentStatusAwaitPayloadSupportTest {

  private PaymentStatusMapper mapper;

  @BeforeEach
  void setUp() {
    CommonConverters commonConverters = new CommonConverters();

    ApprovedPaymentStatusMapperImpl approved = new ApprovedPaymentStatusMapperImpl();
    setField(approved, "commonConverters", commonConverters);

    UnapprovedPaymentStatusMapperImpl unapproved = new UnapprovedPaymentStatusMapperImpl();
    setField(unapproved, "commonConverters", commonConverters);

    mapper = new PaymentStatusMapper();
    setField(mapper, "approvedMapper", approved);
    setField(mapper, "unapprovedMapper", unapproved);
  }

  @Test
  void kafkaCompletionEnvelopePreservesPaymentStatusUnionArmThroughObjectPayload() throws Exception {
    PipelineTypes.PaymentStatus union =
        mapper.toExternal(PaymentStatusVariantMapperTestData.approvedStatus());
    KafkaAwaitCompletionEnvelope envelope =
        new KafkaAwaitCompletionEnvelope(
            "tenant-1",
            "interaction-1",
            "corr-1",
            "resume-token",
            "idempotency-key",
            union,
            "actor");

    KafkaAwaitCompletionEnvelope parsed =
        PipelineJson.mapper()
            .readValue(PipelineJson.mapper().writeValueAsString(envelope), KafkaAwaitCompletionEnvelope.class);

    Object payload = parsed.responsePayload();
    Map<?, ?> payloadMap = assertInstanceOf(Map.class, payload);
    assertTrue(payloadMap.containsKey("approved"));

    Object coerced = AwaitPayloadSupport.coercePayload(payload, PipelineTypes.PaymentStatus.class);
    PipelineTypes.PaymentStatus rebuilt = assertInstanceOf(PipelineTypes.PaymentStatus.class, coerced);
    assertTrue(rebuilt.hasApproved());
    assertEquals(union.getApproved().getReference(), rebuilt.getApproved().getReference());
  }

  @Test
  void awaitPayloadCoercionRebuildsApprovedDomainVariantFromWrappedUnionPayload() throws Exception {
    PaymentStatus status = roundTripDomainStatus(PaymentStatusVariantMapperTestData.approvedStatus());

    ApprovedPaymentStatus approved = assertInstanceOf(ApprovedPaymentStatus.class, status);
    assertEquals("approved-ref", approved.getReference());
    assertEquals("Complete", approved.getStatus());
  }

  @Test
  void awaitPayloadCoercionRebuildsUnapprovedDomainVariantFromWrappedUnionPayload() throws Exception {
    PaymentStatus status = roundTripDomainStatus(PaymentStatusVariantMapperTestData.unapprovedStatus());

    UnapprovedPaymentStatus unapproved = assertInstanceOf(UnapprovedPaymentStatus.class, status);
    assertEquals("rejected-ref", unapproved.getReference());
    assertEquals("Rejected", unapproved.getStatus());
  }

  private PaymentStatus roundTripDomainStatus(PaymentStatus original) throws Exception {
    PipelineTypes.PaymentStatus union = mapper.toExternal(original);
    KafkaAwaitCompletionEnvelope envelope =
        new KafkaAwaitCompletionEnvelope(
            "tenant-1",
            "interaction-1",
            "corr-1",
            "resume-token",
            "idempotency-key",
            union,
            "actor");

    KafkaAwaitCompletionEnvelope parsed =
        PipelineJson.mapper()
            .readValue(
                PipelineJson.mapper().writeValueAsString(envelope), KafkaAwaitCompletionEnvelope.class);

    Object payload = parsed.responsePayload();
    Map<?, ?> payloadMap = assertInstanceOf(Map.class, payload);
    assertTrue(!payloadMap.isEmpty());

    Object coerced = AwaitPayloadSupport.coercePayload(payload, PaymentStatus.class);
    return assertInstanceOf(PaymentStatus.class, coerced);
  }

  private static void setField(Object target, String fieldName, Object value) {
    try {
      Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set mapper dependency " + fieldName, e);
    }
  }
}
