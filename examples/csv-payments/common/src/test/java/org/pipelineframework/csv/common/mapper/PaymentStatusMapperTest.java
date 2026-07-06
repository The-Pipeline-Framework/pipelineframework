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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.PaymentStatus;

class PaymentStatusMapperTest {

  private PaymentStatusMapper mapper;

  @BeforeEach
  void setUp() {
    CommonConverters commonConverters = new CommonConverters();

    ApprovedPaymentStatusMapperImpl approvedImpl = new ApprovedPaymentStatusMapperImpl();
    setField(approvedImpl, "commonConverters", commonConverters);

    UnapprovedPaymentStatusMapperImpl unapprovedImpl = new UnapprovedPaymentStatusMapperImpl();
    setField(unapprovedImpl, "commonConverters", commonConverters);

    mapper = new PaymentStatusMapper();
    setField(mapper, "approvedMapper", approvedImpl);
    setField(mapper, "unapprovedMapper", unapprovedImpl);
  }

  @Test
  void wrapsApprovedStatusInApprovedUnionArm() {
    var status = PaymentStatusVariantMapperTestData.approvedStatus();

    var grpc = mapper.toExternal(status);

    assertTrue(grpc.hasApproved());
    assertEquals(status.getReference(), grpc.getApproved().getReference());
  }

  @Test
  void wrapsUnapprovedStatusInUnapprovedUnionArm() {
    var status = PaymentStatusVariantMapperTestData.unapprovedStatus();

    var grpc = mapper.toExternal(status);

    assertTrue(grpc.hasUnapproved());
    assertEquals(status.getReference(), grpc.getUnapproved().getReference());
  }

  @Test
  void unwrapsApprovedUnionArmBackToDomainSubtype() {
    var status = PaymentStatusVariantMapperTestData.approvedStatus();

    PaymentStatus mapped = mapper.fromExternal(mapper.toExternal(status));

    assertTrue(mapped instanceof org.pipelineframework.csv.common.domain.ApprovedPaymentStatus);
    assertEquals(status.getReference(), mapped.getReference());
  }

  @Test
  void unwrapsUnapprovedUnionArmBackToDomainSubtype() {
    var status = PaymentStatusVariantMapperTestData.unapprovedStatus();

    PaymentStatus mapped = mapper.fromExternal(mapper.toExternal(status));

    assertTrue(mapped instanceof org.pipelineframework.csv.common.domain.UnapprovedPaymentStatus);
    assertEquals(status.getReference(), mapped.getReference());
  }

  private static void setField(Object target, String fieldName, Object value) {
    try {
      java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set mapper dependency " + fieldName, e);
    }
  }
}
