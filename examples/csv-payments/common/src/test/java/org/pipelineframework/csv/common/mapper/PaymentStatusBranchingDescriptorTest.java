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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.lang.reflect.Field;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.branching.StepBranchingDescriptor;
import org.pipelineframework.csv.common.domain.ApprovedPaymentStatus;
import org.pipelineframework.csv.grpc.PipelineTypes;

class PaymentStatusBranchingDescriptorTest {

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
  void extractsApprovedProtoVariantFromPaymentStatusUnionWrapper() {
    PipelineTypes.PaymentStatus union =
        mapper.toExternal(PaymentStatusVariantMapperTestData.approvedStatus());
    StepBranchingDescriptor descriptor =
        new StepBranchingDescriptor(
            2,
            "Process Approved Payment Status",
            "test.Step",
            PipelineTypes.ApprovedPaymentStatus.class.getName(),
            PipelineTypes.ApprovedPaymentStatus.class,
            List.of("ApprovedPaymentStatus"),
            List.of(PipelineTypes.ApprovedPaymentStatus.class.getName()),
            List.of(PipelineTypes.ApprovedPaymentStatus.class),
            false);

    Object extracted = descriptor.applicableItem(union);

    PipelineTypes.ApprovedPaymentStatus approved =
        assertInstanceOf(PipelineTypes.ApprovedPaymentStatus.class, extracted);
    assertNotNull(approved);
    assertEquals(union.getApproved().getReference(), approved.getReference());
  }

  @Test
  void extractsFreshApprovedProtoVariantForEachPaymentStatusUnionWrapper() {
    ApprovedPaymentStatus firstStatus = PaymentStatusVariantMapperTestData.approvedStatus();
    firstStatus.setReference("approved-first");
    firstStatus.getPaymentRecord().setRecipient("First Recipient");
    ApprovedPaymentStatus secondStatus = PaymentStatusVariantMapperTestData.approvedStatus();
    secondStatus.setReference("approved-second");
    secondStatus.getPaymentRecord().setRecipient("Second Recipient");
    StepBranchingDescriptor descriptor = approvedDescriptor();

    PipelineTypes.ApprovedPaymentStatus first =
        assertInstanceOf(
            PipelineTypes.ApprovedPaymentStatus.class,
            descriptor.applicableItem(mapper.toExternal(firstStatus)));
    PipelineTypes.ApprovedPaymentStatus second =
        assertInstanceOf(
            PipelineTypes.ApprovedPaymentStatus.class,
            descriptor.applicableItem(mapper.toExternal(secondStatus)));

    assertEquals("approved-first", first.getReference());
    assertEquals("First Recipient", first.getPaymentRecord().getRecipient());
    assertEquals("approved-second", second.getReference());
    assertEquals("Second Recipient", second.getPaymentRecord().getRecipient());
  }

  private static StepBranchingDescriptor approvedDescriptor() {
    return new StepBranchingDescriptor(
        2,
        "Process Approved Payment Status",
        "test.Step",
        PipelineTypes.ApprovedPaymentStatus.class.getName(),
        PipelineTypes.ApprovedPaymentStatus.class,
        List.of("ApprovedPaymentStatus"),
        List.of(PipelineTypes.ApprovedPaymentStatus.class.getName()),
        List.of(PipelineTypes.ApprovedPaymentStatus.class),
        false);
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
