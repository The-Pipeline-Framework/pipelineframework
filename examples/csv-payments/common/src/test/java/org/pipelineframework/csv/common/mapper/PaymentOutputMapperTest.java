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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.PaymentOutput;

class PaymentOutputMapperTest {

  private PaymentOutputMapper mapper;

  @BeforeEach
  void setUp() {
    CommonConverters commonConverters = new CommonConverters();
    PaymentOutputMapperImpl impl = new PaymentOutputMapperImpl();
    try {
      java.lang.reflect.Field commonConvertersField =
          PaymentOutputMapperImpl.class.getDeclaredField("commonConverters");
      commonConvertersField.setAccessible(true);
      commonConvertersField.set(impl, commonConverters);
      mapper = impl;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to set PaymentOutputMapper dependencies", e);
    }
  }

  @Test
  void roundTripsPaymentOutputIncludingPublishMetadata() {
    PaymentOutput output = new PaymentOutput();
    output.setId(UUID.randomUUID());
    output.setCsvPaymentsOutputFilename("payments.csv");
    output.setCsvPaymentsInputFilePath(Path.of("/tmp/payments.csv"));
    output.setCsvId("csv-1");
    output.setRecipient("Alice");
    output.setAmount(new BigDecimal("12.34"));
    output.setCurrency(Currency.getInstance("EUR"));
    output.setConversationId(UUID.randomUUID());
    output.setStatus(400L);
    output.setMessage("Mock payment provider rejected the payment.");
    output.setFee(new BigDecimal("1.01"));

    PaymentOutput mapped = mapper.fromExternal(mapper.toExternal(output));

    assertNotNull(mapped);
    assertEquals(output.getId(), mapped.getId());
    assertEquals(output.getCsvPaymentsOutputFilename(), mapped.getCsvPaymentsOutputFilename());
    assertEquals(output.getCsvPaymentsInputFilePath(), mapped.getCsvPaymentsInputFilePath());
    assertEquals(output.getCsvId(), mapped.getCsvId());
    assertEquals(output.getRecipient(), mapped.getRecipient());
    assertEquals(output.getAmount(), mapped.getAmount());
    assertEquals(output.getCurrency(), mapped.getCurrency());
    assertEquals(output.getConversationId(), mapped.getConversationId());
    assertEquals(output.getStatus(), mapped.getStatus());
    assertEquals(output.getMessage(), mapped.getMessage());
    assertEquals(output.getFee(), mapped.getFee());
  }
}
