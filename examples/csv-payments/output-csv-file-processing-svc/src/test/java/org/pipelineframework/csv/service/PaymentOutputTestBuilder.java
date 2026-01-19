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

package org.pipelineframework.csv.service;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.UUID;

import org.pipelineframework.csv.common.dto.PaymentOutputDto;
import org.pipelineframework.csv.common.dto.PaymentStatusDto;

public class PaymentOutputTestBuilder {

    private String csvId = "80e055c9-7dbe-4ef0-ad37-8360eb8d1e3e";
    private String recipient = "recipient123";
    private BigDecimal amount = new BigDecimal("100.00");
    private Currency currency = Currency.getInstance("USD");
    private UUID conversationId = UUID.fromString("abacd5c7-2230-4a24-a665-32a542468ea5");
    private PaymentStatusDto paymentStatus;

    public static PaymentOutputTestBuilder aPaymentOutput() {
        return new PaymentOutputTestBuilder();
    }

    public PaymentOutputTestBuilder withCsvId(String csvId) {
        this.csvId = csvId;
        return this;
    }

    public PaymentOutputTestBuilder withRecipient(String recipient) {
        this.recipient = recipient;
        return this;
    }

    public PaymentOutputTestBuilder withAmount(BigDecimal amount) {
        this.amount = amount;
        return this;
    }

    public PaymentOutputTestBuilder withCurrency(Currency currency) {
        this.currency = currency;
        return this;
    }

    public PaymentOutputTestBuilder withConversationId(UUID conversationId) {
        this.conversationId = conversationId;
        return this;
    }

    public PaymentOutputTestBuilder withPaymentStatus(PaymentStatusDto paymentStatus) {
        this.paymentStatus = paymentStatus;
        return this;
    }

    public PaymentOutputDto buildDto() {
        return PaymentOutputDto.builder()
                .csvId(csvId)
                .recipient(recipient)
                .amount(amount)
                .currency(currency)
                .conversationId(conversationId)
                .status(0L)
                .message("")
                .fee(null)
                .paymentStatus(paymentStatus)
                .build();
    }
}
