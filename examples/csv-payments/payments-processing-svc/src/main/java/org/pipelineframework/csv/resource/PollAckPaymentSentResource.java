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

package org.pipelineframework.csv.resource;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.csv.common.domain.AckPaymentSent;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.csv.common.dto.AckPaymentSentDto;
import org.pipelineframework.csv.common.dto.PaymentStatusDto;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;
import org.pipelineframework.csv.service.PollAckPaymentSentReactiveService;

@Path("/api/v1/payments-processing")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PollAckPaymentSentResource {

    @Inject
    PollAckPaymentSentReactiveService service;

    @Inject
    AckPaymentSentMapper ackPaymentSentMapper;
    @Inject
    PaymentStatusMapper paymentStatusMapper;

    @POST
    @Path("/poll-ack-payment")
    public Uni<PaymentStatusDto> process(AckPaymentSentDto ackPaymentSentDto) {
        AckPaymentSent ackPaymentSent = ackPaymentSentMapper.fromDto(ackPaymentSentDto);
        Uni<PaymentStatus> paymentStatus = service.process(ackPaymentSent);
        return paymentStatus.onItem().transform(paymentStatusMapper::toDto);
    }
}
