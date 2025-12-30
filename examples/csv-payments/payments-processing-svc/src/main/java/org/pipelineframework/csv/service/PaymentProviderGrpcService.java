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

import jakarta.inject.Inject;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.csv.common.mapper.AckPaymentSentMapper;
import org.pipelineframework.csv.common.mapper.PaymentStatusMapper;
import org.pipelineframework.csv.common.mapper.SendPaymentRequestMapper;
import org.pipelineframework.csv.grpc.MutinyPaymentProviderServiceGrpc;
import org.pipelineframework.csv.grpc.PaymentsProcessingSvc;

@GrpcService
public class PaymentProviderGrpcService
    extends MutinyPaymentProviderServiceGrpc.PaymentProviderServiceImplBase {

  @Inject PaymentProviderService domainService;

  SendPaymentRequestMapper sendPaymentRequestMapper = SendPaymentRequestMapper.INSTANCE;
  AckPaymentSentMapper ackPaymentSentMapper = AckPaymentSentMapper.INSTANCE;
  PaymentStatusMapper paymentStatusMapper = PaymentStatusMapper.INSTANCE;

  @Override
  public Uni<PaymentsProcessingSvc.AckPaymentSent> sendPayment(
      PaymentsProcessingSvc.SendPaymentRequest grpcRequest) {
    return Uni.createFrom()
        .item(
            () -> {
              var domainIn = sendPaymentRequestMapper.fromGrpc(grpcRequest);
              var domainOut = domainService.sendPayment(domainIn);
              return ackPaymentSentMapper.toDtoToGrpc(domainOut);
            })
        .onFailure()
        .transform(
            throwable -> {
              Metadata metadata = new Metadata();
              metadata.put(
                  Metadata.Key.of("details", Metadata.ASCII_STRING_MARSHALLER),
                  "Error in gRPC server: " + throwable.getMessage());
              return new StatusRuntimeException(
                  Status.INTERNAL.withDescription(throwable.getMessage()).withCause(throwable),
                  metadata);
            });
  }

  @Override
  public Uni<PaymentsProcessingSvc.PaymentStatus> getPaymentStatus(
      PaymentsProcessingSvc.AckPaymentSent grpcRequest) {
    return Uni.createFrom()
        .emitter(
            emitter -> {
              try {
                var domainIn = ackPaymentSentMapper.fromGrpcFromDto(grpcRequest);
                var domainOut = domainService.getPaymentStatus(domainIn);
                var grpcResponse = paymentStatusMapper.toDtoToGrpc(domainOut);
                emitter.complete(grpcResponse);
              } catch (Exception e) {
                emitter.fail(
                    io.grpc.Status.INTERNAL
                        .withDescription("Processing failed: " + e.getMessage())
                        .withCause(e)
                        .asRuntimeException());
              }
            });
  }
}
