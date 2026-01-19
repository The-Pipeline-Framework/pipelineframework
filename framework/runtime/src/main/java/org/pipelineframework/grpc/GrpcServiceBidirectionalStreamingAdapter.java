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

package org.pipelineframework.grpc;

import io.smallrye.mutiny.Multi;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

/**
 * Adapter for gRPC bidirectional streaming services that handle N-N (many-to-many) cardinality.
 * This adapter takes a stream of input messages and returns a stream of output messages, suitable
 * for bidirectional streaming scenarios where both client and server can send multiple messages to
 * each other.
 *
 * @param <GrpcIn> the gRPC input message type
 * @param <GrpcOut> the gRPC output message type
 * @param <DomainIn> the domain input object type
 * @param <DomainOut> the domain output object type
 */
public abstract class GrpcServiceBidirectionalStreamingAdapter<
    GrpcIn, GrpcOut, DomainIn, DomainOut> {

  /**
   * Default constructor for GrpcServiceBidirectionalStreamingAdapter.
   */
  public GrpcServiceBidirectionalStreamingAdapter() {
  }

    /**
   * Gets the reactive bidirectional streaming service for processing.
   *
   * @return the ReactiveBidirectionalStreamingService to use for processing
   */
  protected abstract ReactiveBidirectionalStreamingService<DomainIn, DomainOut> getService();

  /**
 * Map a gRPC request message to the adapter's domain input representation.
 *
 * @param grpcIn the incoming gRPC message
 * @return the domain input object corresponding to the provided gRPC message
 */
  protected abstract DomainIn fromGrpc(GrpcIn grpcIn);

  /**
   * Converts a domain output object to its corresponding gRPC message representation.
   *
   * @param domainOut the domain output object to convert
   * @return the converted gRPC output message
   */
  protected abstract GrpcOut toGrpc(DomainOut domainOut);

  /**
   * Adapts a bidirectional gRPC stream of incoming messages to a stream of outgoing messages by
   * converting inputs to domain objects, delegating processing to the domain service, and converting
   * results back to gRPC responses.
   *
   * @param requestStream the reactive stream of incoming {@code GrpcIn} messages
   * @return a reactive stream of {@code GrpcOut} messages produced by the domain service, or a gRPC
   *         failure if processing fails
   */
  public Multi<GrpcOut> remoteProcess(Multi<GrpcIn> requestStream) {
    Multi<DomainIn> domainStream = requestStream
            .onItem()
            .transform(this::fromGrpc);

    return getService().process(domainStream)
        .onItem()
        .transform(this::toGrpc)
        .onFailure()
        .transform(new throwStatusRuntimeExceptionFunction());
  }


}