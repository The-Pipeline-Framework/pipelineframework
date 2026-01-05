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
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveStreamingClientService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

/**
 * Adapter for gRPC client streaming services that handle N-1 (many-to-one) cardinality.
 * This adapter takes a stream of input messages and returns a single output message, suitable
 * for client streaming scenarios where the client sends multiple messages to the server.
 *
 * @param <GrpcIn> the gRPC input message type
 * @param <GrpcOut> the gRPC output message type
 * @param <DomainIn> the domain input object type
 * @param <DomainOut> the domain output object type
 */
public abstract class GrpcServiceClientStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> {

  /**
   * Default constructor for GrpcServiceClientStreamingAdapter.
   */
  public GrpcServiceClientStreamingAdapter() {
  }

    /**
   * Gets the reactive streaming client service for processing.
   *
   * @return the ReactiveStreamingClientService to use for processing
   */
  protected abstract ReactiveStreamingClientService<DomainIn, DomainOut> getService();

  /**
   * Converts a gRPC input object to the corresponding domain input object.
   *
   * @param grpcIn the gRPC input object to convert
   * @return the corresponding domain input object
   */
  protected abstract DomainIn fromGrpc(GrpcIn grpcIn);

  /**
   * Convert a domain output object to its gRPC representation.
   *
   * @param domainOut the domain-layer result to convert to a gRPC message
   * @return the corresponding gRPC output message
   */
  protected abstract GrpcOut toGrpc(DomainOut domainOut);

  /**
   * Orchestrates processing of a client-streaming gRPC request into a single gRPC response.
   *
   * @param requestStream the incoming stream of gRPC input messages
   * @return the gRPC output message produced from the domain result
   */
  public Uni<GrpcOut> remoteProcess(Multi<GrpcIn> requestStream) {
    // Convert incoming gRPC messages to domain objects
    Multi<DomainIn> domainStream = requestStream.onItem().transform(this::fromGrpc);

    // Pass the streaming Multi directly to the service
    Uni<DomainOut> processedResult = getService().process(domainStream);
    return processedResult
            .onItem().transform(this::toGrpc)
            .onFailure().transform(new throwStatusRuntimeExceptionFunction());
  }
}
