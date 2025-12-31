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
import org.pipelineframework.service.ReactiveStreamingService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

/**
 * Adapter for gRPC server streaming services that handle 1-N (one-to-many) cardinality.
 * This adapter takes a single input message and returns a stream of output messages, suitable
 * for server streaming scenarios where the server sends multiple messages to the client.
 *
 * @param <GrpcIn> the gRPC input message type
 * @param <GrpcOut> the gRPC output message type
 * @param <DomainIn> the domain input object type
 * @param <DomainOut> the domain output object type
 */
public abstract class GrpcServiceStreamingAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut>
        extends ReactiveServiceAdapterBase {

  /**
   * Default constructor for GrpcServiceStreamingAdapter.
   */
  public GrpcServiceStreamingAdapter() {
  }

  /**
   * Gets the reactive streaming service for processing.
   *
   * @return the ReactiveStreamingService to use for processing
   */
  protected abstract ReactiveStreamingService<DomainIn, DomainOut> getService();

  /**
 * Convert a gRPC request object into the domain input representation.
 *
 * @return the domain input corresponding to the provided gRPC input
 */
  protected abstract DomainIn fromGrpc(GrpcIn grpcIn);

  /**
   * Convert a domain-level output object to its gRPC representation.
   *
   * @param domainOut the domain output to convert
   * @return the corresponding gRPC output
   */
  protected abstract GrpcOut toGrpc(DomainOut domainOut);

  /**
   * Adapts a gRPC request into the domain stream and returns a stream of gRPC responses.
   *
   * @param grpcRequest the incoming gRPC request to convert into a domain input
   * @return a Multi stream of gRPC responses corresponding to processed domain outputs; failures are mapped to status exceptions
   */
  public Multi<GrpcOut> remoteProcess(GrpcIn grpcRequest) {
    DomainIn entity = fromGrpc(grpcRequest);
    Multi<DomainOut> processedResult = getService().process(entity); // Multi<DomainOut>

    return processedResult
            .onItem().transform(this::toGrpc)
            .onFailure().transform(new throwStatusRuntimeExceptionFunction());
  }
}