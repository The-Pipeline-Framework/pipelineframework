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

import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.service.throwStatusRuntimeExceptionFunction;

/**
 * Adapter for gRPC reactive services that handle 1-1 (one-to-one) cardinality.
 * This adapter takes a single input message and returns a single output message, suitable
 * for unary gRPC scenarios.
 *
 * @param <GrpcIn> the gRPC input message type
 * @param <GrpcOut> the gRPC output message type
 * @param <DomainIn> the domain input object type
 * @param <DomainOut> the domain output object type
 */
public abstract class GrpcReactiveServiceAdapter<GrpcIn, GrpcOut, DomainIn, DomainOut> {

  /**
   * Default constructor for GrpcReactiveServiceAdapter.
   */
  public GrpcReactiveServiceAdapter() {
  }

  /**
 * Obtain the domain reactive service used to process domain inputs into domain outputs.
 *
 * @return the ReactiveService instance that processes DomainIn into DomainOut
 */
  protected abstract ReactiveService<DomainIn, DomainOut> getService();

  /**
 * Map a gRPC request message to the corresponding domain-layer input object.
 *
 * @param grpcIn the incoming gRPC message to convert
 * @return the domain input object produced from the gRPC message
 */
  protected abstract DomainIn fromGrpc(GrpcIn grpcIn);

  /**
 * Convert a domain-layer output value to its gRPC representation.
 *
 * @param domainOut the domain-layer result to convert into a gRPC message
 * @return the corresponding gRPC output message
 */
  protected abstract GrpcOut toGrpc(DomainOut domainOut);

  /**
   * Process a gRPC request through the reactive domain service.
   *
   * Converts the gRPC request to a domain input, invokes the domain reactive service, and converts the resulting domain output back to a gRPC response.
   *
   * @param grpcRequest the incoming gRPC request to process
   * @return the gRPC response corresponding to the processed domain output
   */
  public Uni<GrpcOut> remoteProcess(GrpcIn grpcRequest) {
    DomainIn entity = fromGrpc(grpcRequest);
    // Process the entity using the domain service
    Uni<DomainOut> processedResult = getService().process(entity);

    return processedResult
            .onItem().transform(this::toGrpc)
            .onFailure().transform(new throwStatusRuntimeExceptionFunction());
  }
}