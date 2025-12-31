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

package org.pipelineframework.grpc.test;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.grpc.GrpcServiceBidirectionalStreamingAdapter;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

@ApplicationScoped
public class ConcreteGrpcBidirectionalStreamingAdapter
        extends GrpcServiceBidirectionalStreamingAdapter<String, String, String, String> {

    /**
     * Provide the reactive bidirectional streaming service used by this adapter.
     *
     * @return the ReactiveBidirectionalStreamingService instance that processes String stream inputs and produces String stream outputs
     */
    @Override
    protected ReactiveBidirectionalStreamingService<String, String> getService() {
        return new TestBidirectionalStreamingService();
    }

    @Override
    protected String fromGrpc(String grpcIn) {
        return "domain_" + grpcIn;
    }

    /**
     * Convert a domain output string to its gRPC representation.
     *
     * @param domainOut the domain output string to convert
     * @return the gRPC-formatted string prefixed with "grpc_"
     */
    @Override
    protected String toGrpc(String domainOut) {
        return "grpc_" + domainOut;
    }

    /**
     * Exposes the conversion of a gRPC input string to the adapter's domain representation for tests.
     *
     * @param grpcIn the input string in gRPC form
     * @return the corresponding domain representation of the input
     */
    public String testFromGrpc(String grpcIn) {
        return fromGrpc(grpcIn);
    }

    public String testToGrpc(String domainOut) {
        return toGrpc(domainOut);
    }

    // Test service implementation
    private static class TestBidirectionalStreamingService
            implements ReactiveBidirectionalStreamingService<String, String> {
        @Override
        public Multi<String> process(Multi<String> input) {
            return input.onItem().transform(item -> "result_" + item.replace("input_", ""));
        }
    }
}