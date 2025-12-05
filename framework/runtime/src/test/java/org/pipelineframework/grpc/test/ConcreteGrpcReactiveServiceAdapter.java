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

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.domain.TestEntity;
import org.pipelineframework.domain.TestResult;
import org.pipelineframework.grpc.GrpcReactiveServiceAdapter;
import org.pipelineframework.service.ReactiveService;

@ApplicationScoped
public class ConcreteGrpcReactiveServiceAdapter
        extends GrpcReactiveServiceAdapter<Object, Object, TestEntity, TestResult> {

    private boolean autoPersist = false;

    public void setAutoPersistence(boolean autoPersist) {
        this.autoPersist = autoPersist;
    }

    @Override
    protected ReactiveService<TestEntity, TestResult> getService() {
        return new org.pipelineframework.service.TestReactiveService();
    }

    @Override
    protected TestEntity fromGrpc(Object grpcIn) {
        return new TestEntity("test", "description");
    }

    /**
     * Convert a domain TestResult into its gRPC representation.
     *
     * @param domainOut the domain-level result to convert
     * @return an Object representing the gRPC response; in this test implementation a newly created
     *         generic Object
     */
    @Override
    protected Object toGrpc(TestResult domainOut) {
        return new Object(); // This is just for testing
    }

    /**
     * Indicates whether automatic persistence is enabled for this adapter.
     *
     * @return `true` if auto-persistence is enabled, `false` otherwise.
     */
    @Override
    protected boolean isAutoPersistenceEnabled() {
        return autoPersist;
    }

    // Public methods for testing
    public TestEntity testFromGrpc(Object grpcIn) {
        return fromGrpc(grpcIn);
    }

    public Object testToGrpc(TestResult domainOut) {
        return toGrpc(domainOut);
    }
}
