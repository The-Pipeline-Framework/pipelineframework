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
package org.pipelineframework.protocol;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class RuntimeProtocolArtifactBoundaryTest {

    private static final ClassLoader CLASS_LOADER = RuntimeProtocolArtifactBoundaryTest.class.getClassLoader();

    @Test
    void packagesProtocolSchemasWithoutGeneratedTransportAdapters() {
        assertNotNull(CLASS_LOADER.getResource("checkpoint_publication.proto"));
        assertNotNull(CLASS_LOADER.getResource("payload_reference_storage.proto"));
        assertNotNull(CLASS_LOADER.getResource("transition_worker.proto"));
        assertNotNull(CLASS_LOADER.getResource(
            "org/pipelineframework/orchestrator/TransitionWireResult.class"));
        assertNotNull(CLASS_LOADER.getResource(
            "org/pipelineframework/orchestrator/TransitionFailureEnvelope.class"));

        assertNull(CLASS_LOADER.getResource(
            "org/pipelineframework/checkpoint/grpc/MutinyCheckpointPublicationServiceGrpc.class"));
        assertNull(CLASS_LOADER.getResource(
            "org/pipelineframework/checkpoint/grpc/CheckpointPublicationServiceGrpc.class"));
        assertNull(CLASS_LOADER.getResource(
            "org/pipelineframework/orchestrator/grpc/MutinyTransitionWorkerServiceGrpc.class"));
        assertNull(CLASS_LOADER.getResource(
            "org/pipelineframework/orchestrator/grpc/TransitionWorkerServiceGrpc.class"));
        assertNull(CLASS_LOADER.getResource(
            "org/pipelineframework/proto/PayloadReferenceStorageProto.class"));
    }
}
