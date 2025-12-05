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
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

/**
 * Example implementation of a bidirectional streaming service. This service demonstrates N-N
 * (many-to-many) cardinality where multiple input messages can generate multiple output messages.
 */
@ApplicationScoped
public class ExampleBidirectionalStreamingService
        implements ReactiveBidirectionalStreamingService<String, String> {

    @Override
    public Multi<String> process(Multi<String> processableObj) {
        // Example: For each input, generate multiple outputs
        return processableObj
                .onItem()
                .transformToMulti(
                        item -> Multi.createFrom()
                                .items(
                                        "processed_" + item + "_part1",
                                        "processed_" + item + "_part2",
                                        "processed_" + item + "_part3"))
                .concatenate(); // Flatten the multi of multis into a single multi
    }
}
