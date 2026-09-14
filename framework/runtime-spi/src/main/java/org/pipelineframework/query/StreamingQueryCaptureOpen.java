/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework.query;

import java.util.Objects;
import java.util.concurrent.Flow;

/** Result of opening a streaming Query capture under first-writer authority. */
public sealed interface StreamingQueryCaptureOpen {
    record Replay(Flow.Publisher<StreamingQueryCaptureItem> items) implements StreamingQueryCaptureOpen {
        public Replay {
            items = Objects.requireNonNull(items, "streaming capture replay items must not be null");
        }
    }

    record Write(StreamingQueryCaptureWriter writer) implements StreamingQueryCaptureOpen {
        public Write {
            writer = Objects.requireNonNull(writer, "streaming capture writer must not be null");
        }
    }
}
