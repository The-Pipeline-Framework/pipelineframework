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

package org.pipelineframework.telemetry;

import io.opentelemetry.api.common.Attributes;
import java.util.Map;

/** Converts already-derived attributes at the imperative SDK boundary. */
public final class TelemetrySdkAttributes {
    private TelemetrySdkAttributes() { }

    public static Attributes from(Map<String, String> values) {
        var builder = Attributes.builder();
        values.forEach(builder::put);
        return builder.build();
    }
}
