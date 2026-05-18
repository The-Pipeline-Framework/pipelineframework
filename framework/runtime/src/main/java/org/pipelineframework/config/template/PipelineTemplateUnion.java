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

package org.pipelineframework.config.template;

import java.util.Map;
import java.util.Objects;

/**
 * First-class named closed union definition for template IDL v2.
 *
 * @param name union wrapper name
 * @param variants variant definitions keyed by authored variant name
 */
public record PipelineTemplateUnion(
    String name,
    Map<String, PipelineTemplateUnionVariant> variants
) {
    public PipelineTemplateUnion {
        Objects.requireNonNull(name, "name must not be null");
        variants = variants == null ? Map.of() : Map.copyOf(variants);
    }
}
