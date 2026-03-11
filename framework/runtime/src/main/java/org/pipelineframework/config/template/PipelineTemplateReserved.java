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

import java.util.List;

/**
 * Reserved field metadata for a named message.
 *
 * @param numbers reserved field numbers
 * @param names reserved field names
 */
public record PipelineTemplateReserved(
    List<Integer> numbers,
    List<String> names
) {
    public PipelineTemplateReserved {
        numbers = numbers == null ? List.of() : List.copyOf(numbers);
        names = names == null ? List.of() : List.copyOf(names);
    }
}
