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

package org.pipelineframework.connector;

import java.util.function.Function;

/**
 * Typed mapper used by generated connector bootstrap classes.
 * It extends {@link Function} so connector mappers remain compatible with existing functional APIs.
 *
 * @param <I> connector source payload type
 * @param <O> connector target payload type
 */
@FunctionalInterface
public interface ConnectorMapper<I, O> extends Function<I, O> {

    /**
 * Converts the given source payload to the target payload type.
 *
 * @param input the source payload to map
 * @return the mapped target payload
 */
O map(I input);

    /**
     * Apply mapping to the given input and return the mapped result.
     *
     * @param input the source payload to map
     * @return the mapped target payload
     */
    @Override
    default O apply(I input) {
        return map(input);
    }
}
