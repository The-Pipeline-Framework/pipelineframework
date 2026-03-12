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

package org.pipelineframework.config.connector;

/**
 * Connector target declaration.
 *
 * @param kind target kind, for example LIVE_INGEST
 * @param pipeline downstream pipeline identifier used for metadata
 * @param type target payload class name
 * @param adapter adapter bean class that implements the connector target contract
 */
public record ConnectorTargetConfig(
    String kind,
    String pipeline,
    String type,
    String adapter
) {
}
