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

package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.StringReader;
import org.junit.jupiter.api.Test;

class PipelineYamlConfigLoaderTest {

    @Test
    void loadsConnectorsFromPipelineYaml() {
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(new StringReader("""
            basePackage: "com.example"
            transport: "GRPC"
            platform: "COMPUTE"
            steps: []
            connectors:
              - name: "orders-to-delivery"
                enabled: true
                transport: "GRPC"
                mapper: "com.example.bridge.OrderConnectorMapper"
                idempotency: "PRE_FORWARD"
                backpressure: "BUFFER"
                failureMode: "PROPAGATE"
                backpressureBufferCapacity: 512
                idempotencyMaxKeys: 4096
                idempotencyKeyFields: ["orderId", "customerId"]
                source:
                  kind: "OUTPUT_BUS"
                  step: "Order Ready"
                  type: "com.example.grpc.OrderReady"
                target:
                  kind: "LIVE_INGEST"
                  pipeline: "deliver-order"
                  type: "com.example.grpc.DispatchReadyOrder"
                  adapter: "com.example.bridge.DeliverConnectorTarget"
            """));

        assertEquals(1, config.connectors().size());
        var connector = config.connectors().getFirst();
        assertEquals("orders-to-delivery", connector.name());
        assertEquals("OUTPUT_BUS", connector.source().kind());
        assertEquals("Order Ready", connector.source().step());
        assertEquals("deliver-order", connector.target().pipeline());
        assertEquals("com.example.bridge.DeliverConnectorTarget", connector.target().adapter());
        assertEquals("PRE_FORWARD", connector.idempotency());
        assertEquals(512, connector.backpressureBufferCapacity());
        assertEquals(4096, connector.idempotencyMaxKeys());
        assertNotNull(connector.idempotencyKeyFields());
        assertEquals(2, connector.idempotencyKeyFields().size());
    }
}
