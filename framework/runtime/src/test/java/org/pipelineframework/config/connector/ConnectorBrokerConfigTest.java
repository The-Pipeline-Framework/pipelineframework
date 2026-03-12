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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConnectorBrokerConfigTest {

    @Test
    void constructorCreatesValidBrokerConfig() {
        ConnectorBrokerConfig config = new ConnectorBrokerConfig(
            "SQS",
            "https://sqs.us-east-1.amazonaws.com/123456789012/queue",
            "com.example.SqsBrokerAdapter");

        assertNotNull(config);
        assertEquals("SQS", config.provider());
        assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/queue", config.destination());
        assertEquals("com.example.SqsBrokerAdapter", config.adapter());
    }

    @Test
    void constructorRejectsNullProvider() {
        assertThrows(NullPointerException.class, () ->
            new ConnectorBrokerConfig(
                null,
                "destination",
                "adapter"));
    }

    @Test
    void constructorRejectsBlankProvider() {
        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorBrokerConfig(
                "",
                "destination",
                "adapter"));

        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorBrokerConfig(
                "   ",
                "destination",
                "adapter"));
    }

    @Test
    void constructorRejectsNullDestination() {
        assertThrows(NullPointerException.class, () ->
            new ConnectorBrokerConfig(
                "SQS",
                null,
                "adapter"));
    }

    @Test
    void constructorRejectsBlankDestination() {
        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorBrokerConfig(
                "SQS",
                "",
                "adapter"));

        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorBrokerConfig(
                "SQS",
                "   ",
                "adapter"));
    }

    @Test
    void constructorRejectsNullAdapter() {
        assertThrows(NullPointerException.class, () ->
            new ConnectorBrokerConfig(
                "SQS",
                "destination",
                null));
    }

    @Test
    void constructorRejectsBlankAdapter() {
        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorBrokerConfig(
                "SQS",
                "destination",
                ""));

        assertThrows(IllegalArgumentException.class, () ->
            new ConnectorBrokerConfig(
                "SQS",
                "destination",
                "   "));
    }

    @Test
    void brokerConfigSupportsKafkaProvider() {
        ConnectorBrokerConfig config = new ConnectorBrokerConfig(
            "KAFKA",
            "topic-name",
            "com.example.KafkaBrokerAdapter");

        assertEquals("KAFKA", config.provider());
    }

    @Test
    void brokerConfigSupportsRabbitMqProvider() {
        ConnectorBrokerConfig config = new ConnectorBrokerConfig(
            "RABBITMQ",
            "amqp://localhost:5672/queue-name",
            "com.example.RabbitMqBrokerAdapter");

        assertEquals("RABBITMQ", config.provider());
    }
}