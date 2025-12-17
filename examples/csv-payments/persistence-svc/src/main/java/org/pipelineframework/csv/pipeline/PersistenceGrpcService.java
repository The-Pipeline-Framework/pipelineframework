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

package org.pipelineframework.csv.pipeline;

import jakarta.inject.Inject;

import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.pipelineframework.csv.grpc.MutinyPersistenceServiceGrpc;
import org.pipelineframework.plugin.api.PluginReactiveUnary;
import org.pipelineframework.plugin.runtime.PluginEngine;

/**
 * A generic persistence service that can persist any entity type that has a corresponding
 * PersistenceProvider configured in the system.
 * <p>
 * This service uses the PluginEngine to resolve the appropriate persistence plugin based on the message type.
 */
@GrpcService
public class PersistenceGrpcService extends MutinyPersistenceServiceGrpc.PersistenceServiceImplBase {

    private static final Logger LOG = Logger.getLogger(PersistenceGrpcService.class);

    @Inject
    PluginEngine pluginEngine;

    @Override
    public Uni<Empty> remoteProcess(Any input) {
        LOG.debugf("Received Any message for persistence");

        if (input == null || input.getValue().isEmpty()) {
            LOG.warn("Received null or empty Any message for persistence, returning empty response");
            return Uni.createFrom().item(Empty.getDefaultInstance());
        }

        // Attempt to get the actual class from the type URL
        Class<?> entityClass = getClassFromTypeUrl(input.getTypeUrl());

        if (entityClass == null) {
            LOG.warnf("Could not determine entity class from type URL: %s", input.getTypeUrl());
            return Uni.createFrom().item(Empty.getDefaultInstance());
        }

        Object entityToPersist;
        try {
            // Determine the actual object by unpacking the Any message
            // For type safety, we need to cast the entityClass to a Class that extends Message
            @SuppressWarnings("unchecked")
            Class<? extends com.google.protobuf.Message> messageClass =
                (Class<? extends com.google.protobuf.Message>) entityClass;
            // The message might need conversion to the appropriate Java entity
            // For entities that are protobuf messages, we can use them directly
            // For JPA entities, the conversion would happen elsewhere in a real implementation
            entityToPersist = input.unpack(messageClass);
        } catch (Exception e) {
            LOG.errorf(e, "Failed to unpack Any message of type URL: %s", input.getTypeUrl());
            return Uni.createFrom().item(Empty.getDefaultInstance());
        }

        LOG.debugf("About to persist entity of type: %s", entityClass.getName());

        try {
            // Use the PluginEngine to resolve the appropriate persistence plugin based on the entity type
            var persistencePlugin = pluginEngine.resolveReactiveUnary(entityClass);
            @SuppressWarnings("unchecked")
            var typedPlugin = (PluginReactiveUnary<Object>) persistencePlugin;
            return typedPlugin.process(entityToPersist)
                .onItem().transform(result -> Empty.getDefaultInstance())
                .onFailure().invoke(failure -> LOG.error("Failed to persist entity of type: " + entityClass.getName(), failure))
                .replaceWith(Empty.getDefaultInstance());
        } catch (Exception e) {
            LOG.errorf(e, "Error during plugin resolution or persistence operation for entity of type: %s", entityClass.getName());
            return Uni.createFrom().item(Empty.getDefaultInstance());
        }
    }

    /**
     * Helper method to map type URLs to actual Java classes
     * Handles all possible protobuf message types in the csv-payments example
     */
    private Class<?> getClassFromTypeUrl(String typeUrl) {
        // Extract the type name from the URL
        // Google's Any type URLs follow the format: type.googleapis.com/package.MessageType
        if (typeUrl != null && typeUrl.startsWith("type.googleapis.com/")) {
            String typeName = typeUrl.substring("type.googleapis.com/".length());

            // Map all known protobuf message types - this covers all messages in the csv-payments example
            // Note: These are the protobuf DTOs, not the JPA entities
            try {
                if (typeName.endsWith("PaymentRecord")) {
                    return Class.forName("org.pipelineframework.csv.grpc.PaymentRecord");
                } else if (typeName.endsWith("PaymentOutput")) {
                    return Class.forName("org.pipelineframework.csv.grpc.PaymentOutput");
                } else if (typeName.endsWith("PaymentStatus")) {
                    return Class.forName("org.pipelineframework.csv.grpc.PaymentStatus");
                } else if (typeName.endsWith("AckPaymentSent")) {
                    return Class.forName("org.pipelineframework.csv.grpc.AckPaymentSent");
                } else if (typeName.endsWith("CsvFolder")) {
                    return Class.forName("org.pipelineframework.csv.grpc.CsvFolder");
                } else if (typeName.endsWith("CsvPaymentsInputFile")) {
                    return Class.forName("org.pipelineframework.csv.grpc.CsvPaymentsInputFile");
                } else if (typeName.endsWith("CsvPaymentsOutputFile")) {
                    return Class.forName("org.pipelineframework.csv.grpc.CsvPaymentsOutputFile");
                }
                // For a more generic approach, we could try to dynamically load the class
                // This handles any other types that might be added in the future
                return Class.forName(typeName.replace("/", "."));
            } catch (ClassNotFoundException e) {
                LOG.warnf("Could not find class for type URL: %s", typeUrl);
                return null;
            }
        }
        return null;
    }
}