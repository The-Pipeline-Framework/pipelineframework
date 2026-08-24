/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.proto;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorPayloadOrigin;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.repository.PayloadReference;

/**
 * Public durable codec for the canonical {@code payload_ref} protobuf representation.
 *
 * <p>Applications may store the returned bytes as one opaque persistence value. Schema evolution
 * follows protobuf compatibility: existing field numbers and meanings are immutable, removed
 * numbers are reserved, and compatible additions use new numbers.</p>
 */
public final class PayloadReferenceProtobufCodec {
    public static final int SCHEMA_VERSION = 1;

    private PayloadReferenceProtobufCodec() {
    }

    /** Encodes the complete canonical reference using the published {@code payload_ref} schema. */
    public static byte[] encode(PayloadReference reference) {
        if (reference == null) {
            throw new IllegalArgumentException("payload reference must not be null");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CodedOutputStream output = CodedOutputStream.newInstance(bytes);
            output.useDeterministicSerialization();
            toProto(reference).writeTo(output);
            output.flush();
            return bytes.toByteArray();
        } catch (IOException failure) {
            throw new IllegalStateException("could not encode payload reference protobuf", failure);
        }
    }

    /** Decodes one opaque {@code payload_ref} protobuf value. */
    public static PayloadReference decode(byte[] encoded) {
        if (encoded == null) {
            throw new IllegalArgumentException("encoded payload reference must not be null");
        }
        try {
            return fromProto(PayloadReferenceStorageProto.PayloadReference.parseFrom(encoded));
        } catch (InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("invalid payload reference protobuf", failure);
        }
    }

    private static PayloadReferenceStorageProto.PayloadReference toProto(PayloadReference value) {
        var reference = PayloadReferenceStorageProto.PayloadReference.newBuilder()
            .setKey(value.key())
            .setSizeBytes(value.sizeBytes())
            .putAllMetadata(value.metadata());
        setIfPresent(value.provider(), reference::setProvider);
        setIfPresent(value.container(), reference::setContainer);
        setIfPresent(value.contentType(), reference::setContentType);
        setIfPresent(value.codec(), reference::setCodec);
        setIfPresent(value.checksum(), reference::setChecksum);
        setIfPresent(value.version(), reference::setVersion);
        value.connectorOrigin().ifPresent(origin -> reference.setConnectorOrigin(toProto(origin)));
        return reference.build();
    }

    private static PayloadReferenceStorageProto.ConnectorPayloadOrigin toProto(ConnectorPayloadOrigin origin) {
        var encoded = PayloadReferenceStorageProto.ConnectorPayloadOrigin.newBuilder()
            .setBindingName(origin.bindingName().value())
            .setProviderId(origin.operation().providerId().value())
            .setOperationId(origin.operation().operationId())
            .setOperationKind(origin.operation().kind().value())
            .setOperationMajorVersion(origin.operation().majorVersion())
            .setProviderMajorVersion(origin.providerMajorVersion());
        origin.configuration().ifPresent(configuration -> encoded.setHasConfiguration(true)
            .setConfigurationSchemaId(configuration.schemaId())
            .setConfigurationSchemaVersion(configuration.schemaVersion())
            .setConfigurationDigest(configuration.digest())
            .addAllConnectionReferences(configuration.connectionReferences().stream()
                .map(ConnectionRef::value).toList()));
        return encoded.build();
    }

    private static PayloadReference fromProto(PayloadReferenceStorageProto.PayloadReference value) {
        Optional<ConnectorPayloadOrigin> origin = value.hasConnectorOrigin()
            ? Optional.of(fromProto(value.getConnectorOrigin()))
            : Optional.empty();
        return new PayloadReference(
            value.getProvider(),
            value.getContainer(),
            value.getKey(),
            value.getContentType(),
            value.getCodec(),
            value.getChecksum(),
            value.getSizeBytes(),
            value.getVersion(),
            value.getMetadataMap(),
            origin);
    }

    private static ConnectorPayloadOrigin fromProto(PayloadReferenceStorageProto.ConnectorPayloadOrigin value) {
        Optional<ConnectorConfigurationSnapshot> configuration = value.getHasConfiguration()
            ? Optional.of(new ConnectorConfigurationSnapshot(
                value.getConfigurationSchemaId(),
                value.getConfigurationSchemaVersion(),
                value.getConfigurationDigest(),
                value.getConnectionReferencesList().stream().map(ConnectionRef::new).toList()))
            : Optional.empty();
        return new ConnectorPayloadOrigin(
            ConnectorBindingName.of(value.getBindingName()),
            new ConnectorOperationIdentity(
                ConnectorProviderId.of(value.getProviderId()),
                value.getOperationId(),
                ConnectorOperationKind.of(value.getOperationKind()),
                value.getOperationMajorVersion()),
            value.getProviderMajorVersion(),
            configuration);
    }

    private static void setIfPresent(String value, java.util.function.Consumer<String> setter) {
        if (value != null) {
            setter.accept(value);
        }
    }
}
