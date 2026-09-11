package org.pipelineframework.connector.http;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.connector.ConnectorOperationCallbackDescriptor;
import org.pipelineframework.connector.ConnectorOperationTypeContract;
import org.pipelineframework.connector.ConnectorProviderId;

/** Immutable correlated completion contract attached to one initiating Command. */
public record HttpCallbackPin(String id, String operation, int majorVersion,
    HttpCallbackInjectionTarget target, String method, String mediaType, HttpWireSchema requestSchema,
    String requestMappingKey, String inputType, HttpSecurityConstraint security,
    int acknowledgementStatus, boolean required, String sourceFingerprint) {

    public HttpCallbackPin {
        id = ConnectorProviderId.of(id).value();
        operation = ConnectorProviderId.of(operation).value();
        if (majorVersion < 1) throw new IllegalArgumentException("callback Command version must be positive");
        target = Objects.requireNonNull(target, "callback injection target");
        method = HttpParameterPin.requireText(method, "callback method").toUpperCase(Locale.ROOT);
        if (!"POST".equals(method)) throw new IllegalArgumentException("HTTP callbacks require POST");
        mediaType = HttpRequestBodyPin.mediaType(mediaType);
        requestSchema = Objects.requireNonNull(requestSchema, "callback wire schema");
        requestMappingKey = ConnectorProviderId.of(requestMappingKey).value();
        inputType = HttpParameterPin.requireText(inputType, "callback canonical input");
        security = Objects.requireNonNull(security, "callback security constraint");
        if (acknowledgementStatus < 200 || acknowledgementStatus >= 300) {
            throw new IllegalArgumentException("callback acknowledgement must be an exact 2xx status");
        }
        sourceFingerprint = HttpParameterPin.requireText(sourceFingerprint, "callback source fingerprint");
        if (!sourceFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("callback source fingerprint must be SHA-256 hex");
        }
    }

    public ConnectorOperationCallbackDescriptor descriptor() {
        return new ConnectorOperationCallbackDescriptor(id,
            new ConnectorOperationTypeContract(inputType, Optional.empty()), required);
    }

    public String fingerprint() {
        return HttpPinnedJson.sha256(HttpPinnedJson.canonicalize(toJson()));
    }

    public ObjectNode toJson() {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("id", id);
        node.put("operation", operation);
        node.put("majorVersion", majorVersion);
        node.set("target", target.toJson());
        node.put("method", method);
        node.put("mediaType", mediaType);
        node.set("requestSchema", requestSchema.node());
        node.put("requestSchemaFingerprint", requestSchema.sha256());
        node.put("requestMapping", requestMappingKey);
        node.put("input", inputType);
        node.set("security", security.toJson());
        node.put("acknowledgementStatus", acknowledgementStatus);
        node.put("required", required);
        node.put("sourceFingerprint", sourceFingerprint);
        return node;
    }
}
