package org.pipelineframework.connector.openapi.maven;

import java.util.List;
import java.util.Map;

/** Reviewable authority choices for one release-time OpenAPI import. */
public final class OpenApiImportConfiguration {
    public int schemaVersion;
    public String importId;
    public Source source;
    public List<OperationSelection> operations = List.of();

    public static final class Source {
        public String snapshot;
        public String closureSha256;
    }

    public static final class OperationSelection {
        public SourceOperation source;
        public String operation;
        public int version;
        public String kind;
        public String input;
        public String output;
        public String server;
        public SecuritySelection security;
        public RequestSelection request;
        public List<ResponseSelection> responses = List.of();
        public ProviderIdempotencyKey providerIdempotencyKey;
        public List<CallbackSelection> callbacks = List.of();
    }

    public static final class CallbackSelection {
        public SourceCallback source;
        public String callback;
        public String input;
        public RequestSelection request;
        public AcknowledgementSelection acknowledgement;
        public SecuritySelection security;
    }

    public static final class SourceCallback {
        public String name;
        public String expression;
        public String operationId;
        public String method;
    }

    public static final class AcknowledgementSelection {
        public String status;
    }

    public static final class SourceOperation {
        public String operationId;
        public String method;
        public String path;
    }

    public static final class SecuritySelection {
        public boolean none;
        public Map<String, List<String>> require = Map.of();
    }

    public static final class RequestSelection {
        public String mediaType;
        public String representation;
        public String bodyPath;
        public Map<String, String> parameterSources = Map.of();
    }

    public static final class ResponseSelection {
        public String status;
        public String mediaType;
        public String outcome;
        public String representation;
        public String code;
        public String confirmation;
    }

    public static final class ProviderIdempotencyKey {
        public String location;
        public String name;
    }
}
