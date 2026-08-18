package org.pipelineframework.connector;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.protocol.ProtocolTypeContributor;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

/** Portable connector-owned result vocabulary for dynamic operation invocation boundaries. */
public final class ConnectorProtocolTypeContributor implements ProtocolTypeContributor {
    public static final ProtocolTypeIdentity OPERATION_OBSERVATION =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.connector"), "OperationObservation");
    public static final ProtocolTypeIdentity RESULT_OBSERVATION =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.connector"), "OperationResultObservation");
    public static final ProtocolTypeIdentity EMPTY_OBSERVATION =
        new ProtocolTypeIdentity(ConnectorProviderId.of("tpf.connector"), "OperationEmptyObservation");

    @Override
    public Collection<ProtocolTypeDescriptor> protocolTypes() {
        PipelineTemplateTypeReference.Scalar string = new PipelineTemplateTypeReference.Scalar("string");
        PipelineTemplateTypeReference.Scalar int32 = new PipelineTemplateTypeReference.Scalar("int32");
        List<PipelineTemplateTypeDefinition.Field> identity = List.of(
            new PipelineTemplateTypeDefinition.Field("binding", string),
            new PipelineTemplateTypeDefinition.Field("operation", string),
            new PipelineTemplateTypeDefinition.Field("kind", string),
            new PipelineTemplateTypeDefinition.Field("operationVersion", int32),
            new PipelineTemplateTypeDefinition.Field("outcome", string),
            new PipelineTemplateTypeDefinition.Field("code", string));
        List<PipelineTemplateTypeDefinition.Field> result = new java.util.ArrayList<>(identity);
        result.add(new PipelineTemplateTypeDefinition.Field("resultType", string));
        result.add(new PipelineTemplateTypeDefinition.Field("resultJson", string));
        return List.of(
            new ProtocolTypeDescriptor(RESULT_OBSERVATION,
                new PipelineTemplateTypeDefinition.RecordType("OperationResultObservation", result)),
            new ProtocolTypeDescriptor(EMPTY_OBSERVATION,
                new PipelineTemplateTypeDefinition.RecordType("OperationEmptyObservation", identity)),
            new ProtocolTypeDescriptor(OPERATION_OBSERVATION,
                new PipelineTemplateTypeDefinition.UnionType("OperationObservation", Map.of(
                    "result", new PipelineTemplateTypeDefinition.Variant("result",
                        new PipelineTemplateTypeReference.Contributed(RESULT_OBSERVATION.qualifiedName())),
                    "empty", new PipelineTemplateTypeDefinition.Variant("empty",
                        new PipelineTemplateTypeReference.Contributed(EMPTY_OBSERVATION.qualifiedName()))))));
    }
}
