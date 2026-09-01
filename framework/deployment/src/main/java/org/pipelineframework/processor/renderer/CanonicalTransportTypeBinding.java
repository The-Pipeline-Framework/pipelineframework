package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.protocol.ProtocolTypeIdentity;
import org.pipelineframework.processor.ir.PipelineTransport;

/** Compiler-owned binding from one normalized v3 record to its Java and transport representations. */
record CanonicalTransportTypeBinding(
    String canonicalName,
    PipelineTemplateTypeDefinition.RecordType definition,
    ClassName javaType,
    ClassName restDtoType,
    ClassName restMapperType,
    ClassName grpcType,
    ClassName grpcMapperType,
    Optional<ProtocolTypeIdentity> contributedIdentity
) {
    CanonicalTransportTypeBinding {
        Objects.requireNonNull(canonicalName, "canonicalName must not be null");
        Objects.requireNonNull(definition, "definition must not be null");
        Objects.requireNonNull(javaType, "javaType must not be null");
        Objects.requireNonNull(restDtoType, "restDtoType must not be null");
        Objects.requireNonNull(restMapperType, "restMapperType must not be null");
        Objects.requireNonNull(grpcType, "grpcType must not be null");
        Objects.requireNonNull(grpcMapperType, "grpcMapperType must not be null");
        contributedIdentity = Objects.requireNonNull(contributedIdentity, "contributedIdentity must not be null");
    }

    boolean contributed() {
        return contributedIdentity.isPresent();
    }

    ClassName transportType(PipelineTransport transport) {
        return transport == PipelineTransport.REST ? restDtoType : grpcType;
    }

    ClassName mapperType(PipelineTransport transport) {
        return transport == PipelineTransport.REST ? restMapperType : grpcMapperType;
    }
}
