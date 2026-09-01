package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.ClassName;
import java.io.IOException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateDialect;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TypeMapping;

/** Resolves transport representations from the normalized v3 type graph rather than Java package conventions. */
final class CanonicalTransportBindingResolver {
    private final GenerationContext context;
    private final Optional<PipelineTemplateConfig> template;
    private final Optional<PipelineTemplateTypeModel> typeModel;

    CanonicalTransportBindingResolver(GenerationContext context) {
        this.context = context;
        this.template = loadTemplate(context);
        this.typeModel = context.canonicalTypeModel().or(() -> template.map(PipelineTemplateConfig::typeModel));
    }

    Optional<CanonicalTransportTypeBinding> resolve(TypeMapping mapping) {
        if (mapping == null || !(mapping.domainType() instanceof ClassName javaType)) {
            return Optional.empty();
        }
        PipelineTemplateTypeModel model = typeModel.orElse(null);
        if (model == null) {
            return Optional.empty();
        }
        String canonicalName = mapping.canonicalTypeName()
            .filter(model::contains)
            .orElseGet(() -> model.contains(javaType.simpleName()) ? javaType.simpleName() : "");
        if (canonicalName.isEmpty()) {
            return Optional.empty();
        }
        PipelineTemplateTypeDefinition definition = model.definition(canonicalName).orElse(null);
        if (!(definition instanceof PipelineTemplateTypeDefinition.RecordType record)) {
            return Optional.empty();
        }
        String basePackage = Optional.ofNullable(context.pipelineBasePackage())
            .filter(value -> !value.isBlank())
            .or(() -> template.map(PipelineTemplateConfig::basePackage))
            .orElse("");
        if (basePackage == null || basePackage.isBlank()) {
            return Optional.empty();
        }
        String suffix = shortHash(javaType.canonicalName());
        String mapperBase = canonicalName + "_" + suffix;
        return Optional.of(new CanonicalTransportTypeBinding(
            canonicalName,
            record,
            javaType,
            ClassName.get(basePackage + ".dto", canonicalName + "Dto"),
            ClassName.get(basePackage + ".transport.generated", mapperBase + "RestMapper"),
            ClassName.get(basePackage + ".grpc", "PipelineTypes", canonicalName),
            ClassName.get(basePackage + ".transport.generated", mapperBase + "GrpcMapper"),
            model.contributedTypeIdentity(canonicalName)));
    }

    static CanonicalTransportBindingPair resolveAndEnsure(
        GenerationContext context,
        PipelineStepModel model,
        PipelineTransport transport
    ) throws IOException {
        if (transport == PipelineTransport.LOCAL) {
            return CanonicalTransportBindingPair.empty();
        }
        return new CanonicalTransportBindingResolver(context)
            .resolveAndEnsure(model.inputMapping(), model.outputMapping(), transport);
    }

    private CanonicalTransportBindingPair resolveAndEnsure(
        TypeMapping inputMapping,
        TypeMapping outputMapping,
        PipelineTransport transport
    ) throws IOException {
        Optional<CanonicalTransportTypeBinding> input = resolve(inputMapping);
        Optional<CanonicalTransportTypeBinding> output = resolve(outputMapping);
        CanonicalTransportBindingPair resolved = new CanonicalTransportBindingPair(input, output);
        if (!resolved.any()) {
            return resolved;
        }
        CanonicalRecordTransportRenderer renderer = new CanonicalRecordTransportRenderer(context, this);
        for (CanonicalTransportTypeBinding binding : java.util.stream.Stream.concat(input.stream(), output.stream()).toList()) {
            if (transport == PipelineTransport.REST) {
                renderer.ensureRest(binding);
            } else {
                renderer.ensureGrpc(binding);
            }
        }
        return resolved;
    }

    Optional<PipelineTemplateTypeModel> typeModel() {
        return typeModel;
    }

    Optional<String> basePackage() {
        return Optional.ofNullable(context.pipelineBasePackage())
            .filter(value -> !value.isBlank())
            .or(() -> template.map(PipelineTemplateConfig::basePackage));
    }

    private static Optional<PipelineTemplateConfig> loadTemplate(GenerationContext context) {
        if (context.processingEnv() == null) {
            return Optional.empty();
        }
        Map<String, String> options = context.processingEnv().getOptions();
        String configuredPath = options == null ? null : options.get("pipeline.config");
        if (configuredPath == null || configuredPath.isBlank()) {
            return Optional.empty();
        }
        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(Path.of(configuredPath));
        return config.dialect() == PipelineTemplateDialect.V3 ? Optional.of(config) : Optional.empty();
    }

    private static String shortHash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required for deterministic generated mapper names", e);
        }
    }
}
