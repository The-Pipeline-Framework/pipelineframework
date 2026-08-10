package org.pipelineframework.processor.renderer;

import java.nio.file.Path;
import java.util.Map;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.TypeName;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;

/** Shared typed descriptor emission for normal Await steps and stream continuations. */
final class AwaitDescriptorCodegen {
    private AwaitDescriptorCodegen() {
    }

    static Binding resolve(PipelineStepModel model, GenerationContext ctx) {
        PipelineConfigHints hints = resolveConfigHints(ctx);
        ClientRepresentation representation = ClientRepresentation.forPipelineTransport(hints.transportMode());
        TypeName transportInput = clientStepType(model.inboundDomainType(), representation, hints.basePackage());
        TypeName transportOutput = clientStepType(model.outboundDomainType(), representation, hints.basePackage());
        V3GeneratedDomainBinding.RepresentationBoundary boundary = V3GeneratedDomainBinding.resolveAwait(
            model, transportInput, transportOutput, hints.basePackage(), ctx.v3GeneratedDomainTypes());
        CodeBlock invocation = boundary.convertsAtBoundary()
            ? CodeBlock.of(
                "descriptorFactory.descriptor($S, $T.class.getName(), $T.class.getName(), $T.class.getName(), $T.class.getName(), value -> $T.toProto(($T) value), value -> $T.fromProto(($T) value))",
                model.serviceName(), boundary.stepInputType(), boundary.stepOutputType(), boundary.transportInputType(),
                boundary.transportOutputType(), boundary.adaptersOrThrow(), boundary.stepInputType(),
                boundary.adaptersOrThrow(), boundary.transportOutputType())
            : CodeBlock.of("descriptorFactory.descriptor($S, $S, $S)", model.serviceName(),
                boundary.stepInputType().toString(), boundary.stepOutputType().toString());
        return new Binding(boundary, invocation);
    }

    record Binding(V3GeneratedDomainBinding.RepresentationBoundary boundary, CodeBlock descriptorInvocation) {
    }

    private static PipelineConfigHints resolveConfigHints(GenerationContext ctx) {
        Map<String, String> options = ctx.processingEnv() == null ? Map.of() : ctx.processingEnv().getOptions();
        PipelineTransport configured = PipelineTransport.fromStringOptional(options.get("pipeline.transport")).orElse(null);
        String basePackage = ctx.pipelineBasePackage();
        String configPath = options.get("pipeline.config");
        if (configPath != null && !configPath.isBlank()) {
            try {
                PipelineYamlConfig config = new PipelineYamlConfigLoader(ctx.processingEnv().getOptions()::get, System::getenv)
                    .load(Path.of(configPath));
                if (configured == null) {
                    configured = PipelineTransport.fromString(config.transport());
                }
                if (config.basePackage() != null && !config.basePackage().isBlank()) {
                    basePackage = config.basePackage();
                }
            } catch (RuntimeException error) {
                throw new IllegalStateException("Failed to load pipeline config at '" + configPath + "'", error);
            }
        }
        return new PipelineConfigHints(configured == null
            ? (ctx.transportMode() == null ? PipelineTransport.GRPC : ctx.transportMode()) : configured, basePackage);
    }

    private static TypeName clientStepType(TypeName domainType, ClientRepresentation representation, String pipelineBasePackage) {
        if (!(domainType instanceof ClassName className)) {
            return domainType;
        }
        String packageName = className.packageName();
        String basePackage = packageName;
        if ((packageName.endsWith(".common.domain") || packageName.endsWith(".common.dto") || packageName.endsWith(".domain"))
            && pipelineBasePackage != null && !pipelineBasePackage.isBlank()) {
            basePackage = pipelineBasePackage;
        } else if (packageName.endsWith(".common.domain")) {
            basePackage = packageName.substring(0, packageName.length() - ".common.domain".length());
        } else if (packageName.endsWith(".common.dto")) {
            basePackage = packageName.substring(0, packageName.length() - ".common.dto".length());
        } else if (packageName.endsWith(".domain")) {
            basePackage = packageName.substring(0, packageName.length() - ".domain".length());
        } else if (packageName.endsWith(".service")) {
            basePackage = packageName.substring(0, packageName.length() - ".service".length());
        }
        return switch (representation) {
            case CANONICAL -> className;
            case REST_DTO -> ClassName.get(basePackage + ".common.dto", className.simpleName() + "Dto");
            case PROTOBUF -> ClassName.get(basePackage + ".grpc", "PipelineTypes", className.simpleName());
        };
    }

    private record PipelineConfigHints(PipelineTransport transportMode, String basePackage) {
    }

    private enum ClientRepresentation {
        CANONICAL, REST_DTO, PROTOBUF;
        static ClientRepresentation forPipelineTransport(PipelineTransport transport) {
            return switch (transport) {
                case LOCAL -> CANONICAL;
                case REST -> REST_DTO;
                case GRPC -> PROTOBUF;
            };
        }
    }
}
