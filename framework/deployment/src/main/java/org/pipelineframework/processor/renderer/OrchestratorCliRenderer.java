package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.lang.model.element.Modifier;

import com.google.protobuf.DescriptorProtos;
import com.squareup.javapoet.*;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.OrchestratorBinding;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.TransportMode;
import org.pipelineframework.processor.util.GrpcJavaTypeResolver;

/**
 * Generates an orchestrator CLI application for running pipelines locally.
 */
public class OrchestratorCliRenderer implements PipelineRenderer<OrchestratorBinding> {

    /**
     * Creates a new OrchestratorCliRenderer.
     */
    public OrchestratorCliRenderer() {
    }

    private static final String APP_CLASS = "OrchestratorApplication";

    @Override
    public GenerationTarget target() {
        return GenerationTarget.ORCHESTRATOR_CLI;
    }

    /**
     * Generates the OrchestratorApplication Java source that implements a CLI for running pipelines locally.
     *
     * <p>The generated class is written to the binding's orchestrator package and is wired with Quarkus, Picocli,
     * dependency injection, optional metrics/telemetry, and the PipelineExecutionService. Input deserialization,
     * transport mode (REST, LOCAL, or gRPC), and optional mapping between DTO/domain/gRPC types are resolved from
     * the provided binding and generation context.</p>
     *
     * @param binding configuration and metadata that determine package, input type, CLI name/description/version, and transport
     * @param ctx     generation context used for type resolution and for writing the generated source file
     * @throws IOException if writing the generated Java file to the processing environment's filer fails
     */
    @Override
    public void render(OrchestratorBinding binding, GenerationContext ctx) throws IOException {
        TransportMode transportMode = TransportMode.fromStringOptional(binding.normalizedTransport()).orElse(TransportMode.GRPC);
        boolean restMode = transportMode == TransportMode.REST;
        boolean localMode = transportMode == TransportMode.LOCAL;
        ClassName pipelineExecutionService = ClassName.get("org.pipelineframework", "PipelineExecutionService");
        ClassName pipelineInputDeserializer = ClassName.get("org.pipelineframework.util", "PipelineInputDeserializer");
        ClassName quarkusApplication = ClassName.get("io.quarkus.runtime", "QuarkusApplication");
        ClassName commandLine = ClassName.get("picocli", "CommandLine");
        ClassName command = ClassName.get("picocli.CommandLine", "Command");
        ClassName option = ClassName.get("picocli.CommandLine", "Option");
        ClassName duration = ClassName.get("java.time", "Duration");
        ClassName dependent = ClassName.get("jakarta.enterprise.context", "Dependent");
        ClassName inject = ClassName.get("jakarta.inject", "Inject");
        ClassName instance = ClassName.get("jakarta.enterprise.inject", "Instance");
        ClassName multi = ClassName.get("io.smallrye.mutiny", "Multi");
        ClassName appClassName = ClassName.get(binding.basePackage() + ".orchestrator", APP_CLASS);
        ClassName meterRegistry = ClassName.get("io.micrometer.core.instrument", "MeterRegistry");
        ClassName tags = ClassName.get("io.micrometer.core.instrument", "Tags");
        ClassName timer = ClassName.get("io.micrometer.core.instrument", "Timer");
        ClassName timerSample = timer.nestedClass("Sample");
        ClassName rpcMetrics = ClassName.get("org.pipelineframework.telemetry", "RpcMetrics");
        ClassName telemetryFlush = ClassName.get("org.pipelineframework.telemetry", "TelemetryFlush");
        ClassName grpcStatus = ClassName.get("io.grpc", "Status");
        ClassName grpcStatusCode = grpcStatus.nestedClass("Code");

        ClassName inputDtoType = ClassName.get(binding.basePackage() + ".common.dto", binding.inputTypeName() + "Dto");
        TypeName inputType;
        if (restMode) {
            inputType = inputDtoType;
        } else if (localMode) {
            inputType = resolveDomainInputType(binding);
        } else {
            inputType = resolveGrpcInputType(binding, ctx);
        }
        ParameterizedTypeName inputMultiType = ParameterizedTypeName.get(multi, inputType);

        FieldSpec inputField = FieldSpec.builder(String.class, "input", Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(option)
                .addMember("names", "{$S, $S}", "-i", "--input")
                .addMember("description", "$S", "JSON input value for the pipeline")
                .addMember("defaultValue", "$S", "")
                .build())
            .build();

        FieldSpec inputListField = FieldSpec.builder(String.class, "inputList", Modifier.PUBLIC)
            .addAnnotation(AnnotationSpec.builder(option)
                .addMember("names", "{$S}", "--input-list")
                .addMember("description", "$S", "JSON array input values for the pipeline")
                .addMember("defaultValue", "$S", "")
                .build())
            .build();

        FieldSpec pipelineExecutionServiceField = FieldSpec.builder(pipelineExecutionService, "pipelineExecutionService")
            .addAnnotation(inject)
            .build();

        FieldSpec meterRegistryField = FieldSpec.builder(ParameterizedTypeName.get(instance, meterRegistry), "meterRegistry")
            .addAnnotation(inject)
            .build();

        FieldSpec inputDeserializerField = FieldSpec.builder(pipelineInputDeserializer, "inputDeserializer")
            .addAnnotation(inject)
            .build();

        FieldSpec mapperField = null;
        if (!restMode) {
            ClassName mapperType = ClassName.get(binding.basePackage() + ".common.mapper", binding.inputTypeName() + "Mapper");
            mapperField = FieldSpec.builder(mapperType, lowerCamel(mapperType.simpleName()))
                .addAnnotation(inject)
                .build();
        }

        MethodSpec mainMethod = MethodSpec.methodBuilder("main")
            .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
            .returns(void.class)
            .addParameter(String[].class, "args")
            .addStatement("$T.run($T.class, args)", ClassName.get("io.quarkus.runtime", "Quarkus"), appClassName)
            .build();

        MethodSpec runMethod = MethodSpec.methodBuilder("run")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(int.class)
            .addParameter(String[].class, "args")
            .addStatement("return new $T(this).execute(args)", commandLine)
            .build();

        String mapperName = mapperField == null ? null : mapperField.name;
        String mapperMethod = localMode ? "fromDto" : "toGrpc";
        String mapSuffix = mapperName == null ? "" : ".map(" + mapperName + "::" + mapperMethod + ")";
        String uniToMultiSuffix = ".toMulti()";

        MethodSpec callMethod = MethodSpec.methodBuilder("call")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Integer.class)
            .addCode("""
                String actualInputList = firstNonBlank(inputList, System.getenv("PIPELINE_INPUT_LIST"));
                String actualInput = firstNonBlank(input, System.getenv("PIPELINE_INPUT"));
                if (isBlank(actualInputList) && isBlank(actualInput)) {
                    System.out.println("Input parameter is empty");
                    return $T.ExitCode.USAGE;
                }

                $T inputMulti;
                try {
                    if (!isBlank(actualInputList)) {
                        if (!looksLikeJsonArray(actualInputList)) {
                            System.err.println("Input list must be a JSON array.");
                            return $T.ExitCode.USAGE;
                        }
                        inputMulti = inputDeserializer
                            .multiFromJsonList(actualInputList, $T.class)%s;
                    } else if (looksLikeJsonObject(actualInput)) {
                        inputMulti = inputDeserializer
                            .uniFromJson(actualInput, $T.class)%s%s;
                    } else {
                        System.err.println("Input must be a JSON object.");
                        return $T.ExitCode.USAGE;
                    }
                } catch (Exception e) {
                    System.err.println("Failed to deserialize input JSON: " + sanitizeErrorMessage(e.getMessage()));
                    return $T.ExitCode.USAGE;
                }

                pipelineExecutionService.awaitStartupHealth($T.ofMinutes(2));

                boolean hasRegistry = !meterRegistry.isUnsatisfied();
                $T registry = hasRegistry ? meterRegistry.get() : null;
                $T grpcTags = hasRegistry ? $T.of($S, $S, $S, $S) : null;
                $T sample = hasRegistry ? $T.start(registry) : null;
                long startTime = System.nanoTime();
                $T statusCode = $T.OK;
                String grpcStatus = "0";
                try {
                    pipelineExecutionService.executePipeline(inputMulti)
                            .collect().asList()
                            .await().indefinitely();

                    System.out.println("Pipeline execution completed");
                    return $T.ExitCode.OK;
                } catch (Exception e) {
                    statusCode = $T.UNKNOWN;
                    grpcStatus = "2";
                    throw e;
                } finally {
                    $T.recordGrpcServer($S, $S, statusCode, System.nanoTime() - startTime);
                    if (hasRegistry) {
                        $T allTags = grpcTags.and($S, grpcStatus);
                        registry.counter($S, allTags).increment();
                        sample.stop(registry.timer($S, allTags));
                    }
                    $T.flush();
                }
                """.formatted(mapSuffix, mapSuffix, uniToMultiSuffix),
                commandLine,
                inputMultiType,
                commandLine,
                inputDtoType,
                inputDtoType,
                commandLine,
                commandLine,
                duration,
                meterRegistry,
                tags,
                tags,
                "service",
                "OrchestratorService",
                "method",
                "Run",
                timerSample,
                timer,
                grpcStatusCode,
                grpcStatusCode,
                commandLine,
                grpcStatusCode,
                rpcMetrics,
                "OrchestratorService",
                "Run",
                tags,
                "grpc.status",
                "grpc.server.requests.received",
                "grpc.server.processing.duration",
                telemetryFlush)
            .build();

        MethodSpec isBlankMethod = MethodSpec.methodBuilder("isBlank")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(boolean.class)
            .addParameter(String.class, "value")
            .addStatement("return value == null || value.trim().isEmpty()")
            .build();

        MethodSpec firstNonBlankMethod = MethodSpec.methodBuilder("firstNonBlank")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(String.class)
            .addParameter(String.class, "primary")
            .addParameter(String.class, "fallback")
            .addStatement("return isBlank(primary) ? fallback : primary")
            .build();

        MethodSpec looksLikeJsonObjectMethod = MethodSpec.methodBuilder("looksLikeJsonObject")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(boolean.class)
            .addParameter(String.class, "value")
            .addStatement("if (isBlank(value)) return false")
            .addStatement("String trimmed = value.trim()")
            .addStatement("return trimmed.startsWith(\"{\")")
            .build();

        MethodSpec looksLikeJsonArrayMethod = MethodSpec.methodBuilder("looksLikeJsonArray")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(boolean.class)
            .addParameter(String.class, "value")
            .addStatement("if (isBlank(value)) return false")
            .addStatement("String trimmed = value.trim()")
            .addStatement("return trimmed.startsWith(\"[\")")
            .build();

        MethodSpec sanitizeErrorMessageMethod = MethodSpec.methodBuilder("sanitizeErrorMessage")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(String.class)
            .addParameter(String.class, "message")
            .addStatement("if (message == null) return \"Unknown error\"")
            .addStatement("return message.replaceAll(\"[\\\\r\\\\n\\\\t]\", \" \").trim()")
            .build();

        String cliName = binding.cliName() == null || binding.cliName().isBlank() ? "orchestrator" : binding.cliName();
        String cliDescription = binding.cliDescription() == null || binding.cliDescription().isBlank()
            ? "Pipeline Orchestrator CLI"
            : binding.cliDescription();
        String cliVersion = binding.cliVersion() == null || binding.cliVersion().isBlank()
            ? "1.0.0"
            : binding.cliVersion();

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(APP_CLASS)
            .addModifiers(Modifier.PUBLIC)
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get("java.util.concurrent", "Callable"),
                ClassName.get(Integer.class)))
            .addSuperinterface(quarkusApplication)
            .addAnnotation(AnnotationSpec.builder(command)
                .addMember("name", "$S", cliName)
                .addMember("mixinStandardHelpOptions", "true")
                .addMember("version", "$S", cliVersion)
                .addMember("description", "$S", cliDescription)
                .build())
            .addAnnotation(dependent)
            .addField(inputField)
            .addField(inputListField)
            .addField(pipelineExecutionServiceField)
            .addField(meterRegistryField)
            .addField(inputDeserializerField)
            .addMethod(mainMethod)
            .addMethod(runMethod)
            .addMethod(callMethod)
            .addMethod(isBlankMethod)
            .addMethod(firstNonBlankMethod)
            .addMethod(looksLikeJsonObjectMethod)
            .addMethod(looksLikeJsonArrayMethod)
            .addMethod(sanitizeErrorMessageMethod);

        if (mapperField != null) {
            typeBuilder.addField(mapperField);
        }

        TypeSpec app = typeBuilder.build();

        JavaFile.builder(binding.basePackage() + ".orchestrator", app)
            .build()
            .writeTo(ctx.processingEnv().getFiler());
    }

    /**
     * Resolve the gRPC Java input TypeName for the orchestrator's first pipeline step.
     *
     * @param binding orchestrator binding containing configuration (including the first step service name and base package)
     * @param ctx generation context providing the protobuf descriptor set and processing environment
     * @return the resolved gRPC parameter TypeName for the first step
     * @throws IllegalStateException if the protobuf descriptor set is not available, if the first step service name is missing or blank, or if the gRPC input type cannot be resolved from the descriptors
     */
    private TypeName resolveGrpcInputType(OrchestratorBinding binding, GenerationContext ctx) {
        DescriptorProtos.FileDescriptorSet descriptorSet = ctx.descriptorSet();
        if (descriptorSet == null) {
            throw new IllegalStateException("No protobuf descriptor set available for orchestrator CLI generation.");
        }
        if (binding.firstStepServiceName() == null || binding.firstStepServiceName().isBlank()) {
            throw new IllegalStateException("Missing first step service name for orchestrator CLI generation.");
        }
        PipelineStepModel firstStepModel = new PipelineStepModel(
            binding.firstStepServiceName(),
            binding.firstStepServiceName(),
            binding.basePackage() + ".service",
            ClassName.get(binding.basePackage() + ".service", binding.firstStepServiceName()),
            null,
            null,
            binding.firstStepStreamingShape(),
            java.util.Set.of(GenerationTarget.GRPC_SERVICE),
            org.pipelineframework.processor.ir.ExecutionMode.DEFAULT,
            org.pipelineframework.processor.ir.DeploymentRole.ORCHESTRATOR_CLIENT,
            false,
            null
        );

        org.pipelineframework.processor.util.GrpcBindingResolver resolver =
            new org.pipelineframework.processor.util.GrpcBindingResolver();
        var grpcBinding = resolver.resolve(firstStepModel, descriptorSet);
        GrpcJavaTypeResolver typeResolver = new GrpcJavaTypeResolver();
        var grpcTypes = typeResolver.resolve(grpcBinding, ctx.processingEnv().getMessager());
        if (grpcTypes.grpcParameterType() == null) {
            throw new IllegalStateException("Failed to resolve orchestrator gRPC input type from descriptors.");
        }
        return grpcTypes.grpcParameterType();
    }

    /**
     * Produce the TypeName that refers to the domain input class for the given binding.
     *
     * <p>The returned TypeName points to the class named by binding.inputTypeName() inside
     * the package "{basePackage}.common.domain".</p>
     *
     * @param binding the orchestrator binding containing basePackage() and inputTypeName()
     * @return the TypeName of the domain input class (package: basePackage + ".common.domain", class: inputTypeName)
     */
    private TypeName resolveDomainInputType(OrchestratorBinding binding) {
        return ClassName.get(binding.basePackage() + ".common.domain", binding.inputTypeName());
    }

    /**
     * Convert a string to lower camel case by lowercasing its first character.
     *
     * @param name the input string
     * @return the input with its first character lowercased; returns the original value if it is null or empty
     */
    private String lowerCamel(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }
}