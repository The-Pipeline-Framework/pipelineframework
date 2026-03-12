package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.lang.model.element.Modifier;
import org.pipelineframework.config.connector.ConnectorConfig;

/**
 * Generates CDI bootstrap beans for declared framework connectors.
 */
public class ConnectorBootstrapRenderer {

    public ClassName render(ConnectorConfig connector, String basePackage, GenerationContext ctx) throws IOException {
        String packageName = basePackage + ".connector";
        String className = toPascalCase(connector.name()) + "ConnectorBridge";
        ClassName generatedType = ClassName.get(packageName, className);

        ClassName outputBus = ClassName.get("org.pipelineframework", "PipelineOutputBus");
        ClassName startupEvent = ClassName.get("io.quarkus.runtime", "StartupEvent");
        ClassName observes = ClassName.get("jakarta.enterprise.event", "Observes");
        ClassName applicationScoped = ClassName.get("jakarta.enterprise.context", "ApplicationScoped");
        ClassName unremovable = ClassName.get("io.quarkus.arc", "Unremovable");
        ClassName preDestroy = ClassName.get("jakarta.annotation", "PreDestroy");
        ClassName cancellable = ClassName.get("io.smallrye.mutiny.subscription", "Cancellable");
        ClassName logger = ClassName.get("org.jboss.logging", "Logger");
        ClassName connectorRuntime = ClassName.get("org.pipelineframework.connector", "ConnectorRuntime");
        ClassName connectorRecord = ClassName.get("org.pipelineframework.connector", "ConnectorRecord");
        ClassName connectorPolicy = ClassName.get("org.pipelineframework.connector", "ConnectorPolicy");
        ClassName connectorBackpressure = ClassName.get("org.pipelineframework.connector", "ConnectorBackpressurePolicy");
        ClassName connectorIdempotency = ClassName.get("org.pipelineframework.connector", "ConnectorIdempotencyPolicy");
        ClassName connectorFailureMode = ClassName.get("org.pipelineframework.connector", "ConnectorFailureMode");
        ClassName connectorSupport = ClassName.get("org.pipelineframework.connector", "ConnectorSupport");
        ClassName connectorTracker = ClassName.get("org.pipelineframework.connector", "ConnectorIdempotencyTracker");
        ClassName outputBusSource = ClassName.get("org.pipelineframework.connector", "OutputBusConnectorSource");
        ClassName configProperty = ClassName.get("org.eclipse.microprofile.config.inject", "ConfigProperty");
        ClassName list = ClassName.get(List.class);
        ClassName map = ClassName.get(Map.class);

        ClassName sourceType = ClassName.bestGuess(connector.source().type());
        ClassName targetType = ClassName.bestGuess(connector.target().type());
        ClassName targetAdapterType = ClassName.bestGuess(connector.target().adapter());
        ClassName mapperType = connector.mapper() == null || connector.mapper().isBlank()
            ? null
            : ClassName.bestGuess(connector.mapper());
        if (mapperType == null && !sourceType.equals(targetType)) {
            throw new IllegalStateException(
                "Connector '" + connector.name() + "' requires a mapper when source and target types differ");
        }

        FieldSpec logField = FieldSpec.builder(logger, "LOG", Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
            .initializer("$T.getLogger($L.class)", logger, className)
            .build();
        FieldSpec outputBusField = FieldSpec.builder(outputBus, "outputBus", Modifier.PRIVATE, Modifier.FINAL).build();
        FieldSpec targetField = FieldSpec.builder(targetAdapterType, "targetAdapter", Modifier.PRIVATE, Modifier.FINAL).build();
        FieldSpec enabledField = FieldSpec.builder(TypeName.BOOLEAN, "enabled", Modifier.PRIVATE, Modifier.FINAL).build();
        FieldSpec subscriptionField = FieldSpec.builder(cancellable, "forwardingSubscription", Modifier.PRIVATE).build();

        TypeSpec.Builder typeBuilder = TypeSpec.classBuilder(className)
            .addModifiers(Modifier.PUBLIC)
            .addAnnotation(applicationScoped)
            .addAnnotation(unremovable)
            .addField(logField)
            .addField(outputBusField)
            .addField(targetField)
            .addField(enabledField)
            .addField(subscriptionField);

        if (mapperType != null) {
            typeBuilder.addField(FieldSpec.builder(mapperType, "mapper", Modifier.PRIVATE, Modifier.FINAL).build());
        }

        MethodSpec.Builder ctor = MethodSpec.constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(outputBus, "outputBus")
            .addParameter(targetAdapterType, "targetAdapter");
        if (mapperType != null) {
            ctor.addParameter(mapperType, "mapper");
        }
        ctor.addParameter(com.squareup.javapoet.ParameterSpec.builder(TypeName.BOOLEAN, "enabled")
            .addAnnotation(AnnotationSpec.builder(configProperty)
                .addMember("name", "$S", "tpf.connector." + connector.name() + ".enabled")
                .addMember("defaultValue", "$S", String.valueOf(connector.enabled()))
                .build())
            .build());
        ctor.addStatement("this.outputBus = outputBus");
        ctor.addStatement("this.targetAdapter = targetAdapter");
        if (mapperType != null) {
            ctor.addStatement("this.mapper = mapper");
        }
        ctor.addStatement("this.enabled = enabled");
        typeBuilder.addMethod(ctor.build());

        CodeBlock trackerInit = "DISABLED".equals(connector.idempotency())
            ? CodeBlock.of("null")
            : CodeBlock.of("new $T($L)", connectorTracker, connector.idempotencyMaxKeys());

        MethodSpec onStartup = MethodSpec.methodBuilder("onStartup")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(com.squareup.javapoet.ParameterSpec.builder(startupEvent, "ignored")
                .addAnnotation(observes)
                .build())
            .beginControlFlow("if (!enabled)")
            .addStatement("LOG.info($S)", "Connector '" + connector.name() + "' disabled via config")
            .addStatement("return")
            .endControlFlow()
            .addStatement(
                "var runtime = new $T<$T, $T>($S, new $T<>(outputBus, $T.class), targetAdapter, this::mapRecord, "
                    + "new $T(true, $T.$L, $L, $T.$L, $T.$L), $L, rejected -> {}, duplicate -> LOG.debugf($S, duplicate == null ? null : duplicate.idempotencyKey()), failure -> LOG.error($S, failure))",
                connectorRuntime,
                sourceType,
                targetType,
                connector.name(),
                outputBusSource,
                sourceType,
                connectorPolicy,
                connectorBackpressure,
                connector.backpressure(),
                connector.backpressureBufferCapacity(),
                connectorIdempotency,
                connector.idempotency(),
                connectorFailureMode,
                connector.failureMode(),
                trackerInit,
                "Dropped duplicate connector handoff " + connector.name() + " idempotencyKey=%s",
                "Connector '" + connector.name() + "' failed")
            .addStatement("forwardingSubscription = runtime.start()")
            .addStatement("LOG.infof($S, $S)", "Connector %s started", connector.name())
            .build();

        MethodSpec onShutdown = MethodSpec.methodBuilder("onShutdown")
            .addAnnotation(preDestroy)
            .addModifiers(Modifier.PUBLIC)
            .beginControlFlow("if (forwardingSubscription != null)")
            .addStatement("forwardingSubscription.cancel()")
            .endControlFlow()
            .build();

        MethodSpec.Builder mapRecord = MethodSpec.methodBuilder("mapRecord")
            .addModifiers(Modifier.PRIVATE)
            .returns(ParameterizedTypeName.get(connectorRecord, targetType))
            .addParameter(ParameterizedTypeName.get(connectorRecord, sourceType), "sourceRecord");

        if (mapperType != null) {
            mapRecord.addStatement("$T mapped = mapper.map(sourceRecord.payload())", targetType);
        } else {
            mapRecord.addStatement("$T mapped = sourceRecord.payload()", targetType);
        }
        mapRecord.beginControlFlow("if (mapped == null)")
            .addStatement("return null")
            .endControlFlow()
            .addStatement(
                "return $T.ofPayload(mapped, $T.ensureDispatchMetadata(sourceRecord.dispatchMetadata(), $S, mapped, $T.of($L)), $T.of($S, $S, $S, $S, $S, $S, $S, $S))",
                connectorRecord,
                connectorSupport,
                connector.name(),
                list,
                joinQuoted(connector.idempotencyKeyFields()),
                map,
                "connector.name",
                connector.name(),
                "connector.source.step",
                safe(connector.source().step()),
                "connector.target.pipeline",
                safe(connector.target().pipeline()),
                "connector.contract",
                connector.target().type());

        typeBuilder.addMethod(onStartup);
        typeBuilder.addMethod(onShutdown);
        typeBuilder.addMethod(mapRecord.build());

        JavaFile.builder(packageName, typeBuilder.build())
            .build()
            .writeTo(ctx.processingEnv().getFiler());
        return generatedType;
    }

    private static String safe(String value) {
        return value == null ? "" : value;
    }

    private static String toPascalCase(String value) {
        if (value == null || value.isBlank()) {
            return "Generated";
        }
        StringBuilder builder = new StringBuilder();
        for (String part : value.split("[^a-zA-Z0-9]+")) {
            if (part.isBlank()) {
                continue;
            }
            String normalizedPart = part.toLowerCase(Locale.ROOT);
            builder.append(Character.toUpperCase(normalizedPart.charAt(0)));
            if (normalizedPart.length() > 1) {
                builder.append(normalizedPart.substring(1));
            }
        }
        return builder.toString();
    }

    private static CodeBlock joinQuoted(List<String> values) {
        if (values == null || values.isEmpty()) {
            return CodeBlock.of("");
        }
        CodeBlock.Builder builder = CodeBlock.builder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                builder.add(", ");
            }
            builder.add("$S", values.get(i));
        }
        return builder.build();
    }
}
