package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Modifier;
import javax.tools.StandardLocation;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.config.boundary.PipelineObjectSelectionConfig;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.objectingest.ObjectSelectionMapper;
import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.processor.PipelineStepProcessor;

/** Generates the typed projection used by grouped Object Ingest. */
public final class ObjectSelectionMapperRenderer {
    private static final String CLASS_NAME = "ObjectSelectionPipelineInputMapper";
    private static final String SERVICE_PATH = "META-INF/services/" + ObjectSelectionMapper.class.getName();

    public ClassName render(String basePackage, ClassName outputType,
                            PipelineTemplateTypeDefinition.RecordType record,
                            PipelineObjectSelectionConfig selection, GenerationContext ctx) throws IOException {
        validate(record, selection);
        String packageName = basePackage + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX;
        ClassName mapperClass = ClassName.get(packageName, CLASS_NAME);
        TypeSpec type = TypeSpec.classBuilder(CLASS_NAME)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addSuperinterface(ParameterizedTypeName.get(ClassName.get(ObjectSelectionMapper.class), outputType))
            .addMethod(MethodSpec.methodBuilder("outputType")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Class.class), outputType))
                .addStatement("return $T.class", outputType)
                .build())
            .addMethod(mapMethod(outputType, record, selection))
            .addMethod(referenceMethod())
            .addMethod(snapshotReferenceMethod())
            .build();
        JavaFile javaFile = JavaFile.builder(packageName, type).build();
        if (ctx.processingEnv() != null) {
            javaFile.writeTo(ctx.processingEnv().getFiler());
            writeServiceDescriptor(ctx.processingEnv().getFiler(), mapperClass.canonicalName());
        } else {
            javaFile.writeTo(ctx.outputDir());
            writeServiceDescriptor(ctx.outputDir(), mapperClass.canonicalName());
        }
        return mapperClass;
    }

    private static MethodSpec mapMethod(ClassName outputType, PipelineTemplateTypeDefinition.RecordType record,
                                        PipelineObjectSelectionConfig selection) {
        CodeBlock.Builder constructor = CodeBlock.builder().add("return new $T(", outputType);
        for (int index = 0; index < record.fields().size(); index++) {
            if (index > 0) {
                constructor.add(", ");
            }
            String field = record.fields().get(index).name();
            if (selection.into().filter(field::equals).isPresent()) {
                constructor.add("snapshots.stream().map(snapshot -> reference(snapshot, snapshot.key())).toList()");
            } else {
                constructor.add("reference(snapshots, $S)", selection.keys().get(field));
            }
        }
        constructor.add(");\n");
        return MethodSpec.methodBuilder("map")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(outputType)
            .addParameter(ParameterizedTypeName.get(ClassName.get(List.class), ClassName.get(ObjectSnapshot.class)),
                "snapshots")
            .addCode(constructor.build())
            .build();
    }

    private static MethodSpec referenceMethod() {
        return MethodSpec.methodBuilder("reference")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(ClassName.bestGuess("org.pipelineframework.repository.PayloadReference"))
            .addParameter(ParameterizedTypeName.get(ClassName.get(List.class), ClassName.get(ObjectSnapshot.class)),
                "snapshots")
            .addParameter(String.class, "key")
            .addCode("""
                java.util.List<org.pipelineframework.objectingest.ObjectSnapshot> matches = snapshots.stream()
                    .filter(snapshot -> key.equals(snapshot.key()))
                    .toList();
                if (matches.size() != 1) {
                    throw new IllegalStateException("Object selection requires exactly one snapshot for key '" + key
                        + "' but found " + matches.size());
                }
                return reference(matches.getFirst(), key);
                """)
            .build();
    }

    private static MethodSpec snapshotReferenceMethod() {
        return MethodSpec.methodBuilder("reference")
            .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
            .returns(ClassName.bestGuess("org.pipelineframework.repository.PayloadReference"))
            .addParameter(ObjectSnapshot.class, "snapshot")
            .addParameter(String.class, "key")
            .addCode("""
                if (snapshot.contentRef() == null) {
                    throw new IllegalStateException("Object selection key '" + key + "' has no payload reference");
                }
                return snapshot.contentRef();
                """)
            .build();
    }

    private static void validate(PipelineTemplateTypeDefinition.RecordType record,
                                 PipelineObjectSelectionConfig selection) {
        if (record.fields().isEmpty() || record.fields().stream().anyMatch(field -> !"payload_ref".equals(field.type().name()))) {
            throw new IllegalStateException("Grouped Object Ingest type '" + record.name()
                + "' must contain payload_ref fields only");
        }
        List<String> fields = record.fields().stream().map(PipelineTemplateTypeDefinition.Field::name).toList();
        if (selection.into().isPresent()) {
            if (record.fields().size() != 1
                || !record.fields().getFirst().repeated()
                || !fields.getFirst().equals(selection.into().orElseThrow())) {
                throw new IllegalStateException("input.object.selection.into must name the sole repeated payload_ref field of '"
                    + record.name() + "'");
            }
        } else if (!selection.keys().keySet().equals(new java.util.LinkedHashSet<>(fields))) {
            throw new IllegalStateException("input.object.selection.keys must map every field of '" + record.name() + "'");
        }
    }

    private static void writeServiceDescriptor(Filer filer, String mapperClassName) throws IOException {
        try (Writer writer = filer.createResource(StandardLocation.CLASS_OUTPUT, "", SERVICE_PATH).openWriter()) {
            writer.write(mapperClassName);
            writer.write(System.lineSeparator());
        }
    }

    private static void writeServiceDescriptor(Path outputDir, String mapperClassName) throws IOException {
        Path servicePath = outputDir.resolve(SERVICE_PATH);
        Files.createDirectories(servicePath.getParent());
        Files.writeString(servicePath, mapperClassName + System.lineSeparator());
    }
}
