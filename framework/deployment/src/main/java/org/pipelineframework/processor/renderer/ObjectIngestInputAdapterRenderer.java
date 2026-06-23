package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.Filer;
import javax.lang.model.element.Modifier;
import javax.tools.StandardLocation;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import org.pipelineframework.objectingest.ObjectIngestInputAdapter;
import org.pipelineframework.processor.PipelineStepProcessor;

/**
 * Generates the service-loaded adapter used by Object Ingest to feed remote pipeline input boundaries.
 */
public final class ObjectIngestInputAdapterRenderer {
    private static final String CLASS_NAME = "ObjectIngestPipelineInputAdapter";
    private static final String SERVICE_PATH = "META-INF/services/" + "org.pipelineframework.objectingest.ObjectIngestInputAdapter";

    public ClassName render(
        String basePackage,
        TypeName domainType,
        TypeName externalType,
        TypeName mapperType,
        GenerationContext ctx
    ) throws IOException {
        if (!(domainType instanceof ClassName domainClass)
            || !(externalType instanceof ClassName externalClass)
            || !(mapperType instanceof ClassName mapperClass)) {
            throw new IllegalArgumentException("Object Ingest input adapter requires class-backed domain, external, and mapper types");
        }
        String packageName = basePackage + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX;
        ClassName adapterClass = ClassName.get(packageName, CLASS_NAME);
        TypeSpec type = TypeSpec.classBuilder(CLASS_NAME)
            .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
            .addSuperinterface(ParameterizedTypeName.get(
                ClassName.get(ObjectIngestInputAdapter.class),
                domainClass,
                externalClass))
            .addField(mapperClass, "mapper", Modifier.PRIVATE, Modifier.FINAL)
            .addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addStatement("this.mapper = loadMapper()")
                .build())
            .addMethod(MethodSpec.methodBuilder("domainType")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ParameterizedTypeName.get(ClassName.get(Class.class), domainClass))
                .addStatement("return $T.class", domainClass)
                .build())
            .addMethod(MethodSpec.methodBuilder("toPipelineInput")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(externalType)
                .addParameter(domainClass, "item")
                .addStatement("return mapper.toExternal(item)")
                .build())
            .addMethod(MethodSpec.methodBuilder("loadMapper")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                .returns(mapperClass)
                .addCode("""
                    try {
                        jakarta.enterprise.inject.Instance<$T> cdiMapper = jakarta.enterprise.inject.spi.CDI.current().select($T.class);
                        if (cdiMapper != null && !cdiMapper.isUnsatisfied() && !cdiMapper.isAmbiguous()) {
                            return cdiMapper.get();
                        }
                    } catch (IllegalStateException ignored) {
                    }
                    try {
                        java.lang.reflect.Field instance = $T.class.getField("INSTANCE");
                        return ($T) instance.get(null);
                    } catch (NoSuchFieldException ignored) {
                        try {
                            return $T.class.getDeclaredConstructor().newInstance();
                        } catch (ReflectiveOperationException e) {
                            throw new IllegalStateException("Failed to instantiate object ingest input mapper: $L", e);
                        }
                    } catch (ReflectiveOperationException e) {
                        throw new IllegalStateException("Failed to access object ingest input mapper: $L", e);
                    }
                    """,
                    mapperClass,
                    mapperClass,
                    mapperClass,
                    mapperClass,
                    mapperClass,
                    mapperClass.canonicalName(),
                    mapperClass.canonicalName())
                .build())
            .build();

        JavaFile javaFile = JavaFile.builder(packageName, type).build();
        if (ctx.processingEnv() != null) {
            javaFile.writeTo(ctx.processingEnv().getFiler());
            writeServiceDescriptor(ctx.processingEnv().getFiler(), adapterClass.canonicalName());
        } else {
            javaFile.writeTo(ctx.outputDir());
            writeServiceDescriptor(ctx.outputDir(), adapterClass.canonicalName());
        }
        return adapterClass;
    }

    private void writeServiceDescriptor(Filer filer, String adapterClassName) throws IOException {
        try (Writer writer = filer.createResource(StandardLocation.CLASS_OUTPUT, "", SERVICE_PATH).openWriter()) {
            writer.write(adapterClassName);
            writer.write(System.lineSeparator());
        }
    }

    private void writeServiceDescriptor(Path outputDir, String adapterClassName) throws IOException {
        Path servicePath = outputDir.resolve(SERVICE_PATH);
        Files.createDirectories(servicePath.getParent());
        Files.writeString(servicePath, adapterClassName + System.lineSeparator());
    }
}
