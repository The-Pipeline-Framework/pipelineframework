package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import org.jboss.logging.Logger;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.DeploymentRole;

/**
 * Generates protobuf parser classes from descriptor sets.
 */
public class ProtobufParserService {

    private static final Logger LOG = Logger.getLogger(ProtobufParserService.class);

    private final GenerationPathResolver pathResolver;

    public ProtobufParserService(GenerationPathResolver pathResolver) {
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver must not be null");
    }

    /**
     * Generates protobuf parser classes for descriptors.
     *
     * @param ctx compilation context
     * @param descriptorSet descriptor set
     */
    public void generateProtobufParsers(PipelineCompilationContext ctx, DescriptorProtos.FileDescriptorSet descriptorSet) {
        if (ctx == null) {
            throw new IllegalArgumentException("ctx must not be null");
        }
        Map<String, Descriptors.FileDescriptor> fileDescriptors = buildFileDescriptors(descriptorSet);
        if (fileDescriptors.isEmpty()) {
            return;
        }
        DeploymentRole role = ctx.isPluginHost() ? DeploymentRole.PLUGIN_SERVER : DeploymentRole.PIPELINE_SERVER;
        Path outputDir = pathResolver.resolveRoleOutputDir(ctx, role);
        Set<String> generated = new HashSet<>();

        for (Descriptors.FileDescriptor fileDescriptor : fileDescriptors.values()) {
            for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
                collectAndGenerateParser(ctx, descriptor, outputDir, generated);
            }
        }
    }

    private void collectAndGenerateParser(
            PipelineCompilationContext ctx,
            Descriptors.Descriptor descriptor,
            Path outputDir,
            Set<String> generated) {
        if (descriptor == null) {
            return;
        }
        if (!descriptor.getOptions().getMapEntry()) {
            ClassName messageType = resolveMessageClassName(descriptor);
            if (messageType != null) {
                String parserPackage = messageType.packageName().isBlank()
                    ? "pipeline"
                    : messageType.packageName() + ".pipeline";
                String parserName = "Proto" + String.join("_", messageType.simpleNames()) + "Parser";
                String fqcn = parserPackage + "." + parserName;
                if (generated.add(fqcn)) {
                    TypeSpec parserClass = buildParserClass(messageType, parserName);
                    try {
                        JavaFile.builder(parserPackage, parserClass).build().writeTo(outputDir);
                    } catch (IOException e) {
                        if (ctx.getProcessingEnv() != null) {
                            ctx.getProcessingEnv().getMessager().printMessage(
                                javax.tools.Diagnostic.Kind.WARNING,
                                "Failed to generate protobuf parser for '" + messageType + "': " + e.getMessage());
                        } else {
                            LOG.warnf(e,
                                "Failed to generate protobuf parser for '%s' in package '%s' at '%s'",
                                messageType,
                                parserPackage,
                                outputDir);
                        }
                    }
                }
            }
        }
        for (Descriptors.Descriptor nested : descriptor.getNestedTypes()) {
            collectAndGenerateParser(ctx, nested, outputDir, generated);
        }
    }

    private TypeSpec buildParserClass(ClassName messageType, String parserName) {
        ClassName parserInterface = ClassName.get("org.pipelineframework.cache", "ProtobufMessageParser");
        ClassName messageBase = ClassName.get("com.google.protobuf", "Message");
        ClassName invalidProto = ClassName.get("com.google.protobuf", "InvalidProtocolBufferException");

        MethodSpec typeMethod = MethodSpec.methodBuilder("type")
            .addAnnotation(Override.class)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .returns(String.class)
            .addStatement("return $S", messageType.toString())
            .build();

        MethodSpec parseMethod = MethodSpec.methodBuilder("parseFrom")
            .addAnnotation(Override.class)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .returns(messageBase)
            .addParameter(byte[].class, "bytes")
            .addCode("""
                try {
                    return $T.parseFrom(bytes);
                } catch ($T e) {
                    throw new RuntimeException("Failed to parse " + type(), e);
                }
                """, messageType, invalidProto)
            .build();

        return TypeSpec.classBuilder(parserName)
            .addModifiers(javax.lang.model.element.Modifier.PUBLIC)
            .addAnnotation(ClassName.get("jakarta.enterprise.context", "ApplicationScoped"))
            .addAnnotation(ClassName.get("io.quarkus.arc", "Unremovable"))
            .addSuperinterface(parserInterface)
            .addMethod(typeMethod)
            .addMethod(parseMethod)
            .build();
    }

    private Map<String, Descriptors.FileDescriptor> buildFileDescriptors(DescriptorProtos.FileDescriptorSet descriptorSet) {
        Map<String, Descriptors.FileDescriptor> built = new HashMap<>();
        if (descriptorSet == null) {
            return built;
        }
        boolean progress = true;
        while (built.size() < descriptorSet.getFileCount() && progress) {
            progress = false;
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                String fileName = fileProto.getName();
                if (built.containsKey(fileName)) {
                    continue;
                }
                boolean depsReady = true;
                for (String dependency : fileProto.getDependencyList()) {
                    if (!built.containsKey(dependency)) {
                        depsReady = false;
                        break;
                    }
                }
                if (!depsReady) {
                    continue;
                }
                try {
                    List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
                    for (String dependency : fileProto.getDependencyList()) {
                        dependencies.add(built.get(dependency));
                    }
                    Descriptors.FileDescriptor[] depsArray = dependencies.toArray(new Descriptors.FileDescriptor[0]);
                    Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileProto, depsArray);
                    built.put(fileName, fileDescriptor);
                    progress = true;
                } catch (Descriptors.DescriptorValidationException e) {
                    LOG.debug("Skipping invalid descriptor while building file descriptors", e);
                }
            }
        }

        if (built.size() < descriptorSet.getFileCount()) {
            List<String> unresolved = new ArrayList<>();
            for (DescriptorProtos.FileDescriptorProto fileProto : descriptorSet.getFileList()) {
                if (!built.containsKey(fileProto.getName())) {
                    unresolved.add(fileProto.getName());
                }
            }
            LOG.warnf("Protobuf descriptor resolution incomplete; unresolved files: %s", unresolved);
        }
        return built;
    }

    private ClassName resolveMessageClassName(Descriptors.Descriptor descriptor) {
        if (descriptor == null) {
            return null;
        }
        Descriptors.FileDescriptor fileDescriptor = descriptor.getFile();
        String javaPkg = fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();

        List<String> nesting = new ArrayList<>();
        Descriptors.Descriptor current = descriptor;
        while (current != null) {
            nesting.add(0, current.getName());
            current = current.getContainingType();
        }

        if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
            String outer = nesting.get(0);
            String[] nested = nesting.size() > 1
                ? nesting.subList(1, nesting.size()).toArray(new String[0])
                : new String[0];
            return ClassName.get(javaPkg, outer, nested);
        }

        String outerClass = deriveOuterClassName(fileDescriptor);
        List<String> full = new ArrayList<>();
        full.add(outerClass);
        full.addAll(nesting);
        String[] nested = full.subList(1, full.size()).toArray(new String[0]);
        return ClassName.get(javaPkg, full.get(0), nested);
    }

    private String deriveOuterClassName(Descriptors.FileDescriptor fileDescriptor) {
        if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
            return fileDescriptor.getOptions().getJavaOuterClassname();
        }
        String fileName = fileDescriptor.getName();
        int slashIndex = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
        if (slashIndex >= 0 && slashIndex + 1 < fileName.length()) {
            fileName = fileName.substring(slashIndex + 1);
        }
        if (fileName.endsWith(".proto")) {
            fileName = fileName.substring(0, fileName.length() - 6);
        }
        String[] parts = fileName.split("[^a-zA-Z0-9]+");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (!part.isEmpty()) {
                sb.append(Character.toUpperCase(part.charAt(0)));
                if (part.length() > 1) {
                    sb.append(part.substring(1));
                }
            }
        }
        if (sb.length() == 0) {
            return "ProtoFile" + Integer.toHexString(fileDescriptor.getName().hashCode()).toUpperCase();
        }
        return sb.toString();
    }
}
