package org.pipelineframework.processor.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.List;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.PipelineCompilationContext;

class ConnectorBindingMetadataGeneratorTest {
    @TempDir
    Path tempDir;

    @Test
    void writesSanitizedBindingAndOperationMetadataWithoutReferenceValues() throws Exception {
        Path metadataRoot = tempDir.resolve("provider-metadata");
        Path manifest = metadataRoot.resolve("META-INF/pipeline/connector-providers.json");
        Files.createDirectories(manifest.getParent());
        Files.writeString(manifest, """
            {"schemaVersion":1,"providers":[{"id":"acme.work","version":{"major":1,"minor":0},
            "configurationSchema":{"id":"acme.work.provider","version":1,"fields":[
            {"name":"connection","type":"CONNECTION_REF","required":true},
            {"name":"secret","type":"SECRET_REF","required":true}]},
            "operations":[{"id":"invoice.send","kind":"tpf:command","majorVersion":1}]}]}
            """);
        Path pipeline = tempDir.resolve("pipeline.yaml");
        Files.writeString(pipeline, """
            version: 3
            basePackage: com.example
            connectors:
              work:
                provider: acme.work
                version: 1
                config:
                  connection: work-connection
                  secret: secret-reference
            steps:
              - name: Send invoice
                kind: command
                operation: invoice.send
                using: work
                input: Invoice
                output: SendResult
                java:
                  input: com.example.Invoice
                  output: com.example.SendResult
                commandIdGenerator: com.example.InvoiceCommandIdGenerator
            """);
        Path classOutput = tempDir.resolve("class-output");
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(classOutput));
        when(processingEnv.getOptions()).thenReturn(Map.of("pipeline.config", pipeline.toString()));
        PipelineCompilationContext context = new PipelineCompilationContext(
            processingEnv, mock(RoundEnvironment.class));
        context.setModuleDir(tempDir);

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (URLClassLoader loader = new URLClassLoader(new URL[] { metadataRoot.toUri().toURL() }, null)) {
            Thread.currentThread().setContextClassLoader(loader);
            new ConnectorBindingMetadataGenerator(processingEnv).writeMetadata(context);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }

        String json = Files.readString(classOutput.resolve(ConnectorBindingMetadataGenerator.RESOURCE_PATH));
        JsonObject binding = JsonParser.parseString(json).getAsJsonObject()
            .getAsJsonArray("bindings").get(0).getAsJsonObject();
        assertEquals("work", binding.get("name").getAsString());
        assertEquals("acme.work", binding.get("provider").getAsString());
        assertEquals("acme.work.provider", binding.getAsJsonObject("configuration").get("schemaId").getAsString());
        assertEquals(
            List.of("schemaId", "schemaVersion", "digest"),
            binding.getAsJsonObject("configuration").keySet().stream().toList());
        assertEquals("invoice.send", binding.getAsJsonArray("operations").get(0).getAsJsonObject()
            .get("operation").getAsString());
        assertFalse(json.contains("work-connection"), json);
        assertFalse(json.contains("secret-reference"), json);
    }

    private static final class PathResourceFiler implements Filer {
        private final Path outputDir;

        private PathResourceFiler(Path outputDir) {
            this.outputDir = outputDir;
        }

        @Override
        public JavaFileObject createSourceFile(CharSequence name, Element... originatingElements) {
            throw new UnsupportedOperationException("Source generation is not supported in this test.");
        }

        @Override
        public JavaFileObject createClassFile(CharSequence name, Element... originatingElements) {
            throw new UnsupportedOperationException("Class generation is not supported in this test.");
        }

        @Override
        public FileObject createResource(
            JavaFileManager.Location location,
            CharSequence pkg,
            CharSequence relativeName,
            Element... originatingElements
        ) {
            return new PathFileObject(outputDir.resolve(relativeName.toString()));
        }

        @Override
        public FileObject getResource(JavaFileManager.Location location, CharSequence pkg, CharSequence relativeName) {
            return new PathFileObject(outputDir.resolve(relativeName.toString()));
        }
    }

    private static final class PathFileObject extends SimpleJavaFileObject {
        private final Path path;

        private PathFileObject(Path path) {
            super(path.toUri(), Kind.OTHER);
            this.path = path;
        }

        @Override
        public Writer openWriter() throws IOException {
            Files.createDirectories(path.getParent());
            return Files.newBufferedWriter(path);
        }

        @Override
        public OutputStream openOutputStream() throws IOException {
            Files.createDirectories(path.getParent());
            return Files.newOutputStream(path);
        }
    }
}
