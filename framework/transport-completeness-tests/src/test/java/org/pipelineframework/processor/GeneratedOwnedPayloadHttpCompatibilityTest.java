package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.processor.renderer.HttpPayloadBoundaryRenderer;

/** Compiler output must compile against the released runtime-safe payload APIs. */
class GeneratedOwnedPayloadHttpCompatibilityTest {
    @TempDir Path work;

    @Test
    void generatedUploadAndDownloadCompileAgainstRuntime() throws Exception {
        Path yaml = work.resolve("pipeline.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: Payload boundaries
            basePackage: org.example
            transport: REST
            platform: COMPUTE
            contract: { input: InvoiceInput, output: ReceiptOutput }
            types:
              InvoiceInput:
                fields:
                  - [payload_ref, payload_ref]
              ReceiptOutput:
                fields:
                  - [payload_ref, payload_ref]
            sources:
              receipts: { kind: object, provider: filesystem, binding: files }
            publish:
              invoices: { kind: object, provider: filesystem, binding: files }
            httpPayloads:
              receipt:
                direction: download
                object: receipts
                canonicalType: ReceiptOutput
                referenceField: payload_ref
                contentTypes: [application/pdf]
                authorizationScope: receipt.read
              invoice:
                direction: upload
                object: invoices
                canonicalType: InvoiceInput
                referenceField: payload_ref
                contentTypes: [application/pdf]
                maxBytes: 1024
                authorizationScope: invoice.write
            steps: []
            """);
        var config = new PipelineTemplateConfigLoader().load(yaml);
        Path generated = work.resolve("generated");
        List<String> classes = new HttpPayloadBoundaryRenderer().render(config, generated);
        assertEquals(List.of("org.example.pipeline.GeneratedPayloadBoundary0",
            "org.example.pipeline.GeneratedPayloadBoundary1"), classes);
        List<Path> sources;
        try (var files = Files.walk(generated)) {
            sources = files.filter(path -> path.toString().endsWith(".java")).sorted().toList();
        }
        assertEquals(2, sources.size());
        assertTrue(Files.readString(sources.getFirst()).contains("@Authenticated"));
        assertTrue(Files.readString(sources.getFirst()).contains("transfer().upload"));
        Path fixture = Path.of("src/test/java/org/example/pipeline");
        for (Path source : sources) {
            assertEquals(Files.readString(source), Files.readString(fixture.resolve(source.getFileName())),
                "HTTP fixture must be the current compiler output");
        }
        compile(sources);
    }

    private void compile(List<Path> sources) throws IOException {
        Path classes = work.resolve("classes");
        Files.createDirectories(classes);
        var compiler = ToolProvider.getSystemJavaCompiler();
        try (var manager = compiler.getStandardFileManager(null, null, null)) {
            var units = manager.getJavaFileObjectsFromPaths(sources);
            Boolean success = compiler.getTask(null, manager, null,
                List.of("-proc:none", "-classpath", System.getProperty("java.class.path"),
                    "-d", classes.toString()), null, units).call();
            assertTrue(Boolean.TRUE.equals(success), "generated HTTP resources must compile against runtime artifacts");
        }
    }
}
