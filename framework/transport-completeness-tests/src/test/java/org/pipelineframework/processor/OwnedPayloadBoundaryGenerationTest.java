package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.processor.renderer.HttpPayloadBoundaryRenderer;

/** Compiles generated HTTP boundary resources against the released runtime artifact set. */
class OwnedPayloadBoundaryGenerationTest {
    @TempDir Path workspace;

    @Test
    void computeCompilesDisklessResource() throws Exception {
        String generated = compile("COMPUTE", "filesystem");
        assertTrue(generated.contains("OwnedPayloadMultipart(body, contentType)"));
        assertFalse(generated.contains("FileUpload"));
    }

    @Test
    void functionCompilesDelegatedControlResource() throws Exception {
        String generated = compile("FUNCTION", "s3");
        assertTrue(generated.contains("initiateDelegatedUpload"));
        assertTrue(generated.contains("completeDelegatedUpload"));
    }

    private String compile(String platform, String provider) throws Exception {
        Path config = workspace.resolve("pipeline-" + platform + ".yaml");
        Files.writeString(config, """
            version: 3
            appName: Owned payload boundary
            basePackage: org.example.payload
            transport: REST
            platform: %s
            contract: { input: InvoiceInput, output: ReceiptOutput }
            types:
              InvoiceInput: { fields: [[payload_ref, payload_ref]] }
              ReceiptOutput: { fields: [[payload_ref, payload_ref]] }
            sources:
              receipts: { kind: object, provider: %s, binding: objects }
            publish:
              invoices: { kind: object, provider: %s, binding: objects }
            httpPayloads:
              invoice:
                direction: upload
                object: invoices
                canonicalType: InvoiceInput
                referenceField: payload_ref
                contentTypes: [application/pdf]
                maxBytes: 16777216
                authorizationScope: invoice.write
              receipt:
                direction: download
                object: receipts
                canonicalType: ReceiptOutput
                referenceField: payload_ref
                contentTypes: [application/pdf]
                authorizationScope: invoice.read
            steps: []
            """.formatted(platform, provider, provider));
        Path generatedDir = workspace.resolve("generated-" + platform)
            .resolve("target/generated-sources/pipeline");
        Files.createDirectories(generatedDir);
        var template = new PipelineTemplateConfigLoader().load(config);
        new HttpPayloadBoundaryRenderer().render(template, generatedDir);
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        var compiler = ToolProvider.getSystemJavaCompiler();
        Path classes = workspace.resolve("classes-" + platform);
        Files.createDirectories(classes);
        try (var files = compiler.getStandardFileManager(diagnostics, null, null)) {
            List<Path> sources;
            try (var paths = Files.walk(generatedDir)) {
                sources = paths.filter(path -> path.toString().endsWith(".java")).toList();
            }
            var units = files.getJavaFileObjectsFromPaths(sources);
            boolean compiled = compiler.getTask(null, files, diagnostics,
                List.of("-proc:none", "-classpath", System.getProperty("java.class.path"),
                    "-d", classes.toString()), null, units).call();
            assertTrue(compiled, diagnostics.getDiagnostics().toString());
        }
        return Files.readString(generatedDir.resolve("org/example/payload/pipeline/GeneratedPayloadBoundary0.java"));
    }
}
