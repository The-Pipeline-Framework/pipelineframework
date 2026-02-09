package org.pipelineframework.processor.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.element.Element;
import javax.tools.FileObject;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.PlatformMode;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.TransportMode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PipelinePlatformMetadataGeneratorTest {

    @TempDir
    Path tempDir;

    @Test
    void writesPlatformMetadata() throws IOException {
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(new PathResourceFiler(tempDir.resolve("class-output")));
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);

        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setPlatformMode(PlatformMode.LAMBDA);
        ctx.setTransportMode(TransportMode.REST);
        ctx.setModuleName("orchestrator-svc");
        ctx.setPluginHost(false);

        new PipelinePlatformMetadataGenerator(processingEnv).writePlatformMetadata(ctx);

        Path file = tempDir.resolve("class-output").resolve("META-INF/pipeline/platform.json");
        JsonObject metadata = new Gson().fromJson(Files.readString(file), JsonObject.class);
        assertEquals("LAMBDA", metadata.get("platform").getAsString());
        assertEquals("REST", metadata.get("transport").getAsString());
        assertEquals("orchestrator-svc", metadata.get("module").getAsString());
        assertEquals(false, metadata.get("pluginHost").getAsBoolean());
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
            Element... originatingElements) {
            Path path = outputDir.resolve(relativeName.toString());
            return new PathFileObject(path);
        }

        @Override
        public FileObject getResource(
            JavaFileManager.Location location,
            CharSequence pkg,
            CharSequence relativeName) {
            Path path = outputDir.resolve(relativeName.toString());
            return new PathFileObject(path);
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
