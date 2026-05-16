package org.pipelineframework.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import com.google.testing.compile.Compilation;
import com.google.testing.compile.Compiler;
import com.google.testing.compile.JavaFileObjects;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static com.google.testing.compile.CompilationSubject.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BlockingServiceProcessorWarningTest {

    @TempDir
    Path tempDir;

    @Test
    void materializingBlockingStreamingServiceEmitsWarning() throws Exception {
        Path generatedSourcesDir = tempDir.resolve("generated-sources");
        Files.createDirectories(generatedSourcesDir);
        Path descriptorDir = prepareDescriptorDir("descriptor-materialized");

        Compilation compilation = compiler(generatedSourcesDir, descriptorDir).compile(
            JavaFileObjects.forSourceString("test.blocking.Input", "package test.blocking; public class Input {}"),
            JavaFileObjects.forSourceString("test.blocking.Output", "package test.blocking; public class Output {}"),
            JavaFileObjects.forSourceString("test.blocking.ProcessMaterializingService", """
                package test.blocking;

                import java.util.List;
                import org.pipelineframework.annotation.PipelineStep;
                import org.pipelineframework.service.blocking.BlockingStreamingService;

                @PipelineStep
                public class ProcessMaterializingService implements BlockingStreamingService<Input, Output> {
                    @Override
                    public List<Output> processBlocking(Input processableObj) {
                        return List.of();
                    }
                }
                """));

        assertThat(compilation).failed();
        assertThat(compilation).hadWarningContaining("BlockingStreamingService materializes the full output list");
    }

    @Test
    void iteratorBlockingServiceDoesNotEmitMaterializationWarning() throws Exception {
        Path generatedSourcesDir = tempDir.resolve("generated-sources-iterator");
        Files.createDirectories(generatedSourcesDir);
        Path descriptorDir = prepareDescriptorDir("descriptor-iterator");

        Compilation compilation = compiler(generatedSourcesDir, descriptorDir).compile(
            JavaFileObjects.forSourceString("test.blocking.Input", "package test.blocking; public class Input {}"),
            JavaFileObjects.forSourceString("test.blocking.Output", "package test.blocking; public class Output {}"),
            JavaFileObjects.forSourceString("test.blocking.ProcessIteratorService", """
                package test.blocking;

                import org.pipelineframework.annotation.PipelineStep;
                import org.pipelineframework.blocking.CloseableIterator;
                import org.pipelineframework.service.blocking.BlockingIteratorService;

                @PipelineStep
                public class ProcessIteratorService implements BlockingIteratorService<Input, Output> {
                    @Override
                    public CloseableIterator<Output> iterateBlocking(Input processableObj) {
                        return new CloseableIterator<>() {
                            @Override
                            public boolean hasNext() {
                                return false;
                            }

                            @Override
                            public Output next() {
                                throw new java.util.NoSuchElementException();
                            }

                            @Override
                            public void close() {
                            }
                        };
                    }
                }
                """));

        assertThat(compilation).failed();
        assertFalse(compilation.warnings().stream()
            .map(diagnostic -> diagnostic.getMessage(Locale.ROOT))
            .anyMatch(message -> message.contains("materializes the full output list")));
    }

    private Compiler compiler(Path generatedSourcesDir, Path descriptorDir) {
        return Compiler.javac()
            .withProcessors(new PipelineStepProcessor())
            .withOptions(
                "-Apipeline.generatedSourcesDir=" + generatedSourcesDir,
                "-Aprotobuf.descriptor.path=" + descriptorDir);
    }

    private Path prepareDescriptorDir(String name) throws Exception {
        Path descriptorDir = tempDir.resolve(name);
        Files.createDirectories(descriptorDir);
        Files.copy(
            Paths.get(System.getProperty("user.dir")).resolve("src/test/resources/descriptor_set_search.dsc"),
            descriptorDir.resolve("descriptor_set.dsc"));
        return descriptorDir;
    }
}
