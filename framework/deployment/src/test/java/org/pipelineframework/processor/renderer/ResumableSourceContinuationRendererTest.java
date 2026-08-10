package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.Modifier;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.DeploymentRole;
import org.pipelineframework.processor.ir.ExecutionMode;
import org.pipelineframework.processor.ir.GenerationTarget;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.ir.PipelineTransport;
import org.pipelineframework.processor.ir.StreamingShape;
import org.pipelineframework.processor.ir.TypeMapping;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;
import org.pipelineframework.parallelism.OrderingRequirement;
import org.pipelineframework.parallelism.ThreadSafety;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderCapability;
import org.pipelineframework.representation.spi.ProviderExecutionStyle;
import org.pipelineframework.representation.spi.ProviderStepContract;

class ResumableSourceContinuationRendererTest {

    @Test
    void generatedContinuationRequiresTheConcreteFacadeCapabilityContract() throws Exception {
        CapturingFiler filer = new CapturingFiler();
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getFiler()).thenReturn(filer);
        when(processingEnv.getOptions()).thenReturn(Map.of());
        GenerationContext context = new GenerationContext(processingEnv, null, DeploymentRole.ORCHESTRATOR_CLIENT,
            Set.of(), null, null, PipelineTransport.LOCAL, "org.pipelineframework", null, false);

        new ResumableSourceContinuationRenderer().render(producer(), await(), 1, boundary(), context);
        String continuation = filer.source();
        assertTrue(continuation.contains("public static record Input"));
        assertTrue(continuation.contains("public static record Page"));
        assertTrue(continuation.contains("ResumableSourceCapability<java.lang.String, java.lang.String> capability()"));

        assertTrue(compiles(continuation, facade(true)));
        assertFalse(compiles(continuation, facade(false)));
    }

    private static boolean compiles(String continuation, String facade) throws IOException {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        java.nio.file.Path output = Files.createTempDirectory("resumable-continuation-compile");
        return compiler.getTask(null, null, null, List.of("-classpath", System.getProperty("java.class.path"), "-d", output.toString()), null,
            List.of(new Source("org.pipelineframework.fixture.GeneratedFacade", facade),
                new Source("org.pipelineframework.fixture.ReadStreamRegionContinuation", continuation))).call();
    }

    private static String facade(boolean resumable) {
        return resumable ? """
            package org.pipelineframework.fixture;
            public final class GeneratedFacade implements org.pipelineframework.stream.ResumableSourceCapability<String, String> {
              public org.pipelineframework.stream.ResumableSourceDescriptor descriptor() { return new org.pipelineframework.stream.ResumableSourceDescriptor("deterministic", "source", "v1"); }
              public io.smallrye.mutiny.Uni<org.pipelineframework.stream.ResumableSourcePage<String>> readPage(String source, org.pipelineframework.stream.OpaqueSourceCheckpoint checkpoint, int limit) { throw new UnsupportedOperationException(); }
            }
            """ : "package org.pipelineframework.fixture; public final class GeneratedFacade {}";
    }

    private static PipelineStepModel producer() {
        return model("ReadService", ClassName.get(String.class), ClassName.get(String.class), StreamingShape.UNARY_STREAMING,
            ClassName.get("org.pipelineframework.fixture", "GeneratedFacade"));
    }

    private static PipelineStepModel await() {
        return model("AwaitService", ClassName.get(String.class), ClassName.get(String.class), StreamingShape.UNARY_UNARY,
            ClassName.get("org.pipelineframework.awaitable", "AwaitStepDescriptor"));
    }

    private static PipelineStepModel model(String name, ClassName input, ClassName output, StreamingShape shape, ClassName service) {
        return new PipelineStepModel.Builder().serviceName(name).generatedName(name).servicePackage("org.pipelineframework.fixture")
            .serviceClassName(service).inputMapping(new TypeMapping(input, null, false))
            .outputMapping(new TypeMapping(output, null, false)).streamingShape(shape)
            .enabledTargets(Set.of(GenerationTarget.LOCAL_CLIENT_STEP)).executionMode(ExecutionMode.DEFAULT)
            .deploymentRole(DeploymentRole.ORCHESTRATOR_CLIENT).sideEffect(false)
            .orderingRequirement(OrderingRequirement.RELAXED).threadSafety(ThreadSafety.SAFE).build();
    }

    private static ResolvedProviderBoundary boundary() {
        CanonicalType string = new CanonicalType("String", String.class.getName(), CanonicalTypeShape.RECORD);
        return new ResolvedProviderBoundary(new BoundaryRequest("ReadService", "org.pipelineframework.fixture.Reader", string,
            string, "UNARY_STREAMING", Set.of(), Map.of()), new BoundaryClaim("deterministic", "binding",
            "org.pipelineframework.fixture.GeneratedFacade", Optional.of(new ProviderStepContract(
                ProviderExecutionStyle.BLOCKING_ITERATOR, "UNARY_STREAMING", Set.of(ProviderCapability.RESUMABLE_SOURCE)))),
            List.of(), Map.of());
    }

    private static final class CapturingFiler implements Filer {
        private final Source source = new Source("org.pipelineframework.fixture.ReadStreamRegionContinuation", "");
        @Override public JavaFileObject createSourceFile(CharSequence name, javax.lang.model.element.Element... elements) { return source; }
        @Override public JavaFileObject createClassFile(CharSequence name, javax.lang.model.element.Element... elements) { throw new UnsupportedOperationException(); }
        @Override public javax.tools.FileObject createResource(javax.tools.JavaFileManager.Location location, CharSequence pkg, CharSequence relativeName, javax.lang.model.element.Element... elements) { throw new UnsupportedOperationException(); }
        @Override public javax.tools.FileObject getResource(javax.tools.JavaFileManager.Location location, CharSequence pkg, CharSequence relativeName) { throw new UnsupportedOperationException(); }
        String source() { return source.writer.toString(); }
    }

    private static final class Source extends SimpleJavaFileObject {
        private final StringWriter writer = new StringWriter();
        private Source(String className, String source) { super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE); writer.write(source); }
        @Override public CharSequence getCharContent(boolean ignoreEncodingErrors) { return writer.toString(); }
        @Override public StringWriter openWriter() throws IOException { return writer; }
    }
}
