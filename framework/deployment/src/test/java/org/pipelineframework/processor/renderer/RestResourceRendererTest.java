package org.pipelineframework.processor.renderer;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import javax.annotation.processing.ProcessingEnvironment;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.processor.ir.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class RestResourceRendererTest {

    @Test
    void rendersUnaryResourceMatchingCsvPaymentExample() throws IOException {
        PipelineStepModel model = new PipelineStepModel.Builder()
            .serviceName("ProcessPaymentStatusReactiveService")
            .servicePackage("org.pipelineframework.csv.service")
            .serviceClassName(ClassName.get(
                "org.pipelineframework.csv.service",
                "ProcessPaymentStatusReactiveService"))
            .streamingShape(StreamingShape.UNARY_UNARY)
            .executionMode(ExecutionMode.DEFAULT)
            .inputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentStatus"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentStatusMapper"),
                true))
            .outputMapping(new TypeMapping(
                ClassName.get("org.pipelineframework.csv.common.domain", "PaymentOutput"),
                ClassName.get("org.pipelineframework.csv.common.mapper", "PaymentOutputMapper"),
                true))
            .enabledTargets(java.util.Set.of(GenerationTarget.REST_RESOURCE))
            .build();

        RestBinding binding = new RestBinding(
            model,
            "/ProcessPaymentStatusReactiveService/remoteProcess");

        JavaFileObject javaFileObject = new InMemoryJavaFileObject(
            "org.pipelineframework.csv.service.pipeline.ProcessPaymentStatusResource");
        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        GenerationContext context = new GenerationContext(processingEnv, javaFileObject);

        RestResourceRenderer renderer = new RestResourceRenderer();
        renderer.render(binding, context);

        String source = javaFileObject.getCharContent(true).toString();

        assertTrue(source.contains("package org.pipelineframework.csv.service.pipeline;"));
        assertTrue(source.contains("@GeneratedRole(Role.REST_SERVER)"));
        assertTrue(source.contains("@Path(\"/ProcessPaymentStatusReactiveService/remoteProcess\")"));
        assertTrue(source.contains("class ProcessPaymentStatusResource"));
        assertTrue(source.contains("private static final Logger logger = Logger.getLogger(ProcessPaymentStatusResource.class);"));
        assertTrue(source.contains("ProcessPaymentStatusReactiveService domainService;"));
        assertTrue(source.contains("PaymentStatusMapper paymentStatusMapper;"));
        assertTrue(source.contains("PaymentOutputMapper paymentOutputMapper;"));
        assertTrue(source.contains("@POST"));
        assertTrue(source.contains("@Path(\"/process\")"));
        assertTrue(source.contains("public Uni<PaymentOutputDto> process(PaymentStatusDto inputDto)"));
        assertTrue(source.contains("PaymentStatus inputDomain = paymentStatusMapper.fromDto(inputDto);"));
        assertTrue(source.contains("return domainService.process(inputDomain).map(output -> paymentOutputMapper.toDto(output));"));
        assertTrue(source.contains("public RestResponse handleException(Exception ex)"));
        assertTrue(source.contains("if (ex instanceof IllegalArgumentException)"));
        assertTrue(source.contains("RestResponse.status(Response.Status.BAD_REQUEST, \"Invalid request\")"));
        assertTrue(source.contains("RestResponse.status(Response.Status.INTERNAL_SERVER_ERROR, \"An unexpected error occurred\")"));
    }

    private static final class InMemoryJavaFileObject extends SimpleJavaFileObject {
        private final StringWriter writer = new StringWriter();

        private InMemoryJavaFileObject(String className) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
        }

        @Override
        public Writer openWriter() {
            return writer;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return writer.toString();
        }
    }
}
