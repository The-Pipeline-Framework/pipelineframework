package org.pipelineframework.processor.renderer;

import java.io.IOException;
import javax.annotation.processing.Filer;
import javax.tools.JavaFileObject;
import com.squareup.javapoet.ClassName;
import org.pipelineframework.processor.PipelineStepProcessor;
import org.pipelineframework.processor.ir.PipelineStepModel;
import org.pipelineframework.processor.representation.ResolvedProviderBoundary;

/** Emits the one generated, CDI-only worker step for a provider-owned resumable producer. */
public final class ResumableSourceContinuationRenderer {

    /** Compatibility entry point for focused renderer fixtures without terminal eligibility metadata. */
    public String render(PipelineStepModel producer, PipelineStepModel await, int awaitStepIndex,
                         ResolvedProviderBoundary providerBoundary, GenerationContext ctx) throws IOException {
        return render(producer, await, Math.max(0, awaitStepIndex - 1), awaitStepIndex, false,
            providerBoundary, ctx);
    }

    public String render(PipelineStepModel producer, PipelineStepModel await, int producerStepIndex,
                         int awaitStepIndex, boolean terminalScalarSuffix,
                         ResolvedProviderBoundary providerBoundary, GenerationContext ctx) throws IOException {
        String baseName = producer.generatedName().endsWith("Service")
            ? producer.generatedName().substring(0, producer.generatedName().length() - "Service".length())
            : producer.generatedName();
        String packageName = producer.servicePackage() + PipelineStepProcessor.PIPELINE_PACKAGE_SUFFIX;
        String simpleName = baseName + "StreamRegionContinuation";
        String className = packageName + "." + simpleName;
        String sourceType = producer.inputMapping().domainType().toString();
        String itemType = producer.outputMapping().domainType().toString();
        String facadeType = providerBoundary.claim().generatedFacadeTypeName();
        String descriptorInvocation = AwaitDescriptorCodegen.resolve(await, ctx).descriptorInvocation().toString();
        String source = """
            package %s;

            @jakarta.enterprise.context.ApplicationScoped
            @io.quarkus.arc.Unremovable
            public final class %s extends org.pipelineframework.step.ConfigurableStep
                implements org.pipelineframework.stream.StreamRegionContinuation,
                    org.pipelineframework.step.StepOneToOne<%s.Input, %s.Page> {

                @jakarta.inject.Inject
                %s facade;

                @jakarta.inject.Inject
                org.pipelineframework.awaitable.AwaitStepDescriptorFactory descriptorFactory;

                public static record Input(%s source,
                                           org.pipelineframework.stream.ResumableSourceDescriptor descriptor,
                                           org.pipelineframework.stream.OpaqueSourceCheckpoint checkpoint,
                                           int limit)
                    implements org.pipelineframework.stream.StreamRegionContinuationInput {}

                public static record Page(java.util.List<%s> items,
                                          org.pipelineframework.stream.OpaqueSourceCheckpoint nextCheckpoint,
                                          boolean endOfSource)
                    implements org.pipelineframework.stream.StreamRegionContinuationResult {}

                @Override
                public int producerStepIndex() {
                    return %d;
                }

                @Override
                public boolean terminalScalarSuffix() {
                    return %s;
                }

                @Override
                public org.pipelineframework.stream.ResumableSourceDescriptor descriptor() {
                    return capability().descriptor();
                }

                @Override
                public org.pipelineframework.stream.StreamRegionAwaitBinding awaitBinding() {
                    return new org.pipelineframework.stream.StreamRegionAwaitBinding((%s).await().indefinitely(), %d);
                }

                @Override
                public Input inputFor(Object source, org.pipelineframework.stream.OpaqueSourceCheckpoint checkpoint, int limit) {
                    return new Input((%s) source, descriptor(), checkpoint, limit);
                }

                @Override
                public org.pipelineframework.step.StepOneToOne<?, ?> transitionStep() {
                    return this;
                }

                @Override
                public io.smallrye.mutiny.Uni<Page> applyOneToOne(Input input) {
                    return capability().readPage(input.source(), input.checkpoint(), input.limit())
                        .map(page -> new Page(page.items(), page.nextCheckpoint(), page.endOfSource()));
                }

                private org.pipelineframework.stream.ResumableSourceCapability<%s, %s> capability() {
                    return facade;
                }
            }
            """.formatted(packageName, simpleName, simpleName, simpleName, facadeType, sourceType, itemType,
                producerStepIndex, terminalScalarSuffix, descriptorInvocation, awaitStepIndex, sourceType, sourceType, itemType);
        Filer filer = ctx.processingEnv().getFiler();
        JavaFileObject file = filer.createSourceFile(className);
        try (var writer = file.openWriter()) {
            writer.write(source);
        }
        return className;
    }
}
