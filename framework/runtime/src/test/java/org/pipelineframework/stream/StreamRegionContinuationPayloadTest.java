package org.pipelineframework.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitStepDescriptor;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

public class StreamRegionContinuationPayloadTest {

    private static final ResumableSourceDescriptor DESCRIPTOR =
        new ResumableSourceDescriptor("deterministic", "payments", "fixture-v1");

    @Test
    void concreteGeneratedInputAndPageRetainCanonicalTypesAcrossPortablePayloads() {
        JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
        GeneratedInput input = new GeneratedInput("source-a", DESCRIPTOR, OpaqueSourceCheckpoint.initial(), 2);
        GeneratedPage page = new GeneratedPage(List.of(new Item("one"), new Item("two")),
            new OpaqueSourceCheckpoint(java.util.Optional.of("2")), false);

        GeneratedInput decodedInput = assertInstanceOf(GeneratedInput.class, codec.decode(codec.encode(input)));
        assertEquals(input, decodedInput);

        TransitionResultEnvelope envelope = TransitionResultEnvelope.completed(codec, List.of(page));
        GeneratedPage decodedPage = assertInstanceOf(GeneratedPage.class, envelope.decodeOutputItems(codec).getFirst());
        assertEquals(page, decodedPage);
        assertInstanceOf(Item.class, decodedPage.items().getFirst());
    }

    @Test
    void remoteTransitionPayloadRoundTripRetainsConcretePageItemType() {
        JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
        GeneratedInput input = new GeneratedInput("source-a", DESCRIPTOR, OpaqueSourceCheckpoint.initial(), 2);
        TransitionWorkerCommand command = new TransitionWorkerCommand(
            "tenant", "execution", 0, 1, 1, ExecutionResultShape.SINGLE, 3, "transition",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, input));
        TransitionCommandEnvelope remote = TransitionCommandEnvelope.from(
            command, "pipeline", "contract", "release", "transition", codec.encode(command.inputPayload()));

        ExecutionInputSnapshot decodedInput = assertInstanceOf(ExecutionInputSnapshot.class,
            remote.toCommand(codec).inputPayload());
        assertEquals(input, assertInstanceOf(GeneratedInput.class, decodedInput.payload()));

        GeneratedPage page = new GeneratedPage(List.of(new Item("one")),
            new OpaqueSourceCheckpoint(java.util.Optional.of("1")), false);
        GeneratedPage decodedPage = assertInstanceOf(GeneratedPage.class,
            TransitionResultEnvelope.completed(codec, List.of(page)).decodeOutputItems(codec).getFirst());
        assertInstanceOf(Item.class, decodedPage.items().getFirst());
    }

    @Test
    void generatedFacadeExposesAConcreteNormalStepOverTheCapability() {
        DeterministicGeneratedFacade facade = new DeterministicGeneratedFacade();
        GeneratedInput input = new GeneratedInput("source-a", DESCRIPTOR, OpaqueSourceCheckpoint.initial(), 2);

        Object output = facade.transitionStep().applyOneToOne(input).await().indefinitely();

        GeneratedPage page = assertInstanceOf(GeneratedPage.class, output);
        assertEquals(List.of(new Item("source-a-0"), new Item("source-a-1")), page.items());
        assertEquals("2", page.nextCheckpoint().value().orElseThrow());
    }

    public record Item(String value) {}

    public record GeneratedInput(
        String source,
        ResumableSourceDescriptor descriptor,
        OpaqueSourceCheckpoint checkpoint,
        int limit
    ) implements StreamRegionContinuationInput {}

    public record GeneratedPage(List<Item> items, OpaqueSourceCheckpoint nextCheckpoint, boolean endOfSource)
        implements StreamRegionContinuationResult {}

    public static final class DeterministicGeneratedFacade implements StreamRegionContinuation,
        ResumableSourceCapability<String, Item> {

        private static final int SOURCE_SIZE = 7;

        @Override
        public int producerStepIndex() {
            return 0;
        }

        @Override
        public boolean terminalScalarSuffix() {
            return true;
        }

        @Override
        public ResumableSourceDescriptor descriptor() {
            return DESCRIPTOR;
        }

        @Override
        public StreamRegionAwaitBinding awaitBinding() {
            return new StreamRegionAwaitBinding(new AwaitStepDescriptor(
                "await-payment", Item.class.getName(), Item.class.getName(), Duration.ofSeconds(30),
                "interactionId", "local", java.util.Map.of(), List.of()), 1);
        }

        @Override
        public StreamRegionContinuationInput inputFor(Object canonicalSourceInput, OpaqueSourceCheckpoint checkpoint, int limit) {
            return new GeneratedInput((String) canonicalSourceInput, DESCRIPTOR, checkpoint, limit);
        }

        @Override
        public StepOneToOne<GeneratedInput, GeneratedPage> transitionStep() {
            return new DeterministicStep(this);
        }

        @Override
        public io.smallrye.mutiny.Uni<ResumableSourcePage<Item>> readPage(
            String source, OpaqueSourceCheckpoint checkpoint, int limit) {
            int start = checkpoint.value().map(Integer::parseInt).orElse(0);
            int end = Math.min(SOURCE_SIZE, start + limit);
            List<Item> items = java.util.stream.IntStream.range(start, end)
                .mapToObj(index -> new Item(source + "-" + index))
                .toList();
            return io.smallrye.mutiny.Uni.createFrom().item(new ResumableSourcePage<>(items,
                new OpaqueSourceCheckpoint(java.util.Optional.of(Integer.toString(end))), end == SOURCE_SIZE));
        }
    }

    static final class DeterministicStep extends ConfigurableStep implements StepOneToOne<GeneratedInput, GeneratedPage> {
        private final DeterministicGeneratedFacade facade;

        DeterministicStep(DeterministicGeneratedFacade facade) {
            this.facade = facade;
        }

        @Override
        public io.smallrye.mutiny.Uni<GeneratedPage> applyOneToOne(GeneratedInput input) {
            return facade.readPage(input.source(), input.checkpoint(), input.limit())
                .map(page -> new GeneratedPage(page.items(), page.nextCheckpoint(), page.endOfSource()));
        }
    }
}
