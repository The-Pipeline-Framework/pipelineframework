package org.pipelineframework;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.cache.CacheKeyStrategy;
import org.pipelineframework.cache.CacheKeyTarget;
import org.pipelineframework.cache.CachePolicyViolation;
import org.pipelineframework.cache.PipelineCacheReader;
import org.pipelineframework.command.CommandConnector;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandDuplicatePolicy;
import org.pipelineframework.command.CommandEffectStatus;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.command.CommandRequest;
import org.pipelineframework.command.CommandStep;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.InMemoryCommandEffectStore;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommandCacheReplayConformanceTest {

    private static final CommandDescriptor DESCRIPTOR = new CommandDescriptor(
        "ProcessExternalCommandService",
        "external-command",
        CommandInput.class.getName(),
        CommandOutput.class.getName(),
        StableCommandIdGenerator.class.getName(),
        CommandDuplicatePolicy.RETURN_RECORDED,
        Map.of());

    private final InMemoryCommandEffectStore effectStore = new InMemoryCommandEffectStore();
    private final RecordingConnector connector = new RecordingConnector();
    private final CommandStepSupport commandSupport = new CommandStepSupport(
        List.of(connector),
        List.of(effectStore),
        queueAsyncConfig());
    private final GeneratedLikeCommandStep commandStep = new GeneratedLikeCommandStep(commandSupport);

    @AfterEach
    void clearContext() {
        AwaitExecutionContextHolder.clear();
        effectStore.clear();
    }

    @Test
    void preferCacheColdExecutesCommandEffectSemantics() {
        CountingReader cache = new CountingReader(Map.of());

        CommandOutput output = run(cache, "prefer-cache", "v1");

        assertEquals("cmd-item", output.commandId());
        assertEquals(1, cache.getCalls.get());
        assertEquals(1, connector.calls.get());
        assertEquals(CommandEffectStatus.SUCCEEDED, effect("cmd-item"));
    }

    @Test
    void preferCacheWarmReplaysPipelineOutputWithoutFabricatingEffect() {
        CommandOutput cached = new CommandOutput("cached-observation");
        CountingReader cache = new CountingReader(Map.of("v1:key", cached));

        CommandOutput output = run(cache, "prefer-cache", "v1");

        assertEquals(cached, output);
        assertEquals(1, cache.getCalls.get());
        assertEquals(0, connector.calls.get());
        assertFalse(effectStore.find("tenant", "cmd-item").await().indefinitely().isPresent());
    }

    @Test
    void requireCacheWarmReplaysWithoutCommandRuntime() {
        CommandOutput cached = new CommandOutput("required-observation");
        CountingReader cache = new CountingReader(Map.of("v1:key", cached));

        assertEquals(cached, run(cache, "require-cache", "v1"));
        assertEquals(0, connector.calls.get());
        assertFalse(effectStore.find("tenant", "cmd-item").await().indefinitely().isPresent());
    }

    @Test
    void requireCacheColdFailsBeforeCommandRuntime() {
        CountingReader cache = new CountingReader(Map.of());

        assertThrows(CachePolicyViolation.class, () -> run(cache, "require-cache", "v1"));
        assertEquals(0, connector.calls.get());
        assertFalse(effectStore.find("tenant", "cmd-item").await().indefinitely().isPresent());
    }

    @Test
    void cacheOnlyIgnoresWarmEntryAndExecutesCommandRuntime() {
        CountingReader cache = new CountingReader(Map.of("v1:key", new CommandOutput("old")));

        CommandOutput output = run(cache, "cache-only", "v1");

        assertEquals("cmd-item", output.commandId());
        assertEquals(0, cache.getCalls.get());
        assertEquals(1, connector.calls.get());
        assertEquals(CommandEffectStatus.SUCCEEDED, effect("cmd-item"));
    }

    @Test
    void bypassCachePerformsNoCacheIoAndStableCommandIdStillReplaysEffect() {
        CountingReader cache = new CountingReader(Map.of("v1:key", new CommandOutput("old")));

        CommandOutput first = run(cache, "bypass-cache", "v1");
        CommandOutput replayed = run(cache, "bypass-cache", "v2");

        assertEquals(first, replayed);
        assertEquals(0, cache.getCalls.get());
        assertEquals(0, cache.existsCalls.get());
        assertEquals(1, connector.calls.get());
        assertEquals(CommandEffectStatus.SUCCEEDED, effect("cmd-item"));
    }

    @Test
    void skipIfPresentFailsBeforeCacheOrCommandRuntime() {
        CountingReader cache = new CountingReader(Map.of("v1:key", new CommandOutput("old")));

        assertThrows(CachePolicyViolation.class, () -> run(cache, "skip-if-present", "v1"));

        assertEquals(0, cache.getCalls.get());
        assertEquals(0, cache.existsCalls.get());
        assertEquals(0, connector.calls.get());
        assertFalse(effectStore.find("tenant", "cmd-item").await().indefinitely().isPresent());
    }

    @Test
    void versionTagIsPipelineReplayIdentityNotCommandEffectIdentity() {
        CountingReader cache = new CountingReader(Map.of("v1:key", new CommandOutput("v1-observation")));

        assertEquals("v1-observation", run(cache, "prefer-cache", "v1").commandId());
        assertEquals("cmd-item", run(cache, "prefer-cache", "v2").commandId());

        assertEquals(2, cache.getCalls.get());
        assertEquals(1, connector.calls.get());
        assertEquals(CommandEffectStatus.SUCCEEDED, effect("cmd-item"));
    }

    private CommandOutput run(CountingReader reader, String policy, String version) {
        AwaitExecutionContext executionContext = new AwaitExecutionContext("tenant", "execution", 1);
        PipelineRunner.CacheReadSupport cacheSupport = new PipelineRunner.CacheReadSupport(
            reader,
            List.of(new CommandCacheKeyStrategy()),
            policy);
        Object result = PipelineStepExecutor.applyOneToOneUnchecked(
            commandStep,
            Uni.createFrom().item(new CommandInput("item")),
            false,
            1,
            null,
            null,
            cacheSupport,
            new PipelineContext(version, null, policy),
            executionContext);
        return ((Uni<CommandOutput>) result).await().indefinitely();
    }

    private CommandEffectStatus effect(String commandId) {
        return effectStore.find("tenant", commandId)
            .await().indefinitely()
            .orElseThrow()
            .status();
    }

    private static PipelineOrchestratorConfig queueAsyncConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
        return config;
    }

    record CommandInput(String id) {
    }

    record CommandOutput(String commandId) {
    }

    static final class StableCommandIdGenerator implements CommandIdGenerator<CommandInput> {
        @Override
        public String commandId(CommandDescriptor descriptor, CommandInput input) {
            return "cmd-" + input.id();
        }
    }

    static final class RecordingConnector implements CommandConnector<CommandInput, CommandOutput> {
        final AtomicInteger calls = new AtomicInteger();

        @Override
        public String command() {
            return "external-command";
        }

        @Override
        public Uni<CommandOutput> execute(CommandRequest<CommandInput> request) {
            calls.incrementAndGet();
            return Uni.createFrom().item(new CommandOutput(request.commandId()));
        }
    }

    static final class GeneratedLikeCommandStep extends ConfigurableStep
        implements StepOneToOne<CommandInput, CommandOutput>, CommandStep, CacheKeyTarget {

        private final CommandStepSupport support;

        GeneratedLikeCommandStep(CommandStepSupport support) {
            this.support = support;
        }

        @Override
        public Uni<CommandOutput> applyOneToOne(CommandInput input) {
            return support.execute(DESCRIPTOR, new StableCommandIdGenerator(), input);
        }

        @Override
        public Class<?> cacheKeyTargetType() {
            return CommandOutput.class;
        }
    }

    static final class CommandCacheKeyStrategy implements CacheKeyStrategy {
        @Override
        public Optional<String> resolveKey(Object item, PipelineContext context) {
            return Optional.of("key");
        }

        @Override
        public boolean supportsTarget(Class<?> targetType) {
            return targetType == CommandOutput.class;
        }
    }

    static final class CountingReader implements PipelineCacheReader {
        private final Map<String, Object> values;
        final AtomicInteger getCalls = new AtomicInteger();
        final AtomicInteger existsCalls = new AtomicInteger();

        CountingReader(Map<String, Object> values) {
            this.values = Map.copyOf(values);
        }

        @Override
        public Uni<Optional<Object>> get(String key) {
            getCalls.incrementAndGet();
            return Uni.createFrom().item(Optional.ofNullable(values.get(key)));
        }

        @Override
        public Uni<Boolean> exists(String key) {
            existsCalls.incrementAndGet();
            return Uni.createFrom().item(values.containsKey(key));
        }
    }
}
