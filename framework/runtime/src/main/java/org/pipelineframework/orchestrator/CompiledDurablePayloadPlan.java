package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.pipelineframework.config.pipeline.PipelineJson;

/** Immutable release-bound encoder/decoder plan; construction is not a steady-state payload operation. */
public record CompiledDurablePayloadPlan(
    CanonicalPayloadBinding binding,
    ObjectWriter writer,
    ObjectReader reader,
    Map<String, UnionCasePlan> unionCases,
    Optional<MethodHandle> discriminator
) {
    public record UnionCasePlan(Class<?> runtimeClass, ObjectWriter writer, ObjectReader reader) {
    }

    public static CompiledDurablePayloadPlan compile(CanonicalPayloadBinding binding) {
        Map<String, UnionCasePlan> unionCases = unionCases(binding.runtimeClass());
        return new CompiledDurablePayloadPlan(
            binding,
            PipelineJson.mapper().writerFor(binding.runtimeType()),
            PipelineJson.mapper().readerFor(binding.runtimeType()),
            unionCases,
            discriminator(binding.runtimeClass(), unionCases));
    }

    public boolean isUnion() {
        return !unionCases.isEmpty();
    }

    private static Map<String, UnionCasePlan> unionCases(Class<?> runtimeClass) {
        if (!runtimeClass.isSealed()) {
            return Map.of();
        }
        Map<String, UnionCasePlan> cases = new TreeMap<>();
        for (Class<?> candidate : runtimeClass.getPermittedSubclasses()) {
            cases.put(candidate.getSimpleName(), new UnionCasePlan(
                candidate,
                PipelineJson.mapper().writerFor(candidate),
                PipelineJson.mapper().readerFor(candidate)));
        }
        return Map.copyOf(cases);
    }

    private static Optional<MethodHandle> discriminator(Class<?> runtimeClass, Map<String, UnionCasePlan> unionCases) {
        if (unionCases.isEmpty()) {
            return Optional.empty();
        }
        try {
            Method method = runtimeClass.getMethod("discriminator");
            return Optional.of(MethodHandles.privateLookupIn(runtimeClass, MethodHandles.lookup()).unreflect(method));
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Canonical union " + runtimeClass.getName()
                + " does not expose its authored discriminator", e);
        }
    }
}
