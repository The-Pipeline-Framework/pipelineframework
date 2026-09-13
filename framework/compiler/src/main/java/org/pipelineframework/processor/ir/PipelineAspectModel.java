package org.pipelineframework.processor.ir;

import java.util.Map;

/**
 * Contains only semantic information derived from pipeline aspect configurations.
 * This class captures all the essential information needed to apply pipeline aspects.
 *
 * <p>GLOBAL aspects apply to all steps present in the pipeline definition at generation time.
 * Future steps added later require regeneration. STEPS scope is reserved for future extensions.
 * In the current version, STEPS scope MUST be empty or treated as GLOBAL with a warning.</p>
 *
 * <p>When multiple aspects share the same order and position, execution order is deterministic
 * but unspecified; implementations MUST preserve declaration order. Order is a signed integer.
 * Lower values execute closer to the step boundary. Negative values are allowed and execute
 * before zero-valued aspects. BEFORE_STEP aspects always execute before the step, and
 * AFTER_STEP aspects always execute after the step, regardless of order value.</p>
 *
 * @param name Gets the name of the aspect.
 * @param scope Gets the scope of the aspect application (GLOBAL or STEPS).
 * @param position Gets the position of the aspect relative to the step (BEFORE_STEP or AFTER_STEP).
 * @param order Gets the order of application (lower executes closer to the step boundary, default is 0).
 * @param config Gets the free-form configuration for the aspect.
 */
public record PipelineAspectModel(
        String name,
        AspectScope scope,
        AspectPosition position,
        int order,
        Map<String, Object> config
) {
    /**
     * Creates a new PipelineAspectModel instance.
     *
     * @param name the name of the aspect
     * @param scope the scope of the aspect application
     * @param position the position of the aspect relative to the step
     * @param order the order of application (lower executes closer to the step boundary)
     * @param config the free-form configuration for the aspect
     */
    public PipelineAspectModel {
        // Validate non-null invariants
        if (name == null)
            throw new IllegalArgumentException("name cannot be null");
        if (scope == null)
            throw new IllegalArgumentException("scope cannot be null");
        if (position == null)
            throw new IllegalArgumentException("position cannot be null");
        if (config == null)
            throw new IllegalArgumentException("config cannot be null");
    }

    /**
     * Creates a new PipelineAspectModel instance with default order (0).
     *
     * @param name the name of the aspect
     * @param scope the scope of the aspect application
     * @param position the position of the aspect relative to the step
     * @param config the free-form configuration for the aspect
     */
    public PipelineAspectModel(String name, AspectScope scope, AspectPosition position, 
                               Map<String, Object> config) {
        this(name, scope, position, 0, config);
    }
}