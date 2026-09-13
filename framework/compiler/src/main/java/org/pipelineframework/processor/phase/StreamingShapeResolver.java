package org.pipelineframework.processor.phase;

import org.pipelineframework.config.CardinalitySemantics;
import org.pipelineframework.processor.ir.StreamingShape;

/**
 * Resolves streaming shapes based on cardinality and other factors.
 */
class StreamingShapeResolver {
    /**
     * Determines the streaming shape based on cardinality.
     *
     * @param cardinality the cardinality string
     * @return the corresponding streaming shape
     */
    static StreamingShape streamingShape(String cardinality) {
        CardinalitySemantics canonical = CardinalitySemantics.fromString(cardinality);
        if (canonical == CardinalitySemantics.ONE_TO_MANY) {
            return StreamingShape.UNARY_STREAMING;
        }
        if (canonical == CardinalitySemantics.MANY_TO_ONE) {
            return StreamingShape.STREAMING_UNARY;
        }
        if (canonical == CardinalitySemantics.MANY_TO_MANY) {
            return StreamingShape.STREAMING_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }

    /**
     * Checks if the input cardinality is streaming.
     *
     * <p>This evaluates the input side only. ONE_TO_MANY is intentionally excluded because
     * its input remains unary; only output becomes streaming. See
     * {@link #applyCardinalityToStreaming(String, boolean)} for output streaming transitions.
     *
     * @param cardinality the cardinality string
     * @return true if the input is streaming, false otherwise
     */
    static boolean isStreamingInputCardinality(String cardinality) {
        return CardinalitySemantics.isStreamingInput(cardinality);
    }

    /**
     * Applies cardinality to determine if streaming should continue.
     *
     * @param cardinality the cardinality string
     * @param currentStreaming the current streaming state
     * @return the updated streaming state
     */
    static boolean applyCardinalityToStreaming(String cardinality, boolean currentStreaming) {
        return CardinalitySemantics.applyToOutputStreaming(cardinality, currentStreaming);
    }

    /**
     * Calculates the combined streaming shape based on input and output streaming states.
     *
     * @param inputStreaming whether the input is streaming
     * @param outputStreaming whether the output is streaming
     * @return the combined streaming shape
     */
    static StreamingShape streamingShape(boolean inputStreaming, boolean outputStreaming) {
        if (inputStreaming && outputStreaming) {
            return StreamingShape.STREAMING_STREAMING;
        }
        if (inputStreaming) {
            return StreamingShape.STREAMING_UNARY;
        }
        if (outputStreaming) {
            return StreamingShape.UNARY_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }
}
