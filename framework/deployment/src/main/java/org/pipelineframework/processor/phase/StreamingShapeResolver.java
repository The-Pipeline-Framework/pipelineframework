package org.pipelineframework.processor.phase;

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
        if ("EXPANSION".equalsIgnoreCase(cardinality)) {
            return StreamingShape.UNARY_STREAMING;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return StreamingShape.STREAMING_UNARY;
        }
        if ("MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return StreamingShape.STREAMING_STREAMING;
        }
        return StreamingShape.UNARY_UNARY;
    }

    /**
     * Checks if the input cardinality is streaming.
     *
     * @param cardinality the cardinality string
     * @return true if the input is streaming, false otherwise
     */
    static boolean isStreamingInputCardinality(String cardinality) {
        return "REDUCTION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality);
    }

    /**
     * Applies cardinality to determine if streaming should continue.
     *
     * @param cardinality the cardinality string
     * @param currentStreaming the current streaming state
     * @return the updated streaming state
     */
    static boolean applyCardinalityToStreaming(String cardinality, boolean currentStreaming) {
        if ("EXPANSION".equalsIgnoreCase(cardinality) || "MANY_TO_MANY".equalsIgnoreCase(cardinality)) {
            return true;
        }
        if ("REDUCTION".equalsIgnoreCase(cardinality)) {
            return false;
        }
        return currentStreaming;
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