package org.pipelineframework.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CardinalitySemanticsTest {

    @Test
    void canonicalNormalizesAliases() {
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.canonical("EXPANSION"));
        assertEquals(CardinalitySemantics.MANY_TO_ONE, CardinalitySemantics.canonical("REDUCTION"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.canonical("one_to_many"));
        assertEquals(CardinalitySemantics.MANY_TO_ONE, CardinalitySemantics.canonical("many_to_one"));
        assertEquals(CardinalitySemantics.MANY_TO_MANY, CardinalitySemantics.canonical("MANY_TO_MANY"));
    }

    @Test
    void streamingStateFollowsCanonicalCardinality() {
        assertTrue(CardinalitySemantics.isStreamingInput("REDUCTION"));
        assertTrue(CardinalitySemantics.isStreamingInput("many_to_many"));
        assertFalse(CardinalitySemantics.isStreamingInput("EXPANSION"));

        assertTrue(CardinalitySemantics.applyToOutputStreaming("EXPANSION", false));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("ONE_TO_MANY", false));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("REDUCTION", true));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("MANY_TO_ONE", true));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("UNKNOWN", true));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("UNKNOWN", false));
    }
}
