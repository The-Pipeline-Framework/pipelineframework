package org.pipelineframework.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CardinalitySemanticsTest {

    @Test
    void canonicalNormalizesAliases() {
        assertEquals(CardinalitySemantics.ONE_TO_ONE, CardinalitySemantics.canonical("ONE_TO_ONE"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.canonical("EXPANSION"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.canonical("expansion"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.canonical("Expansion"));
        assertEquals(CardinalitySemantics.MANY_TO_ONE, CardinalitySemantics.canonical("REDUCTION"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.canonical("one_to_many"));
        assertEquals(CardinalitySemantics.MANY_TO_ONE, CardinalitySemantics.canonical("many_to_one"));
        assertEquals(CardinalitySemantics.MANY_TO_MANY, CardinalitySemantics.canonical("MANY_TO_MANY"));
    }

    @Test
    void canonicalHandlesNullAndRejectsInvalidInputs() {
        assertNull(CardinalitySemantics.canonical(null));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.canonical(""));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.canonical("   "));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.canonical("INVALID"));
    }

    @Test
    void streamingInputSemanticsTest() {
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.isStreamingInput(null));
        assertFalse(CardinalitySemantics.isStreamingInput("ONE_TO_ONE"));
        assertTrue(CardinalitySemantics.isStreamingInput("REDUCTION"));
        assertTrue(CardinalitySemantics.isStreamingInput("many_to_many"));
        assertFalse(CardinalitySemantics.isStreamingInput("EXPANSION"));
    }

    @Test
    void applyToOutputStreamingSemanticsTest() {
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.applyToOutputStreaming(null, true));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("ONE_TO_ONE", false));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("ONE_TO_ONE", true));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("EXPANSION", false));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("ONE_TO_MANY", false));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("REDUCTION", true));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("MANY_TO_ONE", true));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.applyToOutputStreaming("UNKNOWN", true));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.applyToOutputStreaming("UNKNOWN", false));
    }
}
