package org.pipelineframework.config;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CardinalitySemanticsTest {

    @Test
    void canonicalNormalizesAliases() {
        assertEquals(CardinalitySemantics.ONE_TO_ONE, CardinalitySemantics.fromString("ONE_TO_ONE"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.fromString("EXPANSION"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.fromString("expansion"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.fromString("Expansion"));
        assertEquals(CardinalitySemantics.MANY_TO_ONE, CardinalitySemantics.fromString("REDUCTION"));
        assertEquals(CardinalitySemantics.ONE_TO_MANY, CardinalitySemantics.fromString("one_to_many"));
        assertEquals(CardinalitySemantics.MANY_TO_ONE, CardinalitySemantics.fromString("many_to_one"));
        assertEquals(CardinalitySemantics.MANY_TO_MANY, CardinalitySemantics.fromString("MANY_TO_MANY"));
    }

    @Test
    void canonicalHandlesNullAndRejectsInvalidInputs() {
        assertNull(CardinalitySemantics.fromString(null));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.fromString(""));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.fromString("   "));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.fromString("INVALID"));
    }

    @Test
    void streamingInputSemantics() {
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.isStreamingInput(null));
        assertFalse(CardinalitySemantics.isStreamingInput("ONE_TO_ONE"));
        assertFalse(CardinalitySemantics.isStreamingInput("ONE_TO_MANY"));
        assertTrue(CardinalitySemantics.isStreamingInput("REDUCTION"));
        assertTrue(CardinalitySemantics.isStreamingInput("MANY_TO_ONE"));
        assertTrue(CardinalitySemantics.isStreamingInput("MANY_TO_MANY"));
        assertTrue(CardinalitySemantics.isStreamingInput("many_to_many"));
        assertFalse(CardinalitySemantics.isStreamingInput("EXPANSION"));
    }

    @Test
    void applyToOutputStreamingSemantics() {
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.applyToOutputStreaming(null, true));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("ONE_TO_ONE", false));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("ONE_TO_ONE", true));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("EXPANSION", false));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("ONE_TO_MANY", false));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("REDUCTION", true));
        assertFalse(CardinalitySemantics.applyToOutputStreaming("MANY_TO_ONE", true));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("MANY_TO_MANY", true));
        assertTrue(CardinalitySemantics.applyToOutputStreaming("MANY_TO_MANY", false));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.applyToOutputStreaming("UNKNOWN", true));
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.applyToOutputStreaming("UNKNOWN", false));
    }

    @Test
    void invocationShapeDistinguishesPointwiseAndStreamScopedExpansion() {
        CardinalitySemantics.InvocationShape pointwise = CardinalitySemantics.ONE_TO_MANY.invocationShape();
        CardinalitySemantics.InvocationShape streamScoped = CardinalitySemantics.MANY_TO_MANY.invocationShape();

        assertEquals(pointwise.unaryInputOutput(), streamScoped.unaryInputOutput());
        assertEquals(pointwise.streamingInputOutput(), streamScoped.streamingInputOutput());
        assertFalse(pointwise.streamScoped());
        assertTrue(streamScoped.streamScoped());
    }

    @Test
    void composeOneToOneThenOneToOneRemainsPointwiseOneToOne() {
        assertEquals(CardinalitySemantics.ONE_TO_ONE,
            CardinalitySemantics.compose(List.of(
                CardinalitySemantics.ONE_TO_ONE,
                CardinalitySemantics.ONE_TO_ONE)));
    }

    @Test
    void composeOneToManyThenManyToOneBecomesManyToOne() {
        assertEquals(CardinalitySemantics.MANY_TO_ONE,
            CardinalitySemantics.compose(List.of(
                CardinalitySemantics.ONE_TO_MANY,
                CardinalitySemantics.MANY_TO_ONE)));
    }

    @Test
    void composeManyToOneThenOneToManyBecomesManyToMany() {
        assertEquals(CardinalitySemantics.MANY_TO_MANY,
            CardinalitySemantics.compose(List.of(
                CardinalitySemantics.MANY_TO_ONE,
                CardinalitySemantics.ONE_TO_MANY)));
    }

    @Test
    void composeOneToManyThenManyToManyPreservesStreamScopedManyToMany() {
        assertEquals(CardinalitySemantics.MANY_TO_MANY,
            CardinalitySemantics.compose(List.of(
                CardinalitySemantics.ONE_TO_MANY,
                CardinalitySemantics.MANY_TO_MANY)));
    }

    @Test
    void composeLongMixedChainPreservesAggregateScopeAndOutputShape() {
        assertEquals(CardinalitySemantics.MANY_TO_ONE,
            CardinalitySemantics.compose(List.of(
                CardinalitySemantics.ONE_TO_ONE,
                CardinalitySemantics.ONE_TO_MANY,
                CardinalitySemantics.MANY_TO_ONE,
                CardinalitySemantics.ONE_TO_ONE)));
    }

    @Test
    void composeIsClosedForEverySequenceThroughDepthSeven() {
        Set<CardinalitySemantics> observed = EnumSet.noneOf(CardinalitySemantics.class);
        int[] evaluated = {0};
        for (int depth = 1; depth <= 7; depth++) {
            composeAll(depth, new ArrayList<>(), observed, evaluated);
        }

        assertEquals(21_844, evaluated[0]);
        assertEquals(EnumSet.allOf(CardinalitySemantics.class), observed);
    }

    @Test
    void composeRejectsEmptyAndNullSequences() {
        assertThrows(IllegalArgumentException.class, () -> CardinalitySemantics.compose(List.of()));
        assertThrows(NullPointerException.class, () -> CardinalitySemantics.compose(null));
        assertThrows(NullPointerException.class, () -> CardinalitySemantics.compose(
            java.util.Arrays.asList(CardinalitySemantics.ONE_TO_ONE, null)));
    }

    private static void composeAll(
        int remaining,
        List<CardinalitySemantics> prefix,
        Set<CardinalitySemantics> observed,
        int[] evaluated
    ) {
        if (remaining == 0) {
            observed.add(CardinalitySemantics.compose(prefix));
            evaluated[0]++;
            return;
        }
        for (CardinalitySemantics cardinality : CardinalitySemantics.values()) {
            prefix.add(cardinality);
            composeAll(remaining - 1, prefix, observed, evaluated);
            prefix.remove(prefix.size() - 1);
        }
    }
}
