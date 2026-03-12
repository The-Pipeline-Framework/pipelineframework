/*
 * Copyright (c) 2023-2026 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ConnectorIdempotencyTrackerTest {

    @Test
    void tryAcquireWithDisabledPolicyAlwaysReturnsTrue() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.DISABLED));
        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.DISABLED));
        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.DISABLED));
    }

    @Test
    void tryAcquireWithPreForwardPolicyReturnsTrueOnFirstAttempt() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
    }

    @Test
    void tryAcquireWithPreForwardPolicyReturnsFalseOnDuplicate() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
    }

    @Test
    void tryAcquireWithPreForwardPolicyTracksDifferentKeysIndependently() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertTrue(tracker.tryAcquire("key-2", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertFalse(tracker.tryAcquire("key-2", ConnectorIdempotencyPolicy.PRE_FORWARD));
    }

    @Test
    void tryAcquireWithOnAcceptPolicyReturnsTrueOnFirstAttempt() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void tryAcquireWithOnAcceptPolicyReturnsFalseWhenAlreadyInFlight() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void tryAcquireWithOnAcceptPolicyReturnsFalseWhenAlreadyAccepted() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void markAcceptedMovesKeyFromInFlightToAccepted() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
        assertTrue(tracker.containsInFlight("key-1"));
        assertFalse(tracker.containsAccepted("key-1"));

        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);

        assertFalse(tracker.containsInFlight("key-1"));
        assertTrue(tracker.containsAccepted("key-1"));
    }

    @Test
    void markAcceptedIsNoOpWhenPreForwardAlreadyAccepted() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD);
        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD);

        // PRE_FORWARD immediately records the key as accepted, so markAccepted has no additional effect.
        assertTrue(tracker.containsAccepted("key-1"));
        assertFalse(tracker.containsInFlight("key-1"));
    }

    @Test
    void markAcceptedWithDisabledPolicyDoesNothing() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.DISABLED);

        assertFalse(tracker.containsAccepted("key-1"));
        assertFalse(tracker.containsInFlight("key-1"));
    }

    @Test
    void clearReservationsRemovesAllInFlightKeys() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);
        tracker.tryAcquire("key-2", ConnectorIdempotencyPolicy.ON_ACCEPT);
        tracker.tryAcquire("key-3", ConnectorIdempotencyPolicy.ON_ACCEPT);

        assertTrue(tracker.containsInFlight("key-1"));
        assertTrue(tracker.containsInFlight("key-2"));
        assertTrue(tracker.containsInFlight("key-3"));

        tracker.clearReservations();

        assertFalse(tracker.containsInFlight("key-1"));
        assertFalse(tracker.containsInFlight("key-2"));
        assertFalse(tracker.containsInFlight("key-3"));
    }

    @Test
    void clearReservationsDoesNotRemoveAcceptedKeys() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);
        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);
        tracker.tryAcquire("key-2", ConnectorIdempotencyPolicy.PRE_FORWARD);

        tracker.clearReservations();

        assertTrue(tracker.containsAccepted("key-1"));
        assertTrue(tracker.containsAccepted("key-2"));
    }

    @Test
    void preForwardAcceptedKeysEvictOldestEntryWhenCapacityExceeded() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(3);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertTrue(tracker.tryAcquire("key-2", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertTrue(tracker.tryAcquire("key-3", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertTrue(tracker.tryAcquire("key-4", ConnectorIdempotencyPolicy.PRE_FORWARD));

        assertFalse(tracker.containsAccepted("key-1"));
        assertTrue(tracker.containsAccepted("key-2"));
        assertTrue(tracker.containsAccepted("key-3"));
        assertTrue(tracker.containsAccepted("key-4"));
        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD));
    }

    @Test
    void onAcceptAcceptedKeysEvictOldestEntryWhenCapacityExceeded() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(3);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);
        assertTrue(tracker.tryAcquire("key-2", ConnectorIdempotencyPolicy.ON_ACCEPT));
        tracker.markAccepted("key-2", ConnectorIdempotencyPolicy.ON_ACCEPT);
        assertTrue(tracker.tryAcquire("key-3", ConnectorIdempotencyPolicy.ON_ACCEPT));
        tracker.markAccepted("key-3", ConnectorIdempotencyPolicy.ON_ACCEPT);
        assertTrue(tracker.tryAcquire("key-4", ConnectorIdempotencyPolicy.ON_ACCEPT));
        tracker.markAccepted("key-4", ConnectorIdempotencyPolicy.ON_ACCEPT);

        assertFalse(tracker.containsAccepted("key-1"));
        assertTrue(tracker.containsAccepted("key-2"));
        assertTrue(tracker.containsAccepted("key-3"));
        assertTrue(tracker.containsAccepted("key-4"));
        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void containsAcceptedReturnsTrueForAcceptedKeys() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.PRE_FORWARD);

        assertTrue(tracker.containsAccepted("key-1"));
    }

    @Test
    void containsAcceptedReturnsFalseForNonAcceptedKeys() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertFalse(tracker.containsAccepted("key-1"));
    }

    @Test
    void containsInFlightReturnsTrueForInFlightKeys() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);

        assertTrue(tracker.containsInFlight("key-1"));
    }

    @Test
    void containsInFlightReturnsFalseForNonInFlightKeys() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertFalse(tracker.containsInFlight("key-1"));
    }

    @Test
    void tryAcquireRejectsNullKeyWhenNotDisabled() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(NullPointerException.class, () ->
            tracker.tryAcquire(null, ConnectorIdempotencyPolicy.PRE_FORWARD));
    }

    @Test
    void tryAcquireRejectsBlankKeyWhenNotDisabled() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(IllegalArgumentException.class, () ->
            tracker.tryAcquire("", ConnectorIdempotencyPolicy.PRE_FORWARD));
        assertThrows(IllegalArgumentException.class, () ->
            tracker.tryAcquire("   ", ConnectorIdempotencyPolicy.PRE_FORWARD));
    }

    @Test
    void tryAcquireRejectsNullPolicy() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(NullPointerException.class, () ->
            tracker.tryAcquire("key-1", null));
    }

    @Test
    void markAcceptedRejectsNullKeyWhenNotDisabled() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(NullPointerException.class, () ->
            tracker.markAccepted(null, ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void markAcceptedRejectsBlankKeyWhenNotDisabled() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(IllegalArgumentException.class, () ->
            tracker.markAccepted("", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void markAcceptedRejectsNullPolicy() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(NullPointerException.class, () ->
            tracker.markAccepted("key-1", null));
    }

    @Test
    void containsAcceptedRejectsNullKey() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(NullPointerException.class, () ->
            tracker.containsAccepted(null));
    }

    @Test
    void containsAcceptedRejectsBlankKey() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(IllegalArgumentException.class, () ->
            tracker.containsAccepted(""));
    }

    @Test
    void containsInFlightRejectsNullKey() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(NullPointerException.class, () ->
            tracker.containsInFlight(null));
    }

    @Test
    void containsInFlightRejectsBlankKey() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertThrows(IllegalArgumentException.class, () ->
            tracker.containsInFlight(""));
    }

    @Test
    void trackerAcquiresDifferentKeysSequentially() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        for (int i = 0; i < 50; i++) {
            assertTrue(tracker.tryAcquire("key-" + i, ConnectorIdempotencyPolicy.PRE_FORWARD));
        }

        for (int i = 0; i < 50; i++) {
            assertFalse(tracker.tryAcquire("key-" + i, ConnectorIdempotencyPolicy.PRE_FORWARD));
        }
    }

    @Test
    void onAcceptPolicyAllowsRetryAfterClearReservations() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));

        tracker.clearReservations();

        assertTrue(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }

    @Test
    void acceptedKeysRemainAcceptedAfterClearReservations() {
        ConnectorIdempotencyTracker tracker = new ConnectorIdempotencyTracker(100);

        tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);
        tracker.markAccepted("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT);

        tracker.clearReservations();

        assertTrue(tracker.containsAccepted("key-1"));
        assertFalse(tracker.tryAcquire("key-1", ConnectorIdempotencyPolicy.ON_ACCEPT));
    }
}
