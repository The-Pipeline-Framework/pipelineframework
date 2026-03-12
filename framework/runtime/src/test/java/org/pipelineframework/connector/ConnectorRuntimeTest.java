package org.pipelineframework.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class ConnectorRuntimeTest {

    @Test
    void suppressesDuplicatesPreForward() throws InterruptedException {
        List<ConnectorRecord<TestPayload>> accepted = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);
        ConnectorTarget<TestPayload> target = connectorTarget(accepted, latch);
        ConnectorRuntime<TestPayload, TestPayload> runtime = new ConnectorRuntime<>(
            "test-pre-forward",
            () -> Multi.createFrom().items(
                ConnectorRecord.ofPayload(new TestPayload("order-1", "customer-1", "ready-1")),
                ConnectorRecord.ofPayload(new TestPayload("order-1", "customer-1", "ready-1")),
                ConnectorRecord.ofPayload(new TestPayload("order-2", "customer-2", "ready-2"))),
            target,
            record -> record.withDispatchMetadata(
                ConnectorSupport.ensureDispatchMetadata(
                    record.dispatchMetadata(),
                    "test-pre-forward",
                    record.payload(),
                    List.of("orderId", "customerId", "readyAt"))),
            new ConnectorPolicy(true, ConnectorBackpressurePolicy.BUFFER, 16,
                ConnectorIdempotencyPolicy.PRE_FORWARD, ConnectorFailureMode.PROPAGATE),
            new ConnectorIdempotencyTracker(16),
            null,
            null,
            null);

        Cancellable subscription = runtime.start();
        try {
            await(latch, "accepted records for pre-forward duplicate suppression");

            assertEquals(2, accepted.size());
            assertEquals("order-1", accepted.get(0).payload().orderId());
            assertEquals("order-2", accepted.get(1).payload().orderId());
        } finally {
            subscription.cancel();
        }
    }

    @Test
    void clearsReservationsForOnAcceptFailures() throws InterruptedException {
        List<ConnectorRecord<TestPayload>> attempts = new CopyOnWriteArrayList<>();
        AtomicInteger failureCallbacks = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(2);
        ConnectorTarget<TestPayload> target = new ConnectorTarget<>() {
            @Override
            public Cancellable forward(Multi<ConnectorRecord<TestPayload>> connectorStream) {
                throw new UnsupportedOperationException("Use callback-aware overload");
            }

            @Override
            public Cancellable forward(
                Multi<ConnectorRecord<TestPayload>> connectorStream,
                Consumer<ConnectorRecord<TestPayload>> onAccepted,
                Consumer<Throwable> onFailure
            ) {
                return connectorStream.subscribe().with(item -> {
                    attempts.add(item);
                    if (attempts.size() == 1) {
                        failureCallbacks.incrementAndGet();
                        onFailure.accept(new IllegalStateException("forced failure"));
                        latch.countDown();
                        return;
                    }
                    onAccepted.accept(item);
                    latch.countDown();
                });
            }
        };

        ConnectorRuntime<TestPayload, TestPayload> runtime = new ConnectorRuntime<>(
            "test-on-accept",
            () -> Multi.createFrom().items(
                ConnectorRecord.ofPayload(new TestPayload("order-1", "customer-1", "ready-1")),
                ConnectorRecord.ofPayload(new TestPayload("order-1", "customer-1", "ready-1"))),
            target,
            record -> record.withDispatchMetadata(
                ConnectorSupport.ensureDispatchMetadata(
                    record.dispatchMetadata(),
                    "test-on-accept",
                    record.payload(),
                    List.of("orderId", "customerId", "readyAt"))),
            new ConnectorPolicy(true, ConnectorBackpressurePolicy.BUFFER, 16,
                ConnectorIdempotencyPolicy.ON_ACCEPT, ConnectorFailureMode.PROPAGATE),
            new ConnectorIdempotencyTracker(16),
            null,
            null,
            null);

        Cancellable subscription = runtime.start();
        try {
            await(latch, "connector attempts after on-accept failure");

            assertEquals(2, attempts.size());
            assertEquals(1, failureCallbacks.get());
        } finally {
            subscription.cancel();
        }
    }

    @Test
    void logsAndContinuesWhenConfiguredForMappingFailures() throws InterruptedException {
        List<ConnectorRecord<TestPayload>> accepted = new CopyOnWriteArrayList<>();
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch acceptedLatch = new CountDownLatch(1);
        ConnectorTarget<TestPayload> target = connectorTarget(accepted, acceptedLatch);
        ConnectorRuntime<TestPayload, TestPayload> runtime = new ConnectorRuntime<>(
            "test-mapping-failure",
            () -> Multi.createFrom().items(
                ConnectorRecord.ofPayload(new TestPayload("bad", "customer-1", "ready-1")),
                ConnectorRecord.ofPayload(new TestPayload("good", "customer-2", "ready-2"))),
            target,
            record -> {
                if ("bad".equals(record.payload().orderId())) {
                    throw new IllegalStateException("bad payload");
                }
                return record.withDispatchMetadata(
                    ConnectorSupport.ensureDispatchMetadata(
                        record.dispatchMetadata(),
                        "test-mapping-failure",
                        record.payload(),
                        List.of("orderId")));
            },
            new ConnectorPolicy(true, ConnectorBackpressurePolicy.BUFFER, 16,
                ConnectorIdempotencyPolicy.PRE_FORWARD, ConnectorFailureMode.LOG_AND_CONTINUE),
            new ConnectorIdempotencyTracker(16),
            null,
            null,
            failure -> failures.incrementAndGet());

        Cancellable subscription = runtime.start();
        try {
            await(acceptedLatch, "accepted records after mapping failure");

            assertEquals(1, accepted.size());
            assertEquals("good", accepted.get(0).payload().orderId());
            assertEquals(1, failures.get());
        } finally {
            subscription.cancel();
        }
    }

    @Test
    void reportsPropagatedMappingFailureOnlyOnce() throws InterruptedException {
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch failureLatch = new CountDownLatch(1);
        ConnectorRuntime<TestPayload, TestPayload> runtime = new ConnectorRuntime<>(
            "test-mapping-failure-propagate",
            () -> Multi.createFrom().item(ConnectorRecord.ofPayload(new TestPayload("bad", "customer-1", "ready-1"))),
            connectorTarget(new CopyOnWriteArrayList<>(), new CountDownLatch(1)),
            record -> {
                throw new IllegalStateException("bad payload");
            },
            new ConnectorPolicy(true, ConnectorBackpressurePolicy.BUFFER, 16,
                ConnectorIdempotencyPolicy.PRE_FORWARD, ConnectorFailureMode.PROPAGATE),
            new ConnectorIdempotencyTracker(16),
            null,
            null,
            failure -> {
                failures.incrementAndGet();
                failureLatch.countDown();
            });

        Cancellable subscription = runtime.start();
        try {
            await(failureLatch, "single propagated mapping failure callback");
            assertEquals(1, failures.get());
        } finally {
            subscription.cancel();
        }
    }

    private static ConnectorTarget<TestPayload> connectorTarget(
        List<ConnectorRecord<TestPayload>> accepted,
        CountDownLatch latch
    ) {
        return connectorStream -> connectorStream.subscribe().with(item -> {
            accepted.add(item);
            latch.countDown();
        });
    }

    private static void await(CountDownLatch latch, String description) throws InterruptedException {
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for " + description);
    }

    private record TestPayload(String orderId, String customerId, String readyAt) {
    }
}
