package org.pipelineframework.checkout.common.e2e;

import org.pipelineframework.checkout.common.connector.ConnectorUtils;
import org.pipelineframework.checkout.common.connector.IdempotencyGuard;
import org.pipelineframework.transport.function.FunctionTransportContext;
import org.pipelineframework.transport.function.LocalManyToOneFunctionInvokeAdapter;
import org.pipelineframework.transport.function.LocalOneToManyFunctionInvokeAdapter;
import org.pipelineframework.transport.function.TraceEnvelope;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CanonicalFtgoSyncFlowTest {

    @Test
    void executesCanonicalFlowHappyPathWithDeterministicSplitMergeLineage() {
        PlaceOrderRequest request = new PlaceOrderRequest(
            deterministicUuid("request", "happy"),
            deterministicUuid("customer", "happy"),
            deterministicUuid("restaurant", "happy"),
            "burger x1,fries x1,soda x1",
            new BigDecimal("42.50"),
            "EUR");

        OrderPending pending = checkoutCreatePending(checkoutValidate(request));
        OrderApproved approved = consumerValidate(pending);
        OrderAcceptedByRestaurant accepted = restaurantAccept(approved);

        FunctionTransportContext kitchenContext = FunctionTransportContext.of(
            request.requestId().toString(),
            "kitchen-preparation",
            "Kitchen Expand Tasks");
        TraceEnvelope<OrderAcceptedByRestaurant> acceptedEnvelope = TraceEnvelope.root(
            request.requestId().toString(),
            deterministicItemId("accepted", accepted.orderId().toString()),
            "OrderAcceptedByRestaurant",
            "v1",
            ConnectorUtils.deterministicHandoffKey("accepted", accepted.orderId().toString()),
            accepted);

        LocalOneToManyFunctionInvokeAdapter<OrderAcceptedByRestaurant, KitchenTask> expandAdapter =
            new LocalOneToManyFunctionInvokeAdapter<>(
                payload -> Multi.createFrom().iterable(expandKitchenTasks(payload)),
                "KitchenTask",
                "v1");
        List<TraceEnvelope<KitchenTask>> expanded = expandAdapter
            .invokeOneToMany(acceptedEnvelope, kitchenContext)
            .collect().asList()
            .await().atMost(Duration.ofSeconds(2));
        assertEquals(3, expanded.size());
        assertTrue(expanded.stream().allMatch(envelope ->
            envelope.previousItemRef() != null
                && acceptedEnvelope.itemId().equals(envelope.previousItemRef().previousItemId())));

        LocalManyToOneFunctionInvokeAdapter<KitchenTask, OrderReadyForDispatch> reduceAdapter =
            new LocalManyToOneFunctionInvokeAdapter<>(
                payloads -> payloads.collect().asList().onItem().transform(this::reduceKitchenTasks),
                "OrderReadyForDispatch",
                "v1");

        TraceEnvelope<OrderReadyForDispatch> reducedForward = reduceAdapter
            .invokeManyToOne(Multi.createFrom().iterable(expanded), kitchenContext)
            .await().atMost(Duration.ofSeconds(2));

        List<TraceEnvelope<KitchenTask>> reversed = new ArrayList<>(expanded);
        java.util.Collections.reverse(reversed);
        TraceEnvelope<OrderReadyForDispatch> reducedReverse = reduceAdapter
            .invokeManyToOne(Multi.createFrom().iterable(reversed), kitchenContext)
            .await().atMost(Duration.ofSeconds(2));

        assertEquals(reducedForward.itemId(), reducedReverse.itemId());
        assertEquals(reducedForward.idempotencyKey(), reducedReverse.idempotencyKey());
        assertEquals(reducedForward.payload().lineageDigest(), reducedReverse.payload().lineageDigest());
        assertEquals(
            reducedForward.meta().get("previousItemIds"),
            reducedReverse.meta().get("previousItemIds"));

        DeliveryAssigned assigned = dispatchAssign(reducedForward.payload());
        OrderDelivered delivered = deliveryExecute(assigned);
        assertEquals(reducedForward.payload().lineageDigest(), assigned.lineageDigest());
        assertEquals(assigned.lineageDigest(), delivered.lineageDigest());
        assertEquals(assigned.restaurantId(), delivered.restaurantId());
        assertEquals(assigned.kitchenTicketId(), delivered.kitchenTicketId());
        PaymentOutcome paymentOutcome = paymentCapture(delivered);
        assertTrue(paymentOutcome instanceof PaymentCaptured);
        assertEquals(new BigDecimal("42.50"), ((PaymentCaptured) paymentOutcome).amount());
    }

    @Test
    void executesCanonicalFailurePathAndCompensationWithStableConnectorSignatures() {
        PlaceOrderRequest request = new PlaceOrderRequest(
            deterministicUuid("request", "fail"),
            deterministicUuid("customer", "fail"),
            deterministicUuid("restaurant", "fail"),
            "burger x1",
            BigDecimal.ZERO,
            "EUR");

        OrderPending pending = checkoutCreatePending(checkoutValidate(request));
        OrderApproved approved = consumerValidate(pending);
        OrderAcceptedByRestaurant accepted = restaurantAccept(approved);
        OrderReadyForDispatch ready = reduceKitchenTasks(expandKitchenTasks(accepted));
        DeliveryAssigned assigned = dispatchAssign(ready);
        OrderDelivered delivered = deliveryExecute(assigned);
        PaymentOutcome paymentOutcome = paymentCapture(delivered);
        assertTrue(paymentOutcome instanceof PaymentFailed);

        FailureTerminal terminal = compensate((PaymentFailed) paymentOutcome);
        assertEquals(delivered.orderId(), terminal.orderId());
        assertEquals("PAYMENT_CAPTURE_REJECTED", terminal.failureCode());
        assertEquals("manual-review", terminal.resolutionAction());
    }

    @Test
    void suppressesDuplicateReplayAcrossCanonicalBoundaries() {
        IdempotencyGuard guard = new IdempotencyGuard(256);
        List<String> boundaries = List.of(
            "checkout->consumer",
            "consumer->restaurant",
            "restaurant->kitchen",
            "kitchen->dispatch",
            "dispatch->delivery",
            "delivery->payment",
            "payment->compensation");
        String orderId = deterministicUuid("order", "dedup").toString();

        Map<String, AtomicInteger> forwardedByBoundary = new LinkedHashMap<>();
        boundaries.forEach(boundary -> forwardedByBoundary.put(boundary, new AtomicInteger(0)));

        for (String boundary : boundaries) {
            String key = ConnectorUtils.deterministicHandoffKey(boundary, orderId, "v1");
            if (guard.markIfNew(key)) {
                forwardedByBoundary.get(boundary).incrementAndGet();
            }
        }
        for (String boundary : boundaries) {
            String key = ConnectorUtils.deterministicHandoffKey(boundary, orderId, "v1");
            if (guard.markIfNew(key)) {
                forwardedByBoundary.get(boundary).incrementAndGet();
            }
        }

        forwardedByBoundary.forEach((boundary, counter) ->
            assertEquals(1, counter.get(), "Boundary forwarded more than once: " + boundary));

        String signature = ConnectorUtils.failureSignature(
            "delivery->payment",
            "forward",
            "bad;reason=line\nbreak",
            "trace=abc",
            "item\\123");
        String expectedSignature =
            "connector=delivery->payment;phase=forward;reason=bad\\;reason\\=line\\nbreak;"
                + "traceId=trace\\=abc;itemId=item\\\\123";
        assertEquals(expectedSignature, signature);
    }

    private ValidatedOrderRequest checkoutValidate(PlaceOrderRequest request) {
        return new ValidatedOrderRequest(
            request.requestId(),
            request.customerId(),
            request.restaurantId(),
            request.items(),
            request.totalAmount(),
            request.currency(),
            deterministicInstant("validated", request.requestId().toString()));
    }

    private OrderPending checkoutCreatePending(ValidatedOrderRequest validated) {
        return new OrderPending(
            deterministicUuid("pending", validated.requestId().toString()),
            validated.requestId(),
            validated.customerId(),
            validated.restaurantId(),
            validated.totalAmount(),
            validated.currency(),
            deterministicInstant("pending", validated.requestId().toString()));
    }

    private OrderApproved consumerValidate(OrderPending pending) {
        return new OrderApproved(
            pending.orderId(),
            pending.requestId(),
            pending.customerId(),
            pending.restaurantId(),
            pending.totalAmount(),
            pending.currency(),
            deterministicInstant("approved", pending.orderId().toString()),
            pending.totalAmount().compareTo(new BigDecimal("100.00")) > 0 ? "HIGH" : "LOW");
    }

    private OrderAcceptedByRestaurant restaurantAccept(OrderApproved approved) {
        return new OrderAcceptedByRestaurant(
            approved.orderId(),
            approved.requestId(),
            approved.customerId(),
            approved.restaurantId(),
            approved.totalAmount(),
            approved.currency(),
            deterministicInstant("accepted", approved.orderId().toString()),
            deterministicUuid("kitchen-ticket", approved.orderId().toString()));
    }

    private List<KitchenTask> expandKitchenTasks(OrderAcceptedByRestaurant accepted) {
        List<String> taskNames = List.of("prep", "cook", "pack");
        List<KitchenTask> tasks = new ArrayList<>(taskNames.size());
        for (int i = 0; i < taskNames.size(); i++) {
            String taskName = taskNames.get(i);
            tasks.add(new KitchenTask(
                accepted.orderId(),
                accepted.customerId(),
                accepted.restaurantId(),
                accepted.totalAmount(),
                accepted.currency(),
                accepted.kitchenTicketId(),
                deterministicUuid("task", accepted.orderId().toString(), Integer.toString(i)),
                taskName,
                "DONE"));
        }
        return tasks;
    }

    private OrderReadyForDispatch reduceKitchenTasks(List<KitchenTask> tasks) {
        List<KitchenTask> ordered = tasks.stream()
            .sorted(Comparator.comparing(task -> task.taskId().toString()))
            .toList();
        KitchenTask seed = ordered.getFirst();
        String lineageDigest = ordered.stream()
            .map(task -> task.taskId().toString())
            .map(value -> "#" + value.length() + ":" + value)
            .collect(Collectors.joining());
        return new OrderReadyForDispatch(
            seed.orderId(),
            seed.customerId(),
            seed.restaurantId(),
            seed.totalAmount(),
            seed.currency(),
            deterministicInstant("ready", seed.orderId().toString()),
            seed.kitchenTicketId(),
            lineageDigest);
    }

    private DeliveryAssigned dispatchAssign(OrderReadyForDispatch ready) {
        return new DeliveryAssigned(
            ready.orderId(),
            ready.customerId(),
            ready.restaurantId(),
            ready.totalAmount(),
            ready.currency(),
            ready.kitchenTicketId(),
            deterministicUuid("dispatch", ready.orderId().toString()),
            deterministicUuid("courier", ready.orderId().toString()),
            17,
            deterministicInstant("assigned", ready.orderId().toString()),
            ready.lineageDigest());
    }

    private OrderDelivered deliveryExecute(DeliveryAssigned assigned) {
        return new OrderDelivered(
            assigned.orderId(),
            assigned.customerId(),
            assigned.dispatchId(),
            assigned.courierId(),
            assigned.restaurantId(),
            assigned.kitchenTicketId(),
            deterministicInstant("delivered", assigned.orderId().toString()),
            assigned.totalAmount(),
            assigned.currency(),
            assigned.lineageDigest());
    }

    private PaymentOutcome paymentCapture(OrderDelivered delivered) {
        if (delivered.amount().compareTo(BigDecimal.ZERO) <= 0) {
            return new PaymentFailed(
                delivered.orderId(),
                "PAYMENT_CAPTURE_REJECTED",
                "amount must be > 0",
                deterministicInstant("payment-failed", delivered.orderId().toString()));
        }
        return new PaymentCaptured(
            delivered.orderId(),
            deterministicUuid("payment", delivered.orderId().toString()),
            deterministicInstant("payment-captured", delivered.orderId().toString()),
            delivered.amount(),
            delivered.currency());
    }

    private FailureTerminal compensate(PaymentFailed failed) {
        return new FailureTerminal(
            failed.orderId(),
            failed.failureCode(),
            failed.failureReason(),
            deterministicInstant("failure-terminal", failed.orderId().toString()),
            "manual-review");
    }

    private static UUID deterministicUuid(String namespace, String... components) {
        String seed = namespace + "|" + String.join("|", components);
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    }

    private static Instant deterministicInstant(String namespace, String value) {
        int hash = Math.abs((namespace + "|" + value).hashCode());
        return Instant.ofEpochSecond(1_700_000_000L + (hash % 10_000_000L));
    }

    private static String deterministicItemId(String namespace, String value) {
        return ConnectorUtils.deterministicHandoffKey(namespace, value);
    }

    private sealed interface PaymentOutcome permits PaymentCaptured, PaymentFailed {
    }

    private record PlaceOrderRequest(
        UUID requestId,
        UUID customerId,
        UUID restaurantId,
        String items,
        BigDecimal totalAmount,
        String currency
    ) {
    }

    private record ValidatedOrderRequest(
        UUID requestId,
        UUID customerId,
        UUID restaurantId,
        String items,
        BigDecimal totalAmount,
        String currency,
        Instant validatedAt
    ) {
    }

    private record OrderPending(
        UUID orderId,
        UUID requestId,
        UUID customerId,
        UUID restaurantId,
        BigDecimal totalAmount,
        String currency,
        Instant createdAt
    ) {
    }

    private record OrderApproved(
        UUID orderId,
        UUID requestId,
        UUID customerId,
        UUID restaurantId,
        BigDecimal totalAmount,
        String currency,
        Instant approvedAt,
        String riskBand
    ) {
    }

    private record OrderAcceptedByRestaurant(
        UUID orderId,
        UUID requestId,
        UUID customerId,
        UUID restaurantId,
        BigDecimal totalAmount,
        String currency,
        Instant acceptedAt,
        UUID kitchenTicketId
    ) {
    }

    private record KitchenTask(
        UUID orderId,
        UUID customerId,
        UUID restaurantId,
        BigDecimal totalAmount,
        String currency,
        UUID kitchenTicketId,
        UUID taskId,
        String taskName,
        String taskStatus
    ) {
    }

    private record OrderReadyForDispatch(
        UUID orderId,
        UUID customerId,
        UUID restaurantId,
        BigDecimal totalAmount,
        String currency,
        Instant readyAt,
        UUID kitchenTicketId,
        String lineageDigest
    ) {
    }

    private record DeliveryAssigned(
        UUID orderId,
        UUID customerId,
        UUID restaurantId,
        BigDecimal totalAmount,
        String currency,
        UUID kitchenTicketId,
        UUID dispatchId,
        UUID courierId,
        int etaMinutes,
        Instant assignedAt,
        String lineageDigest
    ) {
    }

    private record OrderDelivered(
        UUID orderId,
        UUID customerId,
        UUID dispatchId,
        UUID courierId,
        UUID restaurantId,
        UUID kitchenTicketId,
        Instant deliveredAt,
        BigDecimal amount,
        String currency,
        String lineageDigest
    ) {
    }

    private record PaymentCaptured(
        UUID orderId,
        UUID paymentId,
        Instant capturedAt,
        BigDecimal amount,
        String currency
    ) implements PaymentOutcome {
    }

    private record PaymentFailed(
        UUID orderId,
        String failureCode,
        String failureReason,
        Instant failedAt
    ) implements PaymentOutcome {
    }

    private record FailureTerminal(
        UUID orderId,
        String failureCode,
        String failureReason,
        Instant resolvedAt,
        String resolutionAction
    ) {
    }
}
