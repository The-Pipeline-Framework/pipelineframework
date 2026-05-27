package org.pipelineframework.restaurantapproval.orchestrator.service;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import io.restassured.path.json.JsonPath;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.dto.AwaitCompletionResponseDto;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.dto.ExecutionStatusDto;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

@QuarkusTest
class RestaurantApprovalAwaitMonolithTest {

    private static final String TENANT_ID = "restaurant-demo";
    private static final String AWAIT_STEP_ID = "ProcessAwaitRestaurantDecisionService";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(15);

    @Test
    void acceptedDecisionResumesExecutionToApprovedTerminalState() throws InterruptedException {
        RunAsyncAcceptedDto accepted = submitOrder("Ada Lovelace", "Cafe TPF");
        ExecutionStatusDto waiting = awaitExecutionStatus(accepted.executionId(), ExecutionStatus.WAITING_EXTERNAL);
        assertEquals(2, waiting.stepIndex(), "waiting state should point at the await step that suspended execution");

        PendingInteraction pending = awaitPendingInteraction(accepted.executionId());
        assertEquals(AWAIT_STEP_ID, pending.stepId());
        assertEquals("interaction-api", pending.transportType());

        AwaitCompletionResponseDto completion = completeAccepted(pending);
        assertEquals(pending.interactionId(), completion.interactionId());
        assertEquals(AWAIT_STEP_ID, completion.stepId());
        assertEquals(AwaitInteractionStatus.COMPLETED, completion.status());
        assertFalse(completion.duplicate(), "first completion should not be marked duplicate");

        ExecutionStatusDto succeeded = awaitExecutionStatus(accepted.executionId(), ExecutionStatus.SUCCEEDED);
        assertEquals(3, succeeded.stepIndex(), "execution should resume at the final step after await completion");

        JsonPath result = resultPayload(accepted.executionId());
        assertEquals(pending.orderId(), result.getString("orderId"));
        assertEquals("APPROVED", result.getString("outcome"));
        assertEquals("ACCEPTED", result.getString("restaurantStatus"));
        assertEquals("Approved by Cafe TPF", result.getString("summary"));
        assertNotNull(result.getString("resolvedAt"));
    }

    @Test
    void declinedDecisionResumesExecutionToDeclinedTerminalState() throws InterruptedException {
        RunAsyncAcceptedDto accepted = submitOrder("Grace Hopper", "Bistro Queue");
        awaitExecutionStatus(accepted.executionId(), ExecutionStatus.WAITING_EXTERNAL);

        PendingInteraction pending = awaitPendingInteraction(accepted.executionId());
        AwaitCompletionResponseDto completion = completeDeclined(pending);
        assertEquals(AwaitInteractionStatus.COMPLETED, completion.status());

        awaitExecutionStatus(accepted.executionId(), ExecutionStatus.SUCCEEDED);

        JsonPath result = resultPayload(accepted.executionId());
        assertEquals(pending.orderId(), result.getString("orderId"));
        assertEquals("DECLINED", result.getString("outcome"));
        assertEquals("DECLINED", result.getString("restaurantStatus"));
        assertEquals("Need more prep time (Kitchen is overloaded tonight)", result.getString("summary"));
        assertNotNull(result.getString("resolvedAt"));
    }

    private static RunAsyncAcceptedDto submitOrder(String customerName, String restaurantName) {
        String requestId = UUID.randomUUID().toString();
        Map<String, Object> request = Map.of(
            "requestId", requestId,
            "customerName", customerName,
            "restaurantName", restaurantName,
            "items", "Margherita Pizza, Sparkling Water",
            "totalAmount", "27.50",
            "currency", "EUR");

        return given()
            .header("x-tenant-id", TENANT_ID)
            .header("Idempotency-Key", "order-" + requestId)
            .contentType(ContentType.JSON)
            .body(request)
            .when()
            .post("/pipeline/run-async")
            .then()
            .statusCode(200)
            .extract()
            .as(RunAsyncAcceptedDto.class);
    }

    private static ExecutionStatusDto awaitExecutionStatus(String executionId, ExecutionStatus targetStatus)
            throws InterruptedException {
        Instant deadline = Instant.now().plus(POLL_TIMEOUT);
        ExecutionStatusDto last = null;
        while (Instant.now().isBefore(deadline)) {
            last = given()
                .header("x-tenant-id", TENANT_ID)
                .accept(ContentType.JSON)
                .when()
                .get("/pipeline/executions/{executionId}", executionId)
                .then()
                .statusCode(200)
                .extract()
                .as(ExecutionStatusDto.class);
            if (last.status() == targetStatus) {
                return last;
            }
            if (last.status() == ExecutionStatus.FAILED || last.status() == ExecutionStatus.DLQ) {
                throw new AssertionError("execution moved to terminal failure: " + last.status() + " / "
                    + last.errorCode() + " / " + last.errorMessage());
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("execution " + executionId + " did not reach " + targetStatus + "; last status=" + last);
    }

    private static PendingInteraction awaitPendingInteraction(String executionId) throws InterruptedException {
        Instant deadline = Instant.now().plus(POLL_TIMEOUT);
        while (Instant.now().isBefore(deadline)) {
            List<Map<String, Object>> interactions = given()
                .header("x-tenant-id", TENANT_ID)
                .queryParam("stepId", AWAIT_STEP_ID)
                .accept(ContentType.JSON)
                .when()
                .get("/pipeline/interactions/pending")
                .then()
                .statusCode(200)
                .extract()
                .jsonPath()
                .getList("$");
            for (Map<String, Object> interaction : interactions) {
                if (executionId.equals(interaction.get("executionId"))) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> requestPayload = (Map<String, Object>) interaction.get("requestPayload");
                    return new PendingInteraction(
                        String.valueOf(interaction.get("interactionId")),
                        String.valueOf(interaction.get("stepId")),
                        String.valueOf(interaction.get("transportType")),
                        String.valueOf(requestPayload.get("orderId")));
                }
            }
            Thread.sleep(100L);
        }
        throw new AssertionError("no pending interaction found for execution " + executionId);
    }

    private static JsonPath resultPayload(String executionId) {
        return given()
            .header("x-tenant-id", TENANT_ID)
            .accept(ContentType.JSON)
            .when()
            .get("/pipeline/executions/{executionId}/result", executionId)
            .then()
            .statusCode(200)
            .extract()
            .jsonPath();
    }

    private static AwaitCompletionResponseDto completeAccepted(PendingInteraction pending) {
        Map<String, Object> request = Map.of(
            "interactionId", pending.interactionId(),
            "idempotencyKey", "complete-" + pending.interactionId(),
            "actor", "restaurant-demo-ui",
            "responsePayload", Map.of(
                "accepted", Map.of(
                    "orderId", pending.orderId(),
                    "decidedAt", Instant.now().toString(),
                    "note", "Approved by Cafe TPF")));

        return given()
            .header("x-tenant-id", TENANT_ID)
            .contentType(ContentType.JSON)
            .body(request)
            .when()
            .post("/pipeline/interactions/complete")
            .then()
            .statusCode(200)
            .extract()
            .as(AwaitCompletionResponseDto.class);
    }

    private static AwaitCompletionResponseDto completeDeclined(PendingInteraction pending) {
        Map<String, Object> request = Map.of(
            "interactionId", pending.interactionId(),
            "idempotencyKey", "complete-" + pending.interactionId(),
            "actor", "restaurant-demo-ui",
            "responsePayload", Map.of(
                "declined", Map.of(
                    "orderId", pending.orderId(),
                    "decidedAt", Instant.now().toString(),
                    "note", "Need more prep time",
                    "declineReason", "Kitchen is overloaded tonight")));

        return given()
            .header("x-tenant-id", TENANT_ID)
            .contentType(ContentType.JSON)
            .body(request)
            .when()
            .post("/pipeline/interactions/complete")
            .then()
            .statusCode(200)
            .extract()
            .as(AwaitCompletionResponseDto.class);
    }

    private record PendingInteraction(String interactionId, String stepId, String transportType, String orderId) {
    }
}
