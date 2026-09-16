package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TransitionWorkerProtocolContractTest {

    @Test
    void pinsRestRoutesAndSignatureCanonicalization() {
        byte[] body = "{\"outcome\":\"COMPLETED\"}".getBytes(StandardCharsets.UTF_8);

        String signature = TransitionWorkerSignature.sign(
            "worker-secret",
            RestTransitionWorkerProtocol.EXECUTE_METHOD,
            RestTransitionWorkerProtocol.EXECUTE_PATH,
            "2026-09-14T10:15:30Z",
            "nonce-1",
            body);

        assertEquals("/pipeline/worker/transitions/execute", RestTransitionWorkerProtocol.EXECUTE_PATH);
        assertEquals("/pipeline/worker/capabilities", RestTransitionWorkerProtocol.CAPABILITIES_PATH);
        assertEquals(
            "e22366b38e2b071ccc7e31df9d7c4f7ad13d0b6ac17d2ef0b0f43e89cfb53523",
            signature);
        assertTrue(TransitionWorkerSignature.matches(signature, signature));
        assertFalse(TransitionWorkerSignature.matches(signature, signature + "00"));
        assertEquals(1789380930000L, TransitionWorkerSignature.parseTimestamp("2026-09-14T10:15:30Z"));
    }

    @Test
    void pinsGrpcAndSqsProtocolIdentity() {
        assertEquals("1", GrpcTransitionWorkerProtocol.PROTOCOL_VERSION);
        assertEquals(TransitionPayloadEncoding.JSON, GrpcTransitionWorkerProtocol.PAYLOAD_ENCODING);
        assertEquals("1", SqsTransitionWorkerProtocol.PROTOCOL_VERSION);
        assertEquals(
            "application/tpf-transition-envelope+json",
            SqsTransitionWorkerProtocol.PAYLOAD_ENCODING);
        assertArrayEquals(
            "request-1\n{}".getBytes(StandardCharsets.UTF_8),
            SqsTransitionWorkerProtocol.signedBytes("request-1", "{}"));
    }

    @Test
    void validatesSignedSqsEnvelopeFields() {
        assertThrows(NullPointerException.class, () -> new SqsTransitionWorkerRequest(
            null, "1", "json", "{}", "timestamp", "nonce", "signature"));
        assertThrows(NullPointerException.class, () -> new SqsTransitionWorkerResponse(
            "request-1", "1", "json", "{}", "timestamp", "nonce", null));
    }

    @Test
    void shorterWorkerConstructorPreservesCommandReissueReason() {
        TransitionWorkerCommand command = new TransitionWorkerCommand(
            "tenant-1",
            "execution-1",
            2,
            1,
            ExecutionResultShape.SINGLE,
            4L,
            "transition-1",
            "payload",
            ExecutionRedriveIntent.REISSUE_COMMAND,
            Optional.of("command-1"),
            Optional.of("operator approved reissue"));

        assertEquals(ExecutionRedriveIntent.REISSUE_COMMAND, command.redriveIntent());
        assertEquals(-1, command.redriveStepIndex());
        assertEquals(Optional.of("operator approved reissue"), command.redriveReason());
    }

    @Test
    void retryTargetMustPrecedeExclusiveStopBoundary() {
        TransitionWorkerCommand validCommand = new TransitionWorkerCommand(
            "tenant-1", "execution-1", 2, 4, 1, ExecutionResultShape.SINGLE, 4L,
            "transition-1", "payload", ExecutionRedriveIntent.RETRY_FAILED_COMMAND, 3,
            Optional.of("command-1"), Optional.empty());
        assertEquals(3, validCommand.redriveStepIndex());

        assertThrows(IllegalArgumentException.class, () -> new TransitionWorkerCommand(
            "tenant-1", "execution-1", 2, 4, 1, ExecutionResultShape.SINGLE, 4L,
            "transition-1", "payload", ExecutionRedriveIntent.RETRY_FAILED_COMMAND, 4,
            Optional.of("command-1"), Optional.empty()));
        assertThrows(IllegalArgumentException.class, () -> new TransitionCommandEnvelope(
            "tenant-1", "execution-1", "pipeline-1", "contract-1", "release-1",
            2, 4, 1, ExecutionResultShape.SINGLE, 4L, "transition-1", "trace-1",
            "example.Payload", "JSON", "{}", ExecutionRedriveIntent.RETRY_FAILED_COMMAND, 4,
            Optional.of("command-1"), Optional.empty()));
    }
}
