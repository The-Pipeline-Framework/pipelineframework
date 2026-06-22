package org.pipelineframework.csv.common.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.File;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.SerializedTransitionPayload;

class CsvPaymentsInputFileSerializationTest {

  @Test
  void queueSnapshotRoundTripsInputFile() {
    CsvPaymentsInputFile input = new CsvPaymentsInputFile(new File("/tmp/payments.csv"));
    JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();

    SerializedTransitionPayload serialized =
        codec.encode(new ExecutionInputSnapshot(ExecutionInputShape.UNI, input));
    assertFalse(serialized.payload().contains("sourceName"));

    ExecutionInputSnapshot decoded = assertInstanceOf(
        ExecutionInputSnapshot.class,
        codec.decode(serialized));
    CsvPaymentsInputFile decodedInput = assertInstanceOf(CsvPaymentsInputFile.class, decoded.payload());

    assertEquals(Path.of("/tmp/payments.csv"), decodedInput.getFilepath());
  }
}
