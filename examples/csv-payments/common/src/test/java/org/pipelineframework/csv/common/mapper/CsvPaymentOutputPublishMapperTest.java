package org.pipelineframework.csv.common.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Currency;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.objectpublish.ObjectPayload;

class CsvPaymentOutputPublishMapperTest {

    @TempDir
    Path tempDir;

    @Test
    void groupsByInputFilenameAndRendersCsvPayload() {
        CsvPaymentOutputPublishMapper mapper = new CsvPaymentOutputPublishMapper();
        PaymentOutput output = paymentOutput(tempDir.resolve("payments.csv"), "csv-1", "Alice", "100.00");

        String groupKey = mapper.groupKey(output);
        ObjectPayload payload = mapper.render(groupKey, List.of(output));
        String csv = new String(payload.bytes(), StandardCharsets.UTF_8);

        assertEquals("payments.csv", groupKey);
        assertEquals("text/csv", payload.contentType());
        assertEquals("1", payload.metadata().get("recordCount"));
        assertTrue(csv.contains("AMOUNT"));
        assertTrue(csv.contains("CSV ID"));
        assertTrue(csv.contains("RECIPIENT"));
        assertTrue(csv.contains("STATUS"));
        assertTrue(csv.contains("100.00"));
        assertTrue(csv.contains("csv-1"));
        assertTrue(csv.contains("Alice"));
    }

    private PaymentOutput paymentOutput(Path inputFile, String csvId, String recipient, String amount) {
        PaymentRecord record = new PaymentRecord();
        record.setCsvPaymentsInputFilePath(inputFile);
        record.setCsvId(csvId);
        record.setRecipient(recipient);
        record.setAmount(new BigDecimal(amount));
        record.setCurrency(Currency.getInstance("USD"));

        PaymentStatus status = new PaymentStatus();
        status.setPaymentRecord(record);
        status.setConversationId(UUID.randomUUID());
        status.setStatusCode(1000L);
        status.setStatus("APPROVED");
        status.setMessage("Success");

        PaymentOutput output = new PaymentOutput();
        output.setPaymentStatus(status);
        output.setCsvId(csvId);
        output.setRecipient(recipient);
        output.setAmount(new BigDecimal(amount));
        output.setCurrency(Currency.getInstance("USD"));
        output.setConversationId(UUID.randomUUID());
        output.setStatus(1000L);
        output.setMessage("Success");
        output.setFee(BigDecimal.ZERO);
        return output;
    }
}
