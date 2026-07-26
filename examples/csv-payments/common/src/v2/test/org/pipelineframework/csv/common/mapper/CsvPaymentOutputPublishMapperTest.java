package org.pipelineframework.csv.common.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.objectpublish.ObjectPublishGroupRenderer;

class CsvPaymentOutputPublishMapperTest {

    @TempDir
    Path tempDir;

    @Test
    void groupsByInputFilenameAndRendersCsvPayload() {
        CsvPaymentOutputPublishMapper mapper = new CsvPaymentOutputPublishMapper();
        PaymentOutput output = paymentOutput(tempDir.resolve("payments.csv"), "csv-1", "Alice", "100.00");

        String groupKey = mapper.groupKey(output);
        ObjectPublishGroupRenderer<PaymentOutput> renderer = mapper.openGroup(groupKey, output);
        String csv = new String(renderer.onItem(output).bytes(), StandardCharsets.UTF_8);

        assertEquals("payments.csv", groupKey);
        assertEquals("text/csv", renderer.contentType());
        assertEquals("1", renderer.finalMetadata().get("recordCount"));
        assertTrue(csv.contains("AMOUNT"));
        assertTrue(csv.contains("CSV ID"));
        assertTrue(csv.contains("RECIPIENT"));
        assertTrue(csv.contains("STATUS"));
        assertTrue(csv.contains("100.00"));
        assertTrue(csv.contains("csv-1"));
        assertTrue(csv.contains("Alice"));
    }

    @Test
    void streamsRowsWithoutRepeatingHeader() {
        CsvPaymentOutputPublishMapper mapper = new CsvPaymentOutputPublishMapper();
        PaymentOutput first = paymentOutput(tempDir.resolve("payments.csv"), "csv-1", "Alice", "100.00");
        PaymentOutput second = paymentOutput(tempDir.resolve("payments.csv"), "csv-2", "Bob", "200.00");
        ObjectPublishGroupRenderer<PaymentOutput> renderer = mapper.openGroup("payments.csv", first);

        String csv = new String(renderer.onItem(first).bytes(), StandardCharsets.UTF_8)
            + new String(renderer.onItem(second).bytes(), StandardCharsets.UTF_8);

        assertEquals(1, occurrences(csv, "AMOUNT"));
        assertEquals("2", renderer.finalMetadata().get("recordCount"));
        assertTrue(csv.contains("csv-1"));
        assertTrue(csv.contains("csv-2"));
    }

    private PaymentOutput paymentOutput(Path inputFile, String csvId, String recipient, String amount) {
        PaymentOutput output = new PaymentOutput();
        output.setCsvPaymentsOutputFilename(inputFile.getFileName().toString());
        output.setCsvPaymentsInputFilePath(inputFile);
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

    private int occurrences(String value, String token) {
        int count = 0;
        int index = 0;
        while ((index = value.indexOf(token, index)) >= 0) {
            count++;
            index += token.length();
        }
        return count;
    }
}
