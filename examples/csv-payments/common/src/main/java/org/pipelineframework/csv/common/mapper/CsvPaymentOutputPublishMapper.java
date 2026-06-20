package org.pipelineframework.csv.common.mapper;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.objectpublish.ObjectPayload;
import org.pipelineframework.objectpublish.ObjectPublishMapper;

/**
 * Renders terminal CSV payment outputs as grouped CSV object payloads.
 */
public final class CsvPaymentOutputPublishMapper implements ObjectPublishMapper<PaymentOutput> {

    @Override
    public String groupKey(PaymentOutput item) {
        Path inputFile = inputFile(item);
        Path fileName = inputFile.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("Payment output input file path must have a file name");
        }
        return fileName.toString();
    }

    @Override
    public ObjectPayload render(String groupKey, List<PaymentOutput> items) {
        try {
            StringWriter writer = new StringWriter();
            new StatefulBeanToCsvBuilder<PaymentOutput>(writer)
                .withQuotechar('\'')
                .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                .build()
                .write(items.iterator());
            return new ObjectPayload(
                writer.toString().getBytes(StandardCharsets.UTF_8),
                "text/csv",
                Map.of("recordCount", String.valueOf(items.size())));
        } catch (Exception e) {
            throw new IllegalStateException("Failed rendering CSV payment output object for group: " + groupKey, e);
        }
    }

    private Path inputFile(PaymentOutput item) {
        if (item == null) {
            throw new IllegalArgumentException("Payment output must not be null");
        }
        PaymentStatus status = item.getPaymentStatus();
        if (status == null) {
            throw new IllegalArgumentException("Payment output must include paymentStatus");
        }
        PaymentRecord record = status.getPaymentRecord();
        if (record == null) {
            throw new IllegalArgumentException("Payment output paymentStatus must include paymentRecord");
        }
        Path inputFile = record.getCsvPaymentsInputFilePath();
        if (inputFile == null) {
            throw new IllegalArgumentException("Payment output paymentRecord must include csvPaymentsInputFilePath");
        }
        return inputFile.toAbsolutePath().normalize();
    }
}
