package org.pipelineframework.csv.common.mapper;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import org.pipelineframework.csv.common.domain.PaymentOutput;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.domain.PaymentStatus;
import org.pipelineframework.objectpublish.ObjectPayloadChunk;
import org.pipelineframework.objectpublish.ObjectPublishGroupRenderer;
import org.pipelineframework.objectpublish.StreamingObjectPublishMapper;

/**
 * Renders terminal CSV payment outputs as grouped CSV object payloads.
 */
public final class CsvPaymentOutputPublishMapper implements StreamingObjectPublishMapper<PaymentOutput> {

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
    public ObjectPublishGroupRenderer<PaymentOutput> openGroup(String groupKey, PaymentOutput firstItem) {
        return new CsvPaymentOutputGroupRenderer(groupKey);
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

    private static final class CsvPaymentOutputGroupRenderer implements ObjectPublishGroupRenderer<PaymentOutput> {
        private final String groupKey;
        private final StringWriter writer = new StringWriter();
        private final StatefulBeanToCsv<PaymentOutput> csv;
        private long recordCount;

        private CsvPaymentOutputGroupRenderer(String groupKey) {
            this.groupKey = groupKey;
            this.csv = new StatefulBeanToCsvBuilder<PaymentOutput>(writer)
                .withQuotechar('\'')
                .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                .build();
        }

        @Override
        public String contentType() {
            return "text/csv";
        }

        @Override
        public ObjectPayloadChunk onItem(PaymentOutput item) {
            try {
                csv.write(item);
                recordCount++;
                return drain();
            } catch (Exception e) {
                throw new IllegalStateException("Failed rendering CSV payment output object for group: " + groupKey, e);
            }
        }

        @Override
        public Map<String, String> finalMetadata() {
            return Map.of("recordCount", String.valueOf(recordCount));
        }

        private ObjectPayloadChunk drain() {
            String rendered = writer.toString();
            writer.getBuffer().setLength(0);
            return new ObjectPayloadChunk(rendered.getBytes(StandardCharsets.UTF_8));
        }
    }
}
