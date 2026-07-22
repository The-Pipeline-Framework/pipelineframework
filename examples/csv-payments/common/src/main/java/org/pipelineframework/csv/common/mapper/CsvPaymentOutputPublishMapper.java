package org.pipelineframework.csv.common.mapper;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import org.pipelineframework.objectpublish.ObjectPayloadChunk;
import org.pipelineframework.objectpublish.ObjectPublishGroupRenderer;
import org.pipelineframework.objectpublish.StreamingObjectPublishMapper;

/**
 * Renders terminal CSV payment outputs as grouped CSV object payloads.
 */
public final class CsvPaymentOutputPublishMapper
    implements StreamingObjectPublishMapper<org.pipelineframework.csv.domain.PaymentOutput> {

    @Override
    public String groupKey(org.pipelineframework.csv.domain.PaymentOutput item) {
        if (item == null) {
            throw new IllegalArgumentException("Payment output must not be null");
        }
        if (item.csvPaymentsOutputFilename() != null && !item.csvPaymentsOutputFilename().isBlank()) {
            return item.csvPaymentsOutputFilename();
        }
        Path inputFile = item.csvPaymentsInputFilePath();
        if (inputFile == null || inputFile.getFileName() == null) {
            throw new IllegalArgumentException(
                "Payment output must include csvPaymentsOutputFilename or csvPaymentsInputFilePath");
        }
        return inputFile.getFileName().toString();
    }

    @Override
    public ObjectPublishGroupRenderer<org.pipelineframework.csv.domain.PaymentOutput> openGroup(
        String groupKey, org.pipelineframework.csv.domain.PaymentOutput firstItem) {
        return new CsvPaymentOutputGroupRenderer(groupKey);
    }

    private static final class CsvPaymentOutputGroupRenderer
        implements ObjectPublishGroupRenderer<org.pipelineframework.csv.domain.PaymentOutput> {
        private final String groupKey;
        private final StringWriter writer = new StringWriter();
        private final StatefulBeanToCsv<org.pipelineframework.csv.common.domain.PaymentOutput> csv;
        private long recordCount;

        private CsvPaymentOutputGroupRenderer(String groupKey) {
            this.groupKey = groupKey;
            this.csv = new StatefulBeanToCsvBuilder<org.pipelineframework.csv.common.domain.PaymentOutput>(writer)
                .withQuotechar('\'')
                .withSeparator(CSVWriter.DEFAULT_SEPARATOR)
                .build();
        }

        @Override
        public String contentType() {
            return "text/csv";
        }

        @Override
        public ObjectPayloadChunk onItem(org.pipelineframework.csv.domain.PaymentOutput item) {
            try {
                csv.write(new PaymentOutputPersistenceMapper().toExternal(item));
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
