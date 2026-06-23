/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.csv.service;

import java.io.Reader;
import java.util.Iterator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.opencsv.bean.CsvToBeanBuilder;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.blocking.CloseableIterator;
import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.util.DemandPacerConfig;
import org.pipelineframework.service.blocking.BlockingIteratorService;

@PipelineStep
@ApplicationScoped
@Getter
public class ProcessCsvPaymentsInputService
    implements BlockingIteratorService<CsvPaymentsInputFile, PaymentRecord> {

  private static final Logger LOG = Logger.getLogger(ProcessCsvPaymentsInputService.class);
  private final long rowsPerPeriod;
  private final long millisPeriod;

    @Inject
    public ProcessCsvPaymentsInputService() {
        rowsPerPeriod = 0L;
        millisPeriod = 0L;
        LOG.info("ProcessCsvPaymentsInputService initialized without legacy demand pacing");
    }

    /**
     * @deprecated CSV reader demand pacing is a legacy fallback and is no longer used by this step.
     */
    @Deprecated(since = "26.6.2", forRemoval = true)
    public ProcessCsvPaymentsInputService(DemandPacerConfig config) {
        rowsPerPeriod = config == null ? 0L : config.rowsPerPeriod();
        millisPeriod = config == null ? 0L : config.millisPeriod();
        LOG.warn("Legacy CSV reader demand pacing configuration is ignored by ProcessCsvPaymentsInputService");
    }

  /**
   * Open a blocking iterator over the CSV records without materializing the full file in memory.
   */
  @Override
  public CloseableIterator<PaymentRecord> iterateBlocking(CsvPaymentsInputFile input) {
    try {
        Reader reader = input.openReader();
        try {
            Iterator<PaymentRecord> delegate =
                new CsvToBeanBuilder<PaymentRecord>(reader)
                    .withType(PaymentRecord.class)
                    .withMappingStrategy(input.veryOwnStrategy())
                    .withSeparator(',')
                    .withIgnoreLeadingWhiteSpace(true)
                    .withIgnoreEmptyLine(true)
                    .build()
                    .iterator();
            return new OpenCsvPaymentRecordIterator(reader, delegate, input, rowsPerPeriod, millisPeriod);
        } catch (Exception e) {
            reader.close();
            throw e;
        }
    } catch (Exception e) {
        LOG.errorf(e, "CSV processing failed for file: %s", input.getSourceName());
        throw new RuntimeException("CSV processing error: " + e.getMessage(), e);
    }
  }

  private static final class OpenCsvPaymentRecordIterator implements CloseableIterator<PaymentRecord> {
    private final Reader reader;
    private final Iterator<PaymentRecord> delegate;
    private final CsvPaymentsInputFile input;
    private final long rowsPerPeriod;
    private final long millisPeriod;
    private long emitted;
    private boolean closed;

    private OpenCsvPaymentRecordIterator(
        Reader reader,
        Iterator<PaymentRecord> delegate,
        CsvPaymentsInputFile input,
        long rowsPerPeriod,
        long millisPeriod
    ) {
        this.reader = reader;
        this.delegate = delegate;
        this.input = input;
        this.rowsPerPeriod = rowsPerPeriod;
        this.millisPeriod = millisPeriod;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public PaymentRecord next() {
        PaymentRecord record = delegate.next();
        emitted++;
        String serviceId = ProcessCsvPaymentsInputService.class.toString();
        MDC.put("serviceId", serviceId);
        try {
            LOG.debugf(
                "Executed blocking CSV iteration on %s (csvId=%s)",
                input.getSourceName(),
                record.getCsvId());
        } finally {
            MDC.remove("serviceId");
        }
        return record;
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        reader.close();
        LOG.infof(
            "Closed CSV reader for: %s (iterated %d records, rowsPerPeriod=%d, periodMillis=%d)",
            input.getSourceName(),
            emitted,
            rowsPerPeriod,
            millisPeriod);
    }
  }
}
