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

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.opencsv.bean.CsvToBeanBuilder;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.FixedDemandPacer;
import io.smallrye.mutiny.unchecked.Unchecked;
import lombok.Getter;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.util.DemandPacerConfig;
import org.pipelineframework.service.ReactiveStreamingService;

@PipelineStep
@ApplicationScoped
@Getter
public class ProcessCsvPaymentsInputService
    implements ReactiveStreamingService<CsvPaymentsInputFile, PaymentRecord> {

  private static final Logger LOG = Logger.getLogger(ProcessCsvPaymentsInputService.class);
  private final long rowsPerPeriod;
  private final long millisPeriod;

    /**
     * Create a service instance configured with demand-pacing parameters.
     *
     * Initialises the instance fields used for pacing from the supplied configuration and logs the configured values.
     *
     * @param config configuration supplying the number of rows per pacing period and the period duration in milliseconds
     */
    @Inject
    public ProcessCsvPaymentsInputService(DemandPacerConfig config) {
        rowsPerPeriod = config.rowsPerPeriod();
        millisPeriod = config.millisPeriod();

        LOG.infof(
                "ProcessCsvPaymentsInputService initialized: rowsPerPeriod=%d, periodMillis=%d",
                config.rowsPerPeriod(),
                config.millisPeriod());
    }

  /**
   * Stream parsed PaymentRecord objects from the provided CSV input file with demand pacing.
   *
   * <p>Each subscription opens a fresh reader, validates the CSV eagerly (before emitting any
   * records), and then streams parsed records with demand pacing. The reader is closed when the
   * stream terminates or if an error occurs during setup or parsing.
   *
   * @param input the CSV input file wrapper providing the reader, source name and mapping strategy
   * @return a {@code Multi<PaymentRecord>} that emits parsed payment records paced by the service's
   *     configured rows-per-period and period duration
   */
  @Override
  public Multi<PaymentRecord> process(CsvPaymentsInputFile input) {
    return Multi.createFrom().deferred(
        Unchecked.supplier(
            () -> {
              var reader = input.openReader();
              try {
                var csvReader =
                    new CsvToBeanBuilder<PaymentRecord>(reader)
                        .withType(PaymentRecord.class)
                        .withMappingStrategy(input.veryOwnStrategy())
                        .withSeparator(',')
                        .withIgnoreLeadingWhiteSpace(true)
                        .withIgnoreEmptyLine(true)
                        .build();

                String serviceId = this.getClass().toString();

                // Eagerly parse CSV into a list to catch errors before emitting
                List<PaymentRecord> records = csvReader.parse();

                FixedDemandPacer pacer =
                    new FixedDemandPacer(rowsPerPeriod, Duration.ofMillis(millisPeriod));

                return Multi.createFrom()
                    .iterable(records)
                    .paceDemand()
                    .on(Infrastructure.getDefaultWorkerPool())
                    .using(pacer)
                    .onItem()
                    .invoke(
                        rec -> {
                          MDC.put("serviceId", serviceId);
                          LOG.infof(
                              "Executed command on %s --> %s", input.getSourceName(), rec);
                          MDC.remove("serviceId");
                        })
                    .onTermination()
                    .invoke(
                        () -> {
                          try {
                            reader.close();
                            LOG.infof("Closed CSV reader for: %s", input.getSourceName());
                          } catch (IOException e) {
                            LOG.warnf(e, "Failed to close CSV reader for: %s", input.getSourceName());
                          }
                        });
              } catch (Exception e) {
                try {
                  reader.close();
                } catch (IOException closeEx) {
                  LOG.warnf(closeEx, "Failed to close CSV reader after error for: %s", input.getSourceName());
                }
                LOG.errorf(e, "CSV processing failed for file: %s", input.getSourceName());
                return Multi.createFrom().failure(new RuntimeException("CSV processing error: " + e.getMessage(), e));
              }
            }));
  }
}
