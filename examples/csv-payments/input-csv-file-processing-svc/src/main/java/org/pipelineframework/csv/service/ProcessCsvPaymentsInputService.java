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
import java.util.Iterator;
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
import org.pipelineframework.csv.common.mapper.CsvPaymentsInputFileMapper;
import org.pipelineframework.csv.common.mapper.PaymentRecordMapper;
import org.pipelineframework.csv.util.DemandPacerConfig;
import org.pipelineframework.service.ReactiveStreamingService;

@PipelineStep(
    inputType = CsvPaymentsInputFile.class,
    outputType = PaymentRecord.class,
    stepType = org.pipelineframework.step.StepOneToMany.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = CsvPaymentsInputFileMapper.class,
    outboundMapper = PaymentRecordMapper.class
)
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
   * <p>The returned stream emits records parsed from the CSV using the input's mapping strategy and
   * applies a fixed-rate demand pacer configured for this service. The underlying reader is closed
   * when the stream terminates and each emitted record is logged with a service identifier.
   *
   * @param input the CSV input file wrapper providing the reader, source name and mapping strategy
   * @return a {@code Multi<PaymentRecord>} that emits parsed payment records paced by the service's
   *     configured rows-per-period and period duration
   * @throws RuntimeException if an I/O error occurs while opening or preparing the CSV reader
   */
  @Override
  public Multi<PaymentRecord> process(CsvPaymentsInputFile input) {
    return Multi.createFrom()
        .deferred(
            Unchecked.supplier(
                () -> {
                  try {
                    var reader = input.openReader();
                    var csvReader =
                        new CsvToBeanBuilder<PaymentRecord>(reader)
                            .withType(PaymentRecord.class)
                            .withMappingStrategy(input.veryOwnStrategy())
                            .withSeparator(',')
                            .withIgnoreLeadingWhiteSpace(true)
                            .withIgnoreEmptyLine(true)
                            .build();

                    String serviceId = this.getClass().toString();

                    // Lazy + typed
                    Iterator<PaymentRecord> iterator = csvReader.iterator();
                    Iterable<PaymentRecord> iterable = () -> iterator;

                    // rate limiter
                    FixedDemandPacer pacer =
                        new FixedDemandPacer(rowsPerPeriod, Duration.ofMillis(millisPeriod));

                    return Multi.createFrom()
                        .iterable(iterable)
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
                              } catch (IOException e) {
                                LOG.warn("Failed to close CSV reader", e);
                              }
                            });
                  } catch (IOException e) {
                    throw new RuntimeException("CSV processing error", e);
                  }
                }));
  }
}