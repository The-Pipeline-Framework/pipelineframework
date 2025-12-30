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

import java.util.Iterator;
import java.util.concurrent.Executor;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import com.opencsv.bean.CsvToBeanBuilder;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.unchecked.Unchecked;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.CsvPaymentsInputStream;
import org.pipelineframework.csv.common.domain.PaymentRecord;
import org.pipelineframework.csv.common.mapper.CsvPaymentsInputStreamMapper;
import org.pipelineframework.csv.common.mapper.PaymentRecordMapper;
import org.pipelineframework.service.ReactiveStreamingService;

/**
 * Service that processes CSV payments input and produces a stream of payment records.
 * This converts a single input file into multiple payment records.
 */
@PipelineStep(
    inputType = CsvPaymentsInputStream.class,
    outputType = PaymentRecord.class,
    stepType = org.pipelineframework.step.StepOneToMany.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class,
    inboundMapper = CsvPaymentsInputStreamMapper.class,
    outboundMapper = PaymentRecordMapper.class
)
@ApplicationScoped
public class ProcessCsvPaymentsInputStreamService
    implements ReactiveStreamingService<CsvPaymentsInputStream, PaymentRecord> {

  private static final Logger LOGGER = Logger.getLogger(ProcessCsvPaymentsInputStreamService.class);

  Executor executor;

    @Inject
  public ProcessCsvPaymentsInputStreamService(@Named("virtualExecutor") Executor executor) {
    this.executor = executor;
  }

  @Override
  public Multi<PaymentRecord> process(CsvPaymentsInputStream input) {
    return Multi.createFrom()
        .deferred(
            Unchecked.supplier(() -> {
              var csvReader =
                  new CsvToBeanBuilder<PaymentRecord>(input.openReader())
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

              return Multi.createFrom()
                  .iterable(iterable)
                  .runSubscriptionOn(executor)
                  .onItem()
                  .invoke(
                      rec -> {
                         MDC.put("serviceId", serviceId);
                        LOGGER.infof("Executed command on %s --> %s", input.getSourceName(), rec);
                         MDC.remove("serviceId");
                      });
            }));
  }
}