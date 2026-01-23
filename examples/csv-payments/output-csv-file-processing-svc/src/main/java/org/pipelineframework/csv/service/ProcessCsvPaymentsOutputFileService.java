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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import jakarta.enterprise.context.ApplicationScoped;

import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.*;
import org.pipelineframework.csv.common.mapper.CsvPaymentsOutputFileMapper;
import org.pipelineframework.csv.common.mapper.PaymentOutputMapper;
import org.pipelineframework.service.ReactiveBidirectionalStreamingService;

/**
 * Reactive bidirectional streaming service for processing streams of payment outputs and writing them to CSV files.
 * <p>
 * This service implements a reactive bidirectional streaming pattern using Mutiny, with the following characteristics:
 * 1. Groups payment outputs by input file path to handle multiple output files in order
 * 2. Each related group of payment outputs (belonging to the same input file) is written to its corresponding output file
 * 3. sbc.write() will truncate each file when invoked
 * 4. File I/O operations run on the event loop thread since they're not a bottleneck
 * 5. As the terminal operation in the pipeline, it doesn't create backpressure issues
 * <p>
 * The service uses the iterator-based write method from OpenCSV which provides better
 * streaming characteristics than list-based writing.
 */
@PipelineStep(
  inputType = PaymentOutput.class,
  outputType = CsvPaymentsOutputFile.class,
  stepType = org.pipelineframework.step.StepManyToMany.class,
  backendType = org.pipelineframework.grpc.GrpcServiceBidirectionalStreamingAdapter.class,
  inboundMapper = PaymentOutputMapper.class,
  outboundMapper = CsvPaymentsOutputFileMapper.class
)
@ApplicationScoped
public class ProcessCsvPaymentsOutputFileService
    implements ReactiveBidirectionalStreamingService<PaymentOutput, CsvPaymentsOutputFile> {

  /**
   * Process a stream of payment outputs and write them to CSV files in sequence.
   * <p>
   * Implementation notes:
   * - Groups payment outputs by input file path to handle multiple output files in order
   * - Each related group of payment outputs (belonging to the same input file) is written to its corresponding output file
   * - Files are written using OpenCSV's iterator-based write method for better streaming characteristics
   * - sbc.write() will truncate each file when invoked
   * - File I/O operations run on the event loop thread since they're not a bottleneck
   * - As the terminal operation in the service pipeline, it doesn't create backpressure
   * - Uses try-with-resources to ensure proper file cleanup
   *
   * @param paymentOutputMulti stream of payment outputs to process
   * @return Multi containing the generated CSV file information for each input file
   */
  @Override
  public Multi<CsvPaymentsOutputFile> process(Multi<PaymentOutput> paymentOutputMulti) {
      Logger logger = Logger.getLogger(getClass());
      String serviceId = this.getClass().toString();

      return paymentOutputMulti
              .collect().asList()
              .onItem().transformToMulti(paymentOutputs -> {
                  if (paymentOutputs.isEmpty()) {
                      logger.info("No payment outputs to process");
                      return Multi.createFrom().empty();
                  }

                  // Group by input file path
                  Map<Path, List<PaymentOutput>> groupedOutputs =
                          paymentOutputs.stream()
                                  .collect(Collectors.groupingBy(po -> po.getPaymentStatus()
                                          .getAckPaymentSent()
                                          .getPaymentRecord()
                                          .getCsvPaymentsInputFilePath()
                                          .toAbsolutePath()
                                          .normalize()));

                  logger.infof("Grouped %d file(s)", groupedOutputs.size());
                  groupedOutputs.forEach((k, v) ->
                          logger.infof("File %s has %d records", k, v.size()));

                  return Multi.createFrom().iterable(groupedOutputs.entrySet())
                          .flatMap(entry -> {
                              Path inputFile = entry.getKey();
                              List<PaymentOutput> outputsForFile = entry.getValue();

                              if (outputsForFile.isEmpty()) {
                                  return Multi.createFrom().empty();
                              }

                              final AtomicInteger recordCount = new AtomicInteger(0);

                              // Reactive creation and writing of file
                              Uni<CsvPaymentsOutputFile> writeUni = Uni.createFrom().deferred(() -> {
                                  try {
                                      CsvPaymentsOutputFile file = getCsvPaymentsOutputFile(outputsForFile.getFirst());
                                      file.getSbc().write(outputsForFile.iterator());
                                      recordCount.set(outputsForFile.size());

                                      MDC.put("serviceId", serviceId);
                                      logger.infof("Executed command on stream --> %s with %d records",
                                              file.getFilepath(), recordCount.get());
                                      MDC.remove("serviceId");

                                      return Uni.createFrom().item(file);
                                  } catch (IOException e) {
                                      logger.errorf(e, "Failed to create output file: %s", inputFile);
                                      return Uni.createFrom().nullItem();
                                  } catch (CsvDataTypeMismatchException e) {
                                      logger.errorf(e, "CSV data type mismatch: %s", inputFile);
                                      return Uni.createFrom().nullItem();
                                  } catch (CsvRequiredFieldEmptyException e) {
                                      logger.errorf(e, "A required field is empty: %s", inputFile);
                                      return Uni.createFrom().nullItem();
                                  } catch (Exception e) {
                                      logger.errorf(e, "Failed to write output file: %s", inputFile);
                                      return Uni.createFrom().nullItem();
                                  }
                              });

                              return writeUni
                                      .onItem().call(file -> Uni.createFrom().item(() -> {
                                          try {
                                              if (file != null) file.close();
                                          } catch (Exception e) {
                                              logger.warnf("Failed to close output file: %s", file.getFilepath(), e);
                                          }
                                          return null;
                                      }))
                                      .toMulti();
                          });
              })
              .filter(Objects::nonNull); // drop nulls just in case
  }

  /**
   * Creates a CsvPaymentsOutputFile based on a payment output.
   * <p>
   * Extracts the input file path from the payment output to determine where
   * the output file should be written.
   * 
   * @param paymentOutput Usually the first payment output in the stream
   * @return CsvPaymentsOutputFile instance for writing
   * @throws IOException if there's an error creating the file
   */
  protected CsvPaymentsOutputFile getCsvPaymentsOutputFile(PaymentOutput paymentOutput)
      throws IOException {
    assert paymentOutput != null;
    PaymentStatus paymentStatus = paymentOutput.getPaymentStatus();
    assert paymentStatus != null;
    AckPaymentSent ackPaymentSent = paymentStatus.getAckPaymentSent();
    assert ackPaymentSent != null;
    PaymentRecord paymentRecord = ackPaymentSent.getPaymentRecord();
    assert paymentRecord != null;
    Path csvPaymentsInputFilePath = paymentRecord.getCsvPaymentsInputFilePath();

    return new CsvPaymentsOutputFile(csvPaymentsInputFilePath);
  }

}
