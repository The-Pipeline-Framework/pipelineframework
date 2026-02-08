package org.pipelineframework.csv.process_csv_payments_input.service;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.pipelineframework.csv.util.DemandPacerConfig;

@Dependent
public class ProcessCsvPaymentsInputService extends org.pipelineframework.csv.service.ProcessCsvPaymentsInputService {
  /**
   * Constructs a ProcessCsvPaymentsInputService configured with the provided DemandPacerConfig.
   *
   * @param config the DemandPacerConfig used to initialize the service's pacing configuration
   */
  @Inject
  public ProcessCsvPaymentsInputService(DemandPacerConfig config) {
    super(config);
  }
}