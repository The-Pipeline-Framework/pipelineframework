package org.pipelineframework.csv.process_csv_payments_input.service;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.pipelineframework.csv.util.DemandPacerConfig;

@Dependent
public class ProcessCsvPaymentsInputService extends org.pipelineframework.csv.service.ProcessCsvPaymentsInputService {
  @Inject
  public ProcessCsvPaymentsInputService(DemandPacerConfig config) {
    super(config);
  }
}
