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

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Arrays;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Multi;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.pipelineframework.annotation.PipelineStep;
import org.pipelineframework.csv.common.domain.CsvFolder;
import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.util.HybridResourceLoader;

@ApplicationScoped
@PipelineStep(
    inputType = CsvFolder.class,
    outputType = CsvPaymentsInputFile.class,
    stepType = org.pipelineframework.step.StepOneToMany.class,
    backendType = org.pipelineframework.grpc.GrpcReactiveServiceAdapter.class
)
public class ProcessFolderService implements org.pipelineframework.service.ReactiveStreamingService<CsvFolder, CsvPaymentsInputFile> {

  private static final Logger LOG = Logger.getLogger(ProcessFolderService.class);

  @Inject
  HybridResourceLoader resourceLoader;

  public Multi<CsvPaymentsInputFile> process(CsvFolder csvFolder) {
    Path csvFolderPath = csvFolder.getPath();

    LOG.infof("Reading CSV folder from path: %s", csvFolderPath);

    URL resource = resourceLoader.getResource(String.valueOf(csvFolderPath));
    if (resource == null) {
      LOG.warnf("CSV folder not found: %s", csvFolderPath);
      resourceLoader.diagnoseResourceAccess(String.valueOf(csvFolderPath));
      return Multi.createFrom().failure(
          new IllegalArgumentException(
              MessageFormat.format("CSV folder not found: {0}", csvFolderPath)));
    }

      File directory;
      try {
          directory = new File(resource.toURI());
      } catch (URISyntaxException e) {
        return Multi.createFrom().failure(
            new IllegalArgumentException(
                MessageFormat.format("CSV folder URI is invalid: {0}", csvFolderPath)));
      }

      if (!directory.exists() || !directory.isDirectory()) {
      return Multi.createFrom().failure(
          new IllegalArgumentException(
              MessageFormat.format(
                  "CSV path is not a valid directory: {0}", directory.getAbsolutePath())));
    }

    File[] csvFiles = directory.listFiles((ignored, name) -> name.toLowerCase().endsWith(".csv"));
    if (csvFiles == null || csvFiles.length == 0) {
      LOG.warnf("No CSV files found in %s", csvFolderPath);
      resourceLoader.diagnoseResourceAccess(String.valueOf(csvFolderPath));
      return Multi.createFrom().empty();
    }

    String serviceId = this.getClass().toString();

    return Multi.createFrom().iterable(
            Arrays.stream(csvFiles)
                    .map(CsvPaymentsInputFile::new)
                    .toList()
    ).invoke(
          file -> {
            MDC.put("serviceId", serviceId);
            LOG.infof(
                    "Executed command on %s --> %s", file.getFilepath(), file);
            MDC.remove("serviceId");
          });
  }
}