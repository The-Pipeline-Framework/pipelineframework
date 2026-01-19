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

package org.pipelineframework.health;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;
import org.pipelineframework.PipelineExecutionService;

/**
 * Readiness check that reports UP only after startup dependency health checks pass.
 */
@Readiness
@ApplicationScoped
public class PipelineReadinessCheck implements HealthCheck {

  /**
   * Creates a new PipelineReadinessCheck.
   */
  public PipelineReadinessCheck() {
  }

  @Inject
  PipelineExecutionService pipelineExecutionService;

  @Override
  public HealthCheckResponse call() {
    PipelineExecutionService.StartupHealthState state =
        pipelineExecutionService.getStartupHealthState();
    boolean ready = state == PipelineExecutionService.StartupHealthState.HEALTHY;
    HealthCheckResponseBuilder builder = HealthCheckResponse.named("pipeline-dependencies");
    builder.withData("state", state.name());
    String error = pipelineExecutionService.getStartupHealthError();
    if (error != null && !error.isBlank()) {
      builder.withData("error", error);
    }
    return ready ? builder.up().build() : builder.down().build();
  }
}
