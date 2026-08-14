/*
 * Copyright (c) 2026 Mariano Barcia
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 */

/**
 * Runtime telemetry follows one extension path:
 *
 * <ol>
 *   <li>capture the fact at its semantic owner;</li>
 *   <li>represent it as a granular immutable value in {@code observation};</li>
 *   <li>derive sink-specific immutable plans in {@code derivation};</li>
 *   <li>let the focused metrics, tracing, replay, or safety adapter execute the plan;</li>
 *   <li>update the test-only observability obligation and journey coverage.</li>
 * </ol>
 *
 * <p>For example, cancellation becomes {@code StepObservation.Cancelled}, then
 * {@code StepTelemetryDerivation.terminal(...)} produces consistent metric, span, replay, and safety plans.
 * New telemetry must not call metrics, tracing, and replay independently with arbitrary parameters or reconstruct
 * the outcome in each adapter.</p>
 */
package org.pipelineframework.telemetry;
