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

package org.pipelineframework.grpc;

import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

/**
 * Base class for reactive service adapters that provide common functionality for gRPC reactive services.
 */
public abstract class ReactiveServiceAdapterBase {

  /**
   * Default constructor for ReactiveServiceAdapterBase.
   */
  public ReactiveServiceAdapterBase() {
  }

  /**
   * Switches execution to the current Vert.x event loop.
   *
   * <p>If no Vert.x context is present the returned Uni fails with an {@link IllegalStateException}.
   *
   * @return {@code null} when execution has been scheduled on the current Vert.x event loop,
   *         or fails with an {@link IllegalStateException} when no Vert.x context is available
   */
  protected Uni<Void> switchToEventLoop() {
    var ctx = Vertx.currentContext();
    if (ctx == null) {
      return Uni.createFrom().failure(new IllegalStateException("No Vert.x context available"));
    }
    return Uni.createFrom().emitter(em -> ctx.runOnContext(() -> em.complete(null)));
  }
}