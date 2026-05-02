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

package org.pipelineframework.service;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.function.Function;
import org.jboss.logging.Logger;

/**
 * Function that converts exceptions to gRPC StatusRuntimeExceptions.
 */
public class throwStatusRuntimeExceptionFunction implements Function<Throwable, Throwable> {

  private static final Logger LOG = Logger.getLogger(throwStatusRuntimeExceptionFunction.class);

  /**
   * Default constructor for throwStatusRuntimeExceptionFunction.
   */
  public throwStatusRuntimeExceptionFunction() {
  }

  @Override
  public Throwable apply(Throwable throwable) {
    Metadata metadata = new Metadata();
    Metadata.Key<String> errorKey =
        Metadata.Key.of("error-details", Metadata.ASCII_STRING_MARSHALLER);
    String description = sanitizeForGrpcHeader(throwable);
    metadata.put(errorKey, description);

    Status status = Status.INTERNAL.withDescription(description).withCause(throwable);

    LOG.debug("Runtime exception thrown: ", throwable);

    return new StatusRuntimeException(status, metadata);
  }

  private static String sanitizeForGrpcHeader(Throwable throwable) {
    String message = throwable == null ? null : throwable.getMessage();
    if (message == null || message.isBlank()) {
      return throwable == null ? "Unknown error" : throwable.getClass().getSimpleName();
    }
    return message.replaceAll("[\\r\\n\\t]+", " ").trim();
  }
}
