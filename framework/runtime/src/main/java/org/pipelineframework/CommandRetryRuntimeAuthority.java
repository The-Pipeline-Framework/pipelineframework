/*
 * Copyright (c) 2023-2026 Mariano Barcia
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

package org.pipelineframework;

/**
 * Unforgeable runtime capability for installing deliberate Command re-execution authority.
 *
 * <p>The capability is intentionally obtainable only by framework code in this package. Application
 * steps and connectors can depend on the runtime artifact, but cannot construct or obtain an instance
 * through the public API.</p>
 */
public final class CommandRetryRuntimeAuthority {
  private static final CommandRetryRuntimeAuthority INSTANCE = new CommandRetryRuntimeAuthority();

  private CommandRetryRuntimeAuthority() {
  }

  static CommandRetryRuntimeAuthority frameworkAuthority() {
    return INSTANCE;
  }

  public void requireFrameworkAuthority() {
    if (this != INSTANCE) {
      throw new SecurityException("Command retry authority was not issued by the execution runtime");
    }
  }
}
