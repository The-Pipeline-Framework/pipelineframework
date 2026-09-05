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

package org.pipelineframework.application;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.pipelineframework.CommandRetryRuntimeAuthority;
import org.pipelineframework.command.CommandReexecutionScope;

class CommandRetryAuthorityBoundaryTest {

  @Test
  void applicationCodeCannotConstructOrObtainRetryAuthority() {
    assertEquals(0, CommandRetryRuntimeAuthority.class.getConstructors().length);
    assertTrue(Arrays.stream(CommandRetryRuntimeAuthority.class.getMethods())
        .filter(method -> method.getDeclaringClass() == CommandRetryRuntimeAuthority.class)
        .noneMatch(method -> Modifier.isStatic(method.getModifiers())
            && method.getReturnType() == CommandRetryRuntimeAuthority.class));
  }

  @Test
  void installingRetryAuthorityRequiresTheRuntimeCapability() {
    Method installRetry = Arrays.stream(CommandReexecutionScope.class.getMethods())
        .filter(method -> method.getName().equals("installRetry"))
        .findFirst()
        .orElseThrow();

    assertEquals(CommandRetryRuntimeAuthority.class, installRetry.getParameterTypes()[0]);
  }

  @Test
  void aReflectivelyForgedCapabilityCannotInstallRetryAuthority() throws Exception {
    var constructor = CommandRetryRuntimeAuthority.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    CommandRetryRuntimeAuthority forged = constructor.newInstance();

    assertThrows(SecurityException.class,
        () -> CommandReexecutionScope.installRetry(forged, "command-1", "admission-1"));
  }
}
