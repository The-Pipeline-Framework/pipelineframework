/*
 * Copyright (c) 2026 Mariano Barcia
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

package org.pipelineframework.processor.util;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

/** Resolves a generic interface as implemented after Java type-variable substitution. */
public final class ImplementedGenericInterfaceResolver {

    public Optional<DeclaredType> resolve(
        TypeElement implementation,
        String interfaceName,
        ProcessingEnvironment processingEnv
    ) {
        TypeElement target = processingEnv.getElementUtils().getTypeElement(interfaceName);
        if (target == null || !(target.asType() instanceof DeclaredType targetType)) {
            throw new IllegalStateException("Generic interface not found: " + interfaceName);
        }
        return resolve(implementation.asType(), targetType, processingEnv.getTypeUtils(), new HashSet<>());
    }

    private Optional<DeclaredType> resolve(
        TypeMirror candidate,
        DeclaredType target,
        Types types,
        Set<String> visited
    ) {
        if (!(candidate instanceof DeclaredType declared)
            || !visited.add(types.erasure(candidate).toString())) {
            return Optional.empty();
        }
        if (types.isSameType(types.erasure(declared), types.erasure(target))) {
            return Optional.of(declared);
        }
        for (TypeMirror supertype : types.directSupertypes(declared)) {
            Optional<DeclaredType> resolved = resolve(supertype, target, types, visited);
            if (resolved.isPresent()) {
                return resolved;
            }
        }
        return Optional.empty();
    }
}
