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

package org.pipelineframework.cache;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import io.quarkus.cache.CacheKeyGenerator;
import org.pipelineframework.context.PipelineContext;
import org.pipelineframework.context.PipelineContextHolder;

abstract class PropertyCacheKeyGenerator implements CacheKeyGenerator {

    /**
     * Returns the property name used to build the cache key.
     *
     * @return the property name to read from the first method argument
     */
    protected abstract String propertyName();

    @Override
    public Object generate(Method method, Object... methodParams) {
        String baseKey = buildBaseKey(methodParams);

        PipelineContext context = PipelineContextHolder.get();
        String versionTag = context != null ? context.versionTag() : null;
        return PipelineCacheKeyFormat.applyVersionTag(baseKey, versionTag);
    }

    private String buildBaseKey(Object... methodParams) {
        if (methodParams == null || methodParams.length == 0) {
            return "no-params";
        }

        Object target = methodParams[0];
        String propertyValue = extractPropertyValue(target);
        if (propertyValue == null || propertyValue.isBlank()) {
            return PipelineCacheKeyFormat.baseKeyForParams(methodParams);
        }

        String typeName = target == null ? "null" : target.getClass().getName();
        return typeName + ":" + propertyValue;
    }

    private String extractPropertyValue(Object target) {
        if (target == null) {
            return null;
        }
        String property = propertyName();
        if (property == null || property.isBlank()) {
            return null;
        }

        String capitalized = property.substring(0, 1).toUpperCase() + property.substring(1);
        String[] methodNames = new String[] { property, "get" + capitalized, "is" + capitalized };
        for (String methodName : methodNames) {
            Object value = invokeAccessor(target, methodName);
            if (value != null) {
                return String.valueOf(value);
            }
        }

        Object value = readField(target, property);
        return value != null ? String.valueOf(value) : null;
    }

    private Object invokeAccessor(Object target, String methodName) {
        try {
            Method method = target.getClass().getMethod(methodName);
            if (method.getParameterCount() == 0) {
                return method.invoke(target);
            }
        } catch (Exception ignored) {
            // Ignore and fall back to field access.
        }
        return null;
    }

    private Object readField(Object target, String fieldName) {
        Class<?> current = target.getClass();
        while (current != null && current != Object.class) {
            try {
                Field field = current.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (Exception ignored) {
                // Try parent class.
            }
            current = current.getSuperclass();
        }
        return null;
    }
}
