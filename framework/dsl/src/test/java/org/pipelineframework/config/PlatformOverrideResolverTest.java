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

package org.pipelineframework.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class PlatformOverrideResolverTest {

    @Test
    void normalizesCanonicalAndLegacyValues() {
        assertEquals("COMPUTE", PlatformOverrideResolver.normalizeKnownPlatform("COMPUTE"));
        assertEquals("COMPUTE", PlatformOverrideResolver.normalizeKnownPlatform("STANDARD"));
        assertEquals("FUNCTION", PlatformOverrideResolver.normalizeKnownPlatform("FUNCTION"));
        assertEquals("FUNCTION", PlatformOverrideResolver.normalizeKnownPlatform("LAMBDA"));
    }

    @Test
    void rejectsUnknownPlatform() {
        assertNull(PlatformOverrideResolver.normalizeKnownPlatform("EDGE"));
    }

    @Test
    void rejectsNullPlatform() {
        assertNull(PlatformOverrideResolver.normalizeKnownPlatform(null));
    }

    @Test
    void rejectsEmptyPlatform() {
        assertNull(PlatformOverrideResolver.normalizeKnownPlatform(""));
    }

    @Test
    void resolveOverridePrefersPropertyOverEnvironment() {
        String resolved = PlatformOverrideResolver.resolveOverride(
            key -> PlatformOverrideResolver.PLATFORM_PROPERTY_KEY.equals(key) ? "FUNCTION" : null,
            key -> PlatformOverrideResolver.PLATFORM_ENV_KEY.equals(key) ? "COMPUTE" : null);

        assertEquals("FUNCTION", PlatformOverrideResolver.normalizeKnownPlatform(resolved));
    }

    @Test
    void resolveOverrideUsesEnvironmentWhenPropertyMissingOrBlank() {
        String fromMissingProperty = PlatformOverrideResolver.resolveOverride(
            key -> null,
            key -> PlatformOverrideResolver.PLATFORM_ENV_KEY.equals(key) ? "LAMBDA" : null);
        assertEquals("FUNCTION", PlatformOverrideResolver.normalizeKnownPlatform(fromMissingProperty));

        String fromBlankProperty = PlatformOverrideResolver.resolveOverride(
            key -> PlatformOverrideResolver.PLATFORM_PROPERTY_KEY.equals(key) ? "  " : null,
            key -> PlatformOverrideResolver.PLATFORM_ENV_KEY.equals(key) ? "STANDARD" : null);
        assertEquals("COMPUTE", PlatformOverrideResolver.normalizeKnownPlatform(fromBlankProperty));
    }

    @Test
    void resolveOverrideReturnsNullWhenPropertyAndEnvironmentMissing() {
        String resolved = PlatformOverrideResolver.resolveOverride(key -> null, key -> null);

        assertNull(resolved);
        assertNull(PlatformOverrideResolver.normalizeKnownPlatform(resolved));
    }
}
