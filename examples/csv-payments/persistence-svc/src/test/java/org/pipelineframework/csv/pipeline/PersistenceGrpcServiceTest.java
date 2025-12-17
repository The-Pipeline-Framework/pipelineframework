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

package org.pipelineframework.csv.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PersistenceGrpcServiceTest {

    @Test
    void testGetClassFromTypeUrl_withValidTypeUrls() {
        // Test with a well-known class to verify the logic works
        String objectTypeUrl = "type.googleapis.com/java.lang.String"; // Using a standard Java class

        // The method is private, so we'll use reflection to test it
        // Create an instance of the service using reflection to bypass dependencies
        try {
            // Access the private method
            java.lang.reflect.Method method = PersistenceGrpcService.class.getDeclaredMethod("getClassFromTypeUrl", String.class);
            method.setAccessible(true);

            // Create a minimal instance using reflection
            PersistenceGrpcService service = PersistenceGrpcService.class.getDeclaredConstructor().newInstance();

            Class<?> stringClass = (Class<?>) method.invoke(service, objectTypeUrl);

            assertNotNull(stringClass, "Should return a class for java.lang.String type URL");
            assertEquals(String.class, stringClass, "Should return String class for java.lang.String type URL");

        } catch (Exception e) {
            fail("Exception occurred while testing getClassFromTypeUrl: " + e.getMessage());
        }
    }

    @Test
    void testGetClassFromTypeUrl_withInvalidTypeUrl() {
        // The method is private, so we'll use reflection to test it
        try {
            java.lang.reflect.Method method = PersistenceGrpcService.class.getDeclaredMethod("getClassFromTypeUrl", String.class);
            method.setAccessible(true);

            // Create a minimal instance using reflection
            PersistenceGrpcService service = PersistenceGrpcService.class.getDeclaredConstructor().newInstance();

            Class<?> result = (Class<?>) method.invoke(service, "invalid.type.url");
            assertNull(result, "Should return null for invalid type URL");

        } catch (Exception e) {
            fail("Exception occurred while testing getClassFromTypeUrl: " + e.getMessage());
        }
    }
}