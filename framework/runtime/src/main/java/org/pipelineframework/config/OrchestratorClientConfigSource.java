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

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.eclipse.microprofile.config.spi.ConfigSource;
import org.jboss.logging.Logger;

/**
 * Config source that loads generated orchestrator client wiring properties.
 */
public class OrchestratorClientConfigSource implements ConfigSource {

    private static final Logger logger = Logger.getLogger(OrchestratorClientConfigSource.class);
    private static final String RESOURCE_PATH = "META-INF/pipeline/orchestrator-clients.properties";
    private static final int ORDINAL = 90;

    private final Map<String, String> properties;

    /**
     * Creates a new config source with generated orchestrator client properties.
     */
    public OrchestratorClientConfigSource() {
        this.properties = loadProperties();
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Set<String> getPropertyNames() {
        return properties.keySet();
    }

    @Override
    public String getValue(String propertyName) {
        return properties.get(propertyName);
    }

    @Override
    public String getName() {
        return "pipelineframework-orchestrator-clients";
    }

    @Override
    public int getOrdinal() {
        return ORDINAL;
    }

    private Map<String, String> loadProperties() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = OrchestratorClientConfigSource.class.getClassLoader();
        }
        try (InputStream input = loader.getResourceAsStream(RESOURCE_PATH)) {
            if (input == null) {
                return Collections.emptyMap();
            }
            Properties props = new Properties();
            props.load(input);
            Map<String, String> values = new HashMap<>();
            for (String name : props.stringPropertyNames()) {
                values.put(name, props.getProperty(name));
            }
            return Collections.unmodifiableMap(values);
        } catch (IOException e) {
            logger.warnf("Failed to load %s: %s", RESOURCE_PATH, e.getMessage());
            return Collections.emptyMap();
        }
    }
}
