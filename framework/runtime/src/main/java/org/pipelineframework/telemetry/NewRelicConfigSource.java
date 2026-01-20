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

package org.pipelineframework.telemetry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.eclipse.microprofile.config.spi.ConfigSource;
import org.jboss.logging.Logger;

public class NewRelicConfigSource implements ConfigSource {

    private static final Logger logger = Logger.getLogger(NewRelicConfigSource.class);
    private static final String LICENSE_ENV = "NEW_RELIC_LICENSE_KEY";
    private static final String ENDPOINT_ENV = "NEW_RELIC_OTLP_ENDPOINT";
    private static final String DEFAULT_ENDPOINT = "https://otlp.eu01.nr-data.net:443";

    private final Map<String, String> properties;

    public NewRelicConfigSource() {
        this.properties = buildProperties();
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Set<String> getPropertyNames() {
        return this.properties.keySet();
    }

    @Override
    public String getValue(String propertyName) {
        return properties.get(propertyName);
    }

    @Override
    public String getName() {
        return "pipelineframework-newrelic-config";
    }

    @Override
    public int getOrdinal() {
        return 200;
    }

    private Map<String, String> buildProperties() {
        String licenseKey = System.getenv(LICENSE_ENV);
        if (licenseKey == null || licenseKey.isBlank()) {
            return Collections.emptyMap();
        }
        logger.infof("New Relic OTLP export enabled via %s (LGTM disabled).", LICENSE_ENV);
        Map<String, String> values = new HashMap<>();
        values.put("quarkus.otel.enabled", "true");
        values.put("quarkus.otel.traces.enabled", "true");
        values.put("quarkus.otel.metrics.enabled", "true");
        values.put("quarkus.otel.metric.export.interval", "5s");
        values.put("quarkus.otel.traces.sampler", "parentbased_traceidratio");
        values.put("quarkus.otel.traces.sampler.arg", "0.001");
        values.put("quarkus.otel.exporter.otlp.endpoint", resolveEndpoint());
        values.put("quarkus.otel.exporter.otlp.protocol", "http/protobuf");
        values.put("quarkus.otel.exporter.otlp.compression", "gzip");
        values.put("quarkus.otel.exporter.otlp.metrics.temporality.preference", "delta");
        values.put("quarkus.otel.exporter.otlp.headers", "api-key=" + licenseKey);
        values.put("quarkus.observability.lgtm.enabled", "false");
        return Collections.unmodifiableMap(values);
    }

    private String resolveEndpoint() {
        String endpoint = System.getenv(ENDPOINT_ENV);
        if (endpoint == null || endpoint.isBlank()) {
            return DEFAULT_ENDPOINT;
        }
        return endpoint;
    }
}
