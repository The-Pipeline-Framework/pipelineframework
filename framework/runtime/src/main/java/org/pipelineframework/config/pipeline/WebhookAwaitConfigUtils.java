package org.pipelineframework.config.pipeline;

import java.util.Map;

/**
 * Shared helpers for webhook await transport configuration.
 */
public final class WebhookAwaitConfigUtils {

    private WebhookAwaitConfigUtils() {
    }

    public static boolean hasWebhookUrl(Map<?, ?> config) {
        if (!stringValue(config.get("url")).isBlank()) {
            return true;
        }
        Object requestObj = config.get("request");
        if (requestObj instanceof Map<?, ?> requestMap && !stringValue(requestMap.get("url")).isBlank()) {
            return true;
        }
        Object dispatchObj = config.get("dispatch");
        return dispatchObj instanceof Map<?, ?> dispatchMap && !stringValue(dispatchMap.get("url")).isBlank();
    }

    private static String stringValue(Object value) {
        return value == null ? "" : value.toString().trim();
    }
}
