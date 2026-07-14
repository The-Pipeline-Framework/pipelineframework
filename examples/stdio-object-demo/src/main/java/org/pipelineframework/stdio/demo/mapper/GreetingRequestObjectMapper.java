package org.pipelineframework.stdio.demo.mapper;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.objectingest.ObjectSnapshotMapper;
import org.pipelineframework.stdio.demo.common.domain.GreetingRequests;

/** Maps one EOF-delimited JSON value into the typed pipeline input. */
public final class GreetingRequestObjectMapper implements ObjectSnapshotMapper<GreetingRequests> {
    private static final Pattern NAME = Pattern.compile("\\\"name\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");

    @Override
    public GreetingRequests map(ObjectSnapshot snapshot) {
        String content = snapshot.textContent();
        if (content == null || content.isBlank()) {
            throw new IllegalArgumentException("stdin JSON must not be blank");
        }
        String value = content.trim();
        if (!(value.startsWith("{") && value.endsWith("}")) && !(value.startsWith("[") && value.endsWith("]"))) {
            throw new IllegalArgumentException("stdin must contain one JSON object or JSON collection");
        }
        Matcher matcher = NAME.matcher(value);
        List<String> names = matcher.results().map(result -> result.group(1)).toList();
        if (names.isEmpty()) {
            throw new IllegalArgumentException("stdin JSON must contain at least one name field");
        }
        return new GreetingRequests(names);
    }
}
