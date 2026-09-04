package org.pipelineframework.connector.llm;

import java.util.List;

public record AskUser(String prompt, List<String> choices) {
}
