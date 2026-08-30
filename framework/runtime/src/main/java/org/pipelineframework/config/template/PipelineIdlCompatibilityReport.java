/* Copyright (c) 2026 Mariano Barcia. Licensed under the Apache License, Version 2.0. */
package org.pipelineframework.config.template;

import java.util.List;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.Map;

/** Complete multi-surface result of comparing two normalized IDL snapshots. */
public record PipelineIdlCompatibilityReport(List<PipelineIdlCompatibilityFinding> findings) {
    public PipelineIdlCompatibilityReport {
        findings = List.copyOf(Objects.requireNonNull(findings, "findings must not be null"));
    }

    public List<String> breakingMessages() {
        Map<String, List<PipelineIdlCompatibilityFinding>> bySubject = new LinkedHashMap<>();
        findings.forEach(finding -> bySubject.computeIfAbsent(finding.subject(), ignored -> new java.util.ArrayList<>())
            .add(finding));
        return bySubject.values().stream()
            .filter(subjectFindings -> subjectFindings.stream().anyMatch(finding -> finding.impact().breaking()))
            .map(subjectFindings -> subjectFindings.stream().map(PipelineIdlCompatibilityFinding::diagnostic)
                .collect(java.util.stream.Collectors.joining("; ")))
            .toList();
    }
}
