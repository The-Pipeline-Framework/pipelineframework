package org.pipelineframework;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionWorkItem;

record DueExecutionDispatchPlan(List<ExecutionWorkItem> workItems) {

  DueExecutionDispatchPlan {
    Objects.requireNonNull(workItems, "workItems must not be null");
    workItems = List.copyOf(workItems);
  }

  static DueExecutionDispatchPlan from(List<ExecutionRecord<Object, Object>> dueExecutions) {
    Objects.requireNonNull(dueExecutions, "dueExecutions must not be null");
    return new DueExecutionDispatchPlan(dueExecutions.stream()
        .sorted(Comparator
            .comparingLong(ExecutionRecord<Object, Object>::nextDueEpochMs)
            .thenComparing(ExecutionRecord::tenantId)
            .thenComparing(ExecutionRecord::executionId))
        .map(record -> new ExecutionWorkItem(record.tenantId(), record.executionId()))
        .toList());
  }

  boolean empty() {
    return workItems.isEmpty();
  }
}
