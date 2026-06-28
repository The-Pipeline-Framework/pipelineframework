package org.pipelineframework;

import java.util.Objects;

import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitUnitRecord;

record AwaitCompletionAdmissionPlan(
    AwaitCompletionResult result,
    AwaitUnitRecord unit,
    AwaitCompletionRoute route
) {
  AwaitCompletionAdmissionPlan {
    Objects.requireNonNull(result, "result");
    Objects.requireNonNull(unit, "unit");
    Objects.requireNonNull(route, "route");
  }

  static AwaitCompletionAdmissionPlan live(AwaitCompletionResult result, AwaitUnitRecord unit) {
    return new AwaitCompletionAdmissionPlan(result, unit, AwaitCompletionRoute.LIVE_SESSION);
  }

  static AwaitCompletionAdmissionPlan durable(AwaitCompletionResult result, AwaitUnitRecord unit) {
    return new AwaitCompletionAdmissionPlan(result, unit, AwaitCompletionRoute.DURABLE_CONTINUATION);
  }

  boolean liveSession() {
    return route == AwaitCompletionRoute.LIVE_SESSION;
  }
}

enum AwaitCompletionRoute {
  LIVE_SESSION,
  DURABLE_CONTINUATION
}
