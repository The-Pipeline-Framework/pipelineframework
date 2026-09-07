package org.pipelineframework.examples.quickbooks;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.pipelineframework.connector.JsonPayload;
import org.pipelineframework.examples.quickbooks.domain.CollectionsBriefing;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequest;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParams;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParamsAgingMethodValue;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.type.CanonicalFieldValue;

/** Small command-mode shell for presenting the example against a QuickBooks sandbox. */
@QuarkusMain
public class QuickBooksCollectionsDemo implements QuarkusApplication {
    @Inject
    @Any
    Instance<StepOneToOne<QuickBooksAgedReceivablesRequest, JsonPayload>> querySteps;

    @Inject
    PipelineInvocationRuntime invocationRuntime;

    @Inject
    AgedReceivablesInterpreterService interpreter;

    @Inject
    QuickBooksMcpConnectionResolver connectionResolver;

    @Override
    public int run(String... args) {
        final String reportDate;
        try {
            reportDate = (args.length == 0 ? LocalDate.now() : LocalDate.parse(args[0])).toString();
        } catch (DateTimeParseException invalidDate) {
            System.err.println("report date must use YYYY-MM-DD");
            return 2;
        }
        var request = new QuickBooksAgedReceivablesRequest(new QuickBooksAgedReceivablesRequestParams(
            CanonicalFieldValue.of(new QuickBooksAgedReceivablesRequestParamsAgingMethodValue("Report_Date")),
            CanonicalFieldValue.absent(), CanonicalFieldValue.absent(), CanonicalFieldValue.of(reportDate)));
        String executionId = "quickbooks-collections-" + UUID.randomUUID();
        PipelineExecutionContextHolder.set(new PipelineExecutionContext(connectionResolver.tenantId(), executionId, 0));
        try {
            CollectionsBriefing briefing = invoke(request);
            System.out.println();
            System.out.println(briefing.headline());
            briefing.accounts().forEach(account -> System.out.printf(
                "  %-32s total %s %s, overdue %s %s%n",
                account.customer(), briefing.currency(), account.total(), briefing.currency(),
                account.total().subtract(account.current())));
            return 0;
        } finally {
            PipelineExecutionContextHolder.clear();
        }
    }

    private CollectionsBriefing invoke(QuickBooksAgedReceivablesRequest request) {
        StepOneToOne<QuickBooksAgedReceivablesRequest, JsonPayload> query = querySteps.stream()
            .findFirst().orElseThrow(() -> new IllegalStateException("generated QuickBooks Query step is unavailable"));
        JsonPayload payload = invocationRuntime.invokeStepUni(null, null, () -> query.applyOneToOne(request))
            .await().indefinitely();
        return interpreter.process(payload).await().indefinitely();
    }
}
