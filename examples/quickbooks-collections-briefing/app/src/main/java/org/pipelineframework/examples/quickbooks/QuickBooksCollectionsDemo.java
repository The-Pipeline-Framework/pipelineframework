package org.pipelineframework.examples.quickbooks;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;

import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.examples.quickbooks.domain.CollectionsBriefing;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequest;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParams;
import org.pipelineframework.examples.quickbooks.domain.QuickBooksAgedReceivablesRequestParamsAgingMethodValue;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.type.CanonicalFieldValue;

/** Small command-mode shell for presenting the example against a QuickBooks sandbox. */
@QuarkusMain
public class QuickBooksCollectionsDemo implements QuarkusApplication {
    @Inject
    PipelineExecutionService executionService;

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
            briefing.actions().forEach(action -> System.out.printf(
                "  %-8s %-32s overdue %s %s%n           %s%n",
                action.priority(), action.account().customer(), briefing.currency(), action.overdue(),
                action.recommendedAction()));
            return 0;
        } finally {
            PipelineExecutionContextHolder.clear();
        }
    }

    private CollectionsBriefing invoke(QuickBooksAgedReceivablesRequest request) {
        return executionService.<CollectionsBriefing>executePipelineUnary(Uni.createFrom().item(request))
            .await().indefinitely();
    }
}
