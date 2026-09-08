package org.pipelineframework.examples.quickbooks;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.examples.quickbooks.domain.CollectionAction;
import org.pipelineframework.examples.quickbooks.domain.CollectionsBriefing;
import org.pipelineframework.service.ReactiveStreamingClientService;

/** Reduces independently assessed accounts into the final operational briefing. */
@ApplicationScoped
public final class CollectionsBriefingService
    implements ReactiveStreamingClientService<CollectionAction, CollectionsBriefing> {

    @Override
    public Uni<CollectionsBriefing> process(Multi<CollectionAction> actions) {
        return actions.collect().asList().map(CollectionsBriefingService::briefing);
    }

    static CollectionsBriefing briefing(List<CollectionAction> input) {
        if (input.isEmpty()) {
            throw new IllegalArgumentException("at least one receivables report item is required");
        }
        String reportDate = input.getFirst().account().reportDate();
        String currency = input.getFirst().account().currency();
        if (input.stream().anyMatch(action -> !reportDate.equals(action.account().reportDate())
            || !currency.equals(action.account().currency()))) {
            throw new IllegalArgumentException("all collection actions must belong to one report and currency");
        }
        List<CollectionAction> prioritized = input.stream()
            .filter(action -> action.account().accountPresent())
            .sorted(Comparator.comparingInt(CollectionsBriefingService::priorityRank)
                .thenComparing(CollectionAction::overdue, Comparator.reverseOrder())
                .thenComparing(action -> action.account().customer()))
            .toList();
        if (prioritized.isEmpty()) {
            return new CollectionsBriefing(reportDate, currency, BigDecimal.ZERO, BigDecimal.ZERO, 0,
                "No open receivables were returned for " + reportDate + ".", List.of());
        }
        BigDecimal total = prioritized.stream().map(action -> action.account().total())
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal overdue = prioritized.stream().map(CollectionAction::overdue)
            .reduce(BigDecimal.ZERO, BigDecimal::add);
        long urgent = prioritized.stream()
            .filter(action -> "CRITICAL".equals(action.priority()) || "HIGH".equals(action.priority()))
            .count();
        CollectionAction first = prioritized.getFirst();
        String headline = "%d customers owe %s %s; %s %s is overdue. %d need priority contact; first: %s (%s, %s %s overdue)."
            .formatted(prioritized.size(), currency, money(total), currency, money(overdue), urgent,
                first.account().customer(), first.priority().toLowerCase(), currency, money(first.overdue()));
        return new CollectionsBriefing(reportDate, currency, total, overdue, urgent, headline, prioritized);
    }

    private static int priorityRank(CollectionAction action) {
        return switch (action.priority()) {
            case "CRITICAL" -> 0;
            case "HIGH" -> 1;
            case "STANDARD" -> 2;
            case "MONITOR" -> 3;
            case "NONE" -> 4;
            default -> throw new IllegalArgumentException("unknown collection priority: " + action.priority());
        };
    }

    private static String money(BigDecimal amount) {
        return amount.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }
}
