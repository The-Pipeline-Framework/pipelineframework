package org.pipelineframework.examples.quickbooks;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class QuickBooksCollectionsDemoTest {
    @Test
    void rejectsMalformedReportDateBeforeInvokingQuickBooks() {
        assertEquals(2, new QuickBooksCollectionsDemo().run("07/09/2026"));
    }
}
