package org.pipelineframework.processor.routing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.template.PipelinePlatform;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateMessage;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.pipelineframework.config.template.PipelineTemplateUnion;
import org.pipelineframework.config.template.PipelineTemplateUnionVariant;
import org.pipelineframework.processor.PipelineCompilationContext;
import org.pipelineframework.processor.ir.MapperFallbackMode;
import org.pipelineframework.processor.ir.StepDefinition;
import org.pipelineframework.processor.ir.StepKind;
import org.pipelineframework.processor.ir.StreamingShape;

class PipelineBranchRoutingPlannerTest {

    private final PipelineBranchRoutingPlanner planner = new PipelineBranchRoutingPlanner();

    @Test
    void plansLinearUnionRoutingWithMandatoryTerminalMerge() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Order Routing",
            "com.example.order",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            unions(),
            List.of(
                step("classifyOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("reserveStock", "PhysicalOrder", "StockReserved", List.of("PhysicalOrder"), false),
                step("provisionLicense", "DigitalOrder", "LicenseProvisioned", List.of("DigitalOrder"), false),
                step("requestManualReview", "ManualReviewOrder", "ManualReviewRequested", List.of("ManualReviewOrder"), false),
                step("finalize", "OrderCompletion", "FinalizedOrder",
                    List.of("StockReserved", "LicenseProvisioned", "ManualReviewRequested"), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("classifyOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("reserveStock", "PhysicalOrder", "StockReserved"),
            stepDefinition("provisionLicense", "DigitalOrder", "LicenseProvisioned"),
            stepDefinition("requestManualReview", "ManualReviewOrder", "ManualReviewRequested"),
            stepDefinition("finalize", "OrderCompletion", "FinalizedOrder")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertTrue(plan.orElseThrow().branchAware());
        assertEquals(4, plan.orElseThrow().terminalStepIndex());
        assertEquals(List.of("PhysicalOrder"), plan.orElseThrow().steps().get(1).acceptedContractTypes());
        assertEquals(
            List.of("StockReserved", "LicenseProvisioned", "ManualReviewRequested"),
            plan.orElseThrow().steps().get(4).acceptedContractTypes());
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void allowsConcreteExpansionBeforeUnionRoutingBegins() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Csv Payments",
            "com.example.csv",
            "GRPC",
            PipelinePlatform.COMPUTE,
            Map.of(
                "CsvPaymentsInputFile", message("CsvPaymentsInputFile"),
                "PaymentRecord", message("PaymentRecord"),
                "ApprovedPaymentStatus", message("ApprovedPaymentStatus"),
                "UnapprovedPaymentStatus", message("UnapprovedPaymentStatus"),
                "ApprovedPaymentOutput", message("ApprovedPaymentOutput"),
                "UnapprovedPaymentOutput", message("UnapprovedPaymentOutput"),
                "PaymentOutput", message("PaymentOutput")),
            Map.of(
                "PaymentStatus", new PipelineTemplateUnion(
                    "PaymentStatus",
                    Map.of(
                        "approved", new PipelineTemplateUnionVariant("approved", "ApprovedPaymentStatus", 1),
                        "unapproved", new PipelineTemplateUnionVariant("unapproved", "UnapprovedPaymentStatus", 2))),
                "PaymentOutputBranch", new PipelineTemplateUnion(
                    "PaymentOutputBranch",
                    Map.of(
                        "approved", new PipelineTemplateUnionVariant("approved", "ApprovedPaymentOutput", 1),
                        "unapproved", new PipelineTemplateUnionVariant("unapproved", "UnapprovedPaymentOutput", 2)))),
            List.of(
                step("processCsvPaymentsInput", "EXPANSION", "CsvPaymentsInputFile", "PaymentRecord", List.of(), false),
                step("awaitPaymentProvider", "ONE_TO_ONE", "PaymentRecord", "PaymentStatus", List.of(), false),
                step("processApprovedPaymentStatus", "ONE_TO_ONE", "ApprovedPaymentStatus", "ApprovedPaymentOutput", List.of(), false),
                step("processUnapprovedPaymentStatus", "ONE_TO_ONE", "UnapprovedPaymentStatus", "UnapprovedPaymentOutput", List.of(), false),
                step("finalizePaymentOutput", "ONE_TO_ONE", "PaymentOutputBranch", "PaymentOutput",
                    List.of("ApprovedPaymentOutput", "UnapprovedPaymentOutput"), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("processCsvPaymentsInput", "CsvPaymentsInputFile", "PaymentRecord"),
            stepDefinition("awaitPaymentProvider", "PaymentRecord", "PaymentStatus"),
            stepDefinition("processApprovedPaymentStatus", "ApprovedPaymentStatus", "ApprovedPaymentOutput"),
            stepDefinition("processUnapprovedPaymentStatus", "UnapprovedPaymentStatus", "UnapprovedPaymentOutput"),
            stepDefinition("finalizePaymentOutput", "PaymentOutputBranch", "PaymentOutput")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertTrue(plan.orElseThrow().branchAware());
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void rejectsBranchAwarePipelineWithoutTerminalStep() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Order Routing",
            "com.example.order",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            unions(),
            List.of(
                step("classifyOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("reserveStock", "PhysicalOrder", "StockReserved", List.of("PhysicalOrder"), false)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("classifyOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("reserveStock", "PhysicalOrder", "StockReserved")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message -> message.contains("exactly one step with terminal: true")),
            diagnostics.toString());
    }

    @Test
    void autoResolvesAcceptedTypesFromInputTypeNameWhenAcceptsOmitted() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Order Routing",
            "com.example.order",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            unions(),
            List.of(
                step("classifyOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("reserveStock", "PhysicalOrder", "StockReserved", List.of(), false),
                step("provisionLicense", "DigitalOrder", "LicenseProvisioned", List.of(), false),
                step("requestManualReview", "ManualReviewOrder", "ManualReviewRequested", List.of(), false),
                step("finalize", "OrderCompletion", "FinalizedOrder",
                    List.of("StockReserved", "LicenseProvisioned", "ManualReviewRequested"), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("classifyOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("reserveStock", "PhysicalOrder", "StockReserved"),
            stepDefinition("provisionLicense", "DigitalOrder", "LicenseProvisioned"),
            stepDefinition("requestManualReview", "ManualReviewOrder", "ManualReviewRequested"),
            stepDefinition("finalize", "OrderCompletion", "FinalizedOrder")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertTrue(plan.orElseThrow().branchAware());
        assertEquals(4, plan.orElseThrow().terminalStepIndex());
        assertEquals(List.of("PhysicalOrder"), plan.orElseThrow().steps().get(1).acceptedContractTypes());
        assertEquals(List.of("DigitalOrder"), plan.orElseThrow().steps().get(2).acceptedContractTypes());
        assertEquals(List.of("ManualReviewOrder"), plan.orElseThrow().steps().get(3).acceptedContractTypes());
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void implicitlyAcceptsAllUnionVariantsWhenAcceptsOmitted() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Order Routing",
            "com.example.order",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            unions(),
            List.of(
                step("classifyOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("routeOrder", "OrderDecision", "OrderCompletion", List.of(), false),
                step("finalize", "OrderCompletion", "FinalizedOrder",
                    List.of("StockReserved", "LicenseProvisioned", "ManualReviewRequested"), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("classifyOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("routeOrder", "OrderDecision", "OrderCompletion"),
            stepDefinition("finalize", "OrderCompletion", "FinalizedOrder")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertTrue(plan.orElseThrow().branchAware());
        java.util.Set<String> expected = java.util.Set.of("PhysicalOrder", "DigitalOrder", "ManualReviewOrder");
        java.util.Set<String> actual = new java.util.LinkedHashSet<>(plan.orElseThrow().steps().get(1).acceptedContractTypes());
        assertEquals(expected, actual);
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void resolvesImplicitUnionAcceptedTypesFromSharedDomainPackage() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Compensation Finalize",
            "org.pipelineframework.tpfgo.compensation.failure",
            "GRPC",
            PipelinePlatform.COMPUTE,
            Map.of(
                "PaymentCaptured", message("PaymentCaptured"),
                "PaymentRejected", message("PaymentRejected"),
                "PaymentRequiresReview", message("PaymentRequiresReview"),
                "TerminalOrderState", message("TerminalOrderState")),
            Map.of(
                "PaymentOutcome", new PipelineTemplateUnion(
                    "PaymentOutcome",
                    Map.of(
                        "captured", new PipelineTemplateUnionVariant("captured", "PaymentCaptured", 1),
                        "rejected", new PipelineTemplateUnionVariant("rejected", "PaymentRejected", 2),
                        "review", new PipelineTemplateUnionVariant("review", "PaymentRequiresReview", 3)))),
            List.of(
                step("compensationFinalizeOrder", "PaymentOutcome", "TerminalOrderState", List.of(), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition(
                "compensationFinalizeOrder",
                "org.pipelineframework.tpfgo.compensation.failure.pipeline",
                "org.pipelineframework.tpfgo.common.domain.PaymentOutcome",
                "org.pipelineframework.tpfgo.common.domain.TerminalOrderState")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertEquals(
            java.util.Set.of(
                "org.pipelineframework.tpfgo.common.domain.PaymentCaptured",
                "org.pipelineframework.tpfgo.common.domain.PaymentRejected",
                "org.pipelineframework.tpfgo.common.domain.PaymentRequiresReview"),
            plan.orElseThrow().steps().getFirst().acceptedDomainTypes().stream()
                .map(ClassName::canonicalName)
                .collect(java.util.stream.Collectors.toSet()));
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void rejectsTerminalThatDoesNotCoverAllReachableAlternatives() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Order Routing",
            "com.example.order",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            unions(),
            List.of(
                step("classifyOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("reserveStock", "PhysicalOrder", "StockReserved", List.of("PhysicalOrder"), false),
                step("provisionLicense", "DigitalOrder", "LicenseProvisioned", List.of("DigitalOrder"), false),
                step("requestManualReview", "ManualReviewOrder", "ManualReviewRequested", List.of("ManualReviewOrder"), false),
                step("finalize", "OrderCompletion", "FinalizedOrder",
                    List.of("StockReserved", "LicenseProvisioned"), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("classifyOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("reserveStock", "PhysicalOrder", "StockReserved"),
            stepDefinition("provisionLicense", "DigitalOrder", "LicenseProvisioned"),
            stepDefinition("requestManualReview", "ManualReviewOrder", "ManualReviewRequested"),
            stepDefinition("finalize", "OrderCompletion", "FinalizedOrder")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message ->
                message.contains("does not cover all reachable branch-end alternatives")
                    && message.contains("ManualReviewRequested")),
            diagnostics.toString());
    }

    @Test
    void linearTemplateWithOnlyConcreteInputTypesIsNotBranchAware() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Linear Pipeline",
            "com.example.pipeline",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            Map.of(),
            List.of(
                step("processOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("finalize", "OrderDecision", "FinalizedOrder", List.of(), false)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("processOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("finalize", "OrderDecision", "FinalizedOrder")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertFalse(plan.orElseThrow().branchAware());
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void v1TemplateWithStepsIsNotBranchAware() {
        List<String> diagnostics = new ArrayList<>();
        PipelineCompilationContext ctx = context(diagnostics);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            1,
            "Simple Pipeline",
            "com.example.pipeline",
            "GRPC",
            PipelinePlatform.COMPUTE,
            Map.of(),
            Map.of(),
            List.of(
                step("processOrder", "OrderRequest", "OrderDecision", List.of(), false)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("processOrder", "OrderRequest", "OrderDecision")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isPresent(), diagnostics.toString());
        assertFalse(plan.orElseThrow().branchAware());
        assertTrue(diagnostics.isEmpty(), diagnostics.toString());
    }

    @Test
    void rejectsBranchAwareStepWhenAcceptedJavaTypeCannotBeResolved() {
        List<String> diagnostics = new ArrayList<>();
        Messager messager = mock(Messager.class);
        doAnswer(invocation -> {
            Diagnostic.Kind kind = invocation.getArgument(0);
            CharSequence message = invocation.getArgument(1);
            diagnostics.add(kind + ":" + message);
            return null;
        }).when(messager).printMessage(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(CharSequence.class));

        TypeMirror orderRequestMirror = mock(TypeMirror.class);
        TypeMirror physicalOrderMirror = mock(TypeMirror.class);
        TypeMirror digitalOrderMirror = mock(TypeMirror.class);
        TypeMirror orderCompletionMirror = mock(TypeMirror.class);
        TypeMirror stockReservedMirror = mock(TypeMirror.class);

        TypeElement orderRequestElement = mock(TypeElement.class);
        when(orderRequestElement.asType()).thenReturn(orderRequestMirror);
        TypeElement physicalOrderElement = mock(TypeElement.class);
        when(physicalOrderElement.asType()).thenReturn(physicalOrderMirror);
        TypeElement digitalOrderElement = mock(TypeElement.class);
        when(digitalOrderElement.asType()).thenReturn(digitalOrderMirror);
        TypeElement orderCompletionElement = mock(TypeElement.class);
        when(orderCompletionElement.asType()).thenReturn(orderCompletionMirror);
        TypeElement stockReservedElement = mock(TypeElement.class);
        when(stockReservedElement.asType()).thenReturn(stockReservedMirror);

        Elements elements = mock(Elements.class);
        when(elements.getTypeElement("com.example.order.common.domain.OrderRequest")).thenReturn(orderRequestElement);
        when(elements.getTypeElement("com.example.order.common.domain.PhysicalOrder")).thenReturn(physicalOrderElement);
        when(elements.getTypeElement("com.example.order.common.domain.DigitalOrder")).thenReturn(digitalOrderElement);
        when(elements.getTypeElement("com.example.order.common.domain.OrderCompletion")).thenReturn(orderCompletionElement);
        when(elements.getTypeElement("com.example.order.common.domain.StockReserved")).thenReturn(stockReservedElement);

        Types types = mock(Types.class);
        when(types.isAssignable(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any())).thenReturn(true);

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getMessager()).thenReturn(messager);
        when(processingEnv.getElementUtils()).thenReturn(elements);
        when(processingEnv.getTypeUtils()).thenReturn(types);

        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        PipelineCompilationContext ctx = new PipelineCompilationContext(processingEnv, roundEnv);
        ctx.setPipelineTemplateConfig(new PipelineTemplateConfig(
            2,
            "Order Routing",
            "com.example.order",
            "GRPC",
            PipelinePlatform.COMPUTE,
            messages(),
            unions(),
            List.of(
                step("classifyOrder", "OrderRequest", "OrderDecision", List.of(), false),
                step("reserveStock", "PhysicalOrder", "StockReserved", List.of("PhysicalOrder"), false),
                step("provisionLicense", "DigitalOrder", "LicenseProvisioned", List.of("DigitalOrder"), false),
                step("finalize", "OrderCompletion", "FinalizedOrder", List.of("StockReserved", "LicenseProvisioned"), true)),
            Map.of(),
            null,
            null,
            null));
        ctx.setStepDefinitions(List.of(
            stepDefinition("classifyOrder", "OrderRequest", "OrderDecision"),
            stepDefinition("reserveStock", "PhysicalOrder", "StockReserved"),
            stepDefinition("provisionLicense", "DigitalOrder", "LicenseProvisioned"),
            stepDefinition("finalize", "OrderCompletion", "FinalizedOrder")));

        var plan = planner.plan(ctx);

        assertTrue(plan.isEmpty());
        assertTrue(diagnostics.stream().anyMatch(message ->
                message.contains("accepted type 'com.example.order.common.domain.LicenseProvisioned'")
                    && message.contains("could not be resolved during branch routing validation")),
            diagnostics.toString());
        assertTrue(diagnostics.stream().anyMatch(message ->
                message.contains("WARNING:Union contract 'OrderCompletion' variant 'license'")
                    && message.contains("com.example.order.common.domain.LicenseProvisioned")
                    && message.contains("skipping runtime type indexing")),
            diagnostics.toString());
        assertFalse(diagnostics.isEmpty());
    }

    private PipelineCompilationContext context(List<String> diagnostics) {
        Messager messager = mock(Messager.class);
        doAnswer(invocation -> {
            Diagnostic.Kind kind = invocation.getArgument(0);
            CharSequence message = invocation.getArgument(1);
            diagnostics.add(kind + ":" + message);
            return null;
        }).when(messager).printMessage(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(CharSequence.class));

        ProcessingEnvironment processingEnv = mock(ProcessingEnvironment.class);
        when(processingEnv.getMessager()).thenReturn(messager);
        RoundEnvironment roundEnv = mock(RoundEnvironment.class);
        return new PipelineCompilationContext(processingEnv, roundEnv);
    }

    private static Map<String, PipelineTemplateMessage> messages() {
        return Map.of(
            "OrderRequest", message("OrderRequest"),
            "PhysicalOrder", message("PhysicalOrder"),
            "DigitalOrder", message("DigitalOrder"),
            "ManualReviewOrder", message("ManualReviewOrder"),
            "StockReserved", message("StockReserved"),
            "LicenseProvisioned", message("LicenseProvisioned"),
            "ManualReviewRequested", message("ManualReviewRequested"),
            "FinalizedOrder", message("FinalizedOrder"));
    }

    private static Map<String, PipelineTemplateUnion> unions() {
        return Map.of(
            "OrderDecision", new PipelineTemplateUnion(
                "OrderDecision",
                Map.of(
                    "physical", new PipelineTemplateUnionVariant("physical", "PhysicalOrder", 1),
                    "digital", new PipelineTemplateUnionVariant("digital", "DigitalOrder", 2),
                    "manualReview", new PipelineTemplateUnionVariant("manualReview", "ManualReviewOrder", 3))),
            "OrderCompletion", new PipelineTemplateUnion(
                "OrderCompletion",
                Map.of(
                    "stock", new PipelineTemplateUnionVariant("stock", "StockReserved", 1),
                    "license", new PipelineTemplateUnionVariant("license", "LicenseProvisioned", 2),
                    "review", new PipelineTemplateUnionVariant("review", "ManualReviewRequested", 3))));
    }

    private static PipelineTemplateStep step(
        String name,
        String cardinality,
        String inputTypeName,
        String outputTypeName,
        List<String> accepts,
        boolean terminal
    ) {
        return new PipelineTemplateStep(
            name,
            cardinality,
            inputTypeName,
            List.of(),
            null,
            outputTypeName,
            List.of(),
            null,
            null,
            accepts,
            terminal);
    }

    private static PipelineTemplateStep step(
        String name,
        String inputTypeName,
        String outputTypeName,
        List<String> accepts,
        boolean terminal
    ) {
        return step(name, "ONE_TO_ONE", inputTypeName, outputTypeName, accepts, terminal);
    }

    private static PipelineTemplateMessage message(String name) {
        return new PipelineTemplateMessage(name, List.of(), null);
    }

    private static StepDefinition stepDefinition(String name, String inputTypeName, String outputTypeName) {
        return new StepDefinition(
            name,
            StepKind.INTERNAL,
            ClassName.get("com.example.order.pipeline", capitalize(name) + "Service"),
            null,
            null,
            null,
            MapperFallbackMode.NONE,
            ClassName.get("com.example.order.common.domain", inputTypeName),
            ClassName.get("com.example.order.common.domain", outputTypeName),
            StreamingShape.UNARY_UNARY);
    }

    private static StepDefinition stepDefinition(String name, String servicePackage, String inputTypeName, String outputTypeName) {
        return new StepDefinition(
            name,
            StepKind.INTERNAL,
            ClassName.get(servicePackage, capitalize(name) + "Service"),
            null,
            null,
            null,
            MapperFallbackMode.NONE,
            ClassName.bestGuess(inputTypeName),
            ClassName.bestGuess(outputTypeName),
            StreamingShape.UNARY_UNARY);
    }

    private static String capitalize(String value) {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }
}
