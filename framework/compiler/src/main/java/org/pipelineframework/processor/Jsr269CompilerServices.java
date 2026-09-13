package org.pipelineframework.processor;

import java.util.Objects;
import java.util.function.Supplier;
import javax.annotation.processing.Filer;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

/** Explicit, lazily accessed Java compiler services supplied by the JSR-269 host. */
public final class Jsr269CompilerServices {

    private final boolean available;
    private final Supplier<Elements> elements;
    private final Supplier<Types> types;
    private final Supplier<Filer> filer;

    private Jsr269CompilerServices(
        boolean available,
        Supplier<Elements> elements,
        Supplier<Types> types,
        Supplier<Filer> filer
    ) {
        this.available = available;
        this.elements = Objects.requireNonNull(elements, "elements must not be null");
        this.types = Objects.requireNonNull(types, "types must not be null");
        this.filer = Objects.requireNonNull(filer, "filer must not be null");
    }

    public boolean available() {
        return available;
    }

    public Elements elements() {
        return Objects.requireNonNull(elements.get(), "JSR-269 Elements service is unavailable");
    }

    public Types types() {
        return Objects.requireNonNull(types.get(), "JSR-269 Types service is unavailable");
    }

    public Filer filer() {
        return Objects.requireNonNull(filer.get(), "JSR-269 Filer service is unavailable");
    }

    public static Jsr269CompilerServices from(ProcessingEnvironment environment) {
        if (environment == null) {
            return empty();
        }
        return new Jsr269CompilerServices(
            true,
            environment::getElementUtils,
            environment::getTypeUtils,
            environment::getFiler
        );
    }

    public static Jsr269CompilerServices empty() {
        Supplier<Elements> unavailableElements = () -> {
            throw new IllegalStateException("JSR-269 Elements service is unavailable");
        };
        Supplier<Types> unavailableTypes = () -> {
            throw new IllegalStateException("JSR-269 Types service is unavailable");
        };
        Supplier<Filer> unavailableFiler = () -> {
            throw new IllegalStateException("JSR-269 Filer service is unavailable");
        };
        return new Jsr269CompilerServices(false, unavailableElements, unavailableTypes, unavailableFiler);
    }
}
