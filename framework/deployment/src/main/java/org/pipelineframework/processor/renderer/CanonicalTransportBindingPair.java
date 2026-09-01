package org.pipelineframework.processor.renderer;

import com.squareup.javapoet.TypeName;
import java.util.Optional;
import java.util.function.Supplier;

/** Normalized input and output transport bindings resolved for one generated boundary. */
record CanonicalTransportBindingPair(
    Optional<CanonicalTransportTypeBinding> input,
    Optional<CanonicalTransportTypeBinding> output
) {
    static CanonicalTransportBindingPair empty() {
        return new CanonicalTransportBindingPair(Optional.empty(), Optional.empty());
    }

    boolean any() {
        return input.isPresent() || output.isPresent();
    }

    TypeName restInputOr(Supplier<TypeName> fallback) {
        return input.<TypeName>map(CanonicalTransportTypeBinding::restDtoType).orElseGet(fallback);
    }

    TypeName restOutputOr(Supplier<TypeName> fallback) {
        return output.<TypeName>map(CanonicalTransportTypeBinding::restDtoType).orElseGet(fallback);
    }
}
