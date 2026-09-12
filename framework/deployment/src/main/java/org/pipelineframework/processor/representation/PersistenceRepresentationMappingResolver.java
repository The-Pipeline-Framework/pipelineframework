package org.pipelineframework.processor.representation;

import java.util.Optional;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.RepresentationMapping;
import org.pipelineframework.processor.Jsr269CompilerServices;

/** Resolves and validates the existing v3 persistence representation contract. */
public final class PersistenceRepresentationMappingResolver {
    private PersistenceRepresentationMappingResolver() {
    }

    public static Optional<ResolvedPersistenceRepresentation> resolve(
        PipelineTemplateConfig config,
        ClassName domainType,
        ProcessingEnvironment processingEnv
    ) {
        return resolve(config, domainType, Jsr269CompilerServices.from(processingEnv));
    }

    public static Optional<ResolvedPersistenceRepresentation> resolve(
        PipelineTemplateConfig config,
        ClassName domainType,
        Jsr269CompilerServices compilerServices
    ) {
        String domainPrefix = config.basePackage() + ".domain.";
        if (!domainType.canonicalName().startsWith(domainPrefix)) {
            return Optional.empty();
        }
        String domainName = domainType.simpleName();
        RepresentationMapping mapping = config.typeModel().representationMapping(domainName, "persistence")
            .orElse(null);
        if (mapping == null) {
            return Optional.empty();
        }
        if (!(config.typeModel().definition(domainName).orElse(null)
            instanceof PipelineTemplateTypeDefinition.RecordType)) {
            throw failure(mapping, "persistence mappings currently support only record domain types");
        }
        String representationName = mapping.representationType().orElseThrow(() ->
            failure(mapping, "persistence mapping requires representation type"));
        String mapperName = mapping.mapperType().orElseThrow(() ->
            failure(mapping, "persistence mapping requires mapper type"));
        validateTypes(compilerServices, mapping, domainType, representationName, mapperName);
        return Optional.of(new ResolvedPersistenceRepresentation(
            ClassName.bestGuess(representationName), ClassName.bestGuess(mapperName)));
    }

    private static void validateTypes(
        Jsr269CompilerServices compilerServices,
        RepresentationMapping mapping,
        ClassName domainType,
        String representationName,
        String mapperName
    ) {
        if (compilerServices == null || compilerServices.elements() == null || compilerServices.types() == null) {
            return;
        }
        TypeElement representation = compilerServices.elements().getTypeElement(representationName);
        if (representation == null) {
            throw failure(mapping, "representation class is unavailable");
        }
        TypeElement mapper = compilerServices.elements().getTypeElement(mapperName);
        if (mapper == null) {
            throw failure(mapping, "mapper class is unavailable");
        }
        TypeElement mapperContract = compilerServices.elements().getTypeElement("org.pipelineframework.mapper.Mapper");
        TypeElement domain = compilerServices.elements().getTypeElement(domainType.canonicalName());
        if (mapperContract == null || domain == null) {
            throw failure(mapping, "canonical mapper contract is unavailable");
        }
        var expected = compilerServices.types().getDeclaredType(
            mapperContract, domain.asType(), representation.asType());
        if (!compilerServices.types().isAssignable(mapper.asType(), expected)) {
            throw failure(mapping, "mapper must implement Mapper<" + domainType.canonicalName() + ", "
                + representationName + ">");
        }
    }

    private static IllegalStateException failure(RepresentationMapping mapping, String reason) {
        return new IllegalStateException("Representation mapping failure for domain type '" + mapping.domainType()
            + "', key '" + mapping.key() + "', representation type '"
            + mapping.representationType().orElse("<missing>") + "', mapper type '"
            + mapping.mapperType().orElse("<missing>") + "': " + reason);
    }

    public record ResolvedPersistenceRepresentation(ClassName representationType, ClassName mapperType) {
    }
}
