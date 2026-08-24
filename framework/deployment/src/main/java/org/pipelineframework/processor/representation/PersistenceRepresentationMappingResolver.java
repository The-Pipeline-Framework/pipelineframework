package org.pipelineframework.processor.representation;

import java.util.Optional;
import javax.annotation.processing.ProcessingEnvironment;
import javax.lang.model.element.TypeElement;

import com.squareup.javapoet.ClassName;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.RepresentationMapping;

/** Resolves and validates the existing v3 persistence representation contract. */
public final class PersistenceRepresentationMappingResolver {
    private PersistenceRepresentationMappingResolver() {
    }

    public static Optional<ResolvedPersistenceRepresentation> resolve(
        PipelineTemplateConfig config,
        ClassName domainType,
        ProcessingEnvironment processingEnv
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
        validateTypes(processingEnv, mapping, domainType, representationName, mapperName);
        return Optional.of(new ResolvedPersistenceRepresentation(
            ClassName.bestGuess(representationName), ClassName.bestGuess(mapperName)));
    }

    private static void validateTypes(
        ProcessingEnvironment processingEnv,
        RepresentationMapping mapping,
        ClassName domainType,
        String representationName,
        String mapperName
    ) {
        if (processingEnv == null || processingEnv.getElementUtils() == null || processingEnv.getTypeUtils() == null) {
            return;
        }
        TypeElement representation = processingEnv.getElementUtils().getTypeElement(representationName);
        if (representation == null) {
            throw failure(mapping, "representation class is unavailable");
        }
        TypeElement mapper = processingEnv.getElementUtils().getTypeElement(mapperName);
        if (mapper == null) {
            throw failure(mapping, "mapper class is unavailable");
        }
        TypeElement mapperContract = processingEnv.getElementUtils().getTypeElement("org.pipelineframework.mapper.Mapper");
        TypeElement domain = processingEnv.getElementUtils().getTypeElement(domainType.canonicalName());
        if (mapperContract == null || domain == null) {
            throw failure(mapping, "canonical mapper contract is unavailable");
        }
        var expected = processingEnv.getTypeUtils().getDeclaredType(
            mapperContract, domain.asType(), representation.asType());
        if (!processingEnv.getTypeUtils().isAssignable(mapper.asType(), expected)) {
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
