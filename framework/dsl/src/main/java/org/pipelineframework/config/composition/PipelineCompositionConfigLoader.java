package org.pipelineframework.config.composition;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateField;
import org.pipelineframework.config.template.PipelineTemplateStep;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

/**
 * Loads a pipeline composition manifest and resolves it into a validated typed handoff IR.
 */
public class PipelineCompositionConfigLoader {
    private final PipelineTemplateConfigLoader pipelineLoader;

    public PipelineCompositionConfigLoader() {
        this(new PipelineTemplateConfigLoader());
    }

    public PipelineCompositionConfigLoader(PipelineTemplateConfigLoader pipelineLoader) {
        this.pipelineLoader = pipelineLoader == null ? new PipelineTemplateConfigLoader() : pipelineLoader;
    }

    public PipelineCompositionConfig load(Path manifestPath) {
        Object root = loadYaml(manifestPath);
        if (!(root instanceof Map<?, ?> rootMap)) {
            throw new IllegalStateException("Pipeline composition root is not a map");
        }
        return new PipelineCompositionConfig(
            readVersion(rootMap),
            readRequiredString(rootMap, "name", "pipeline composition"),
            readPipelines(rootMap));
    }

    public PipelineCompositionIr loadIr(Path manifestPath) {
        PipelineCompositionConfig config = load(manifestPath);
        Path baseDir = manifestPath.toAbsolutePath().normalize().getParent();
        if (baseDir == null) {
            baseDir = Path.of(".").toAbsolutePath().normalize();
        }

        List<PipelineCompositionNode> nodes = new ArrayList<>();
        for (PipelineCompositionPipeline pipeline : config.pipelines()) {
            Path pipelinePath = resolvePipelinePath(baseDir, pipeline);
            PipelineTemplateConfig pipelineConfig = pipelineLoader.load(pipelinePath);
            nodes.add(new PipelineCompositionNode(pipeline.id(), pipelinePath, pipelineConfig));
        }
        return validate(config, nodes);
    }

    private Object loadYaml(Path path) {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(1_000_000);
        loaderOptions.setMaxAliasesForCollections(50);
        loaderOptions.setAllowDuplicateKeys(false);
        Yaml yaml = new Yaml(new SafeConstructor(loaderOptions));
        try (Reader reader = Files.newBufferedReader(path)) {
            return yaml.load(reader);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline composition config: " + path, e);
        }
    }

    private int readVersion(Map<?, ?> rootMap) {
        Object rawVersion = rootMap.get("version");
        if (rawVersion == null) {
            throw new IllegalArgumentException("pipeline composition version must be declared");
        }
        if (!(rawVersion instanceof Number number)) {
            throw new IllegalArgumentException("pipeline composition version must be numeric");
        }
        int version = number.intValue();
        if (number.doubleValue() != version) {
            throw new IllegalArgumentException("pipeline composition version must be an integer");
        }
        return version;
    }

    private List<PipelineCompositionPipeline> readPipelines(Map<?, ?> rootMap) {
        Object pipelinesObj = rootMap.get("pipelines");
        if (!(pipelinesObj instanceof Iterable<?> pipelineItems)) {
            throw new IllegalArgumentException("pipeline composition pipelines must be declared as a YAML list");
        }
        List<PipelineCompositionPipeline> pipelines = new ArrayList<>();
        int index = 0;
        for (Object pipelineObj : pipelineItems) {
            if (!(pipelineObj instanceof Map<?, ?> pipelineMap)) {
                throw new IllegalArgumentException(
                    "pipeline composition pipeline entry at index " + index + " must be a YAML map");
            }
            pipelines.add(new PipelineCompositionPipeline(
                readRequiredString(pipelineMap, "id", "pipeline composition pipeline[" + index + "]"),
                readRequiredString(pipelineMap, "path", "pipeline composition pipeline[" + index + "]")));
            index++;
        }
        return pipelines;
    }

    private String readRequiredString(Map<?, ?> map, String key, String context) {
        Object value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException(context + "." + key + " must not be blank");
        }
        if (!(value instanceof String stringValue)) {
            throw new IllegalArgumentException(context + "." + key + " must be a string");
        }
        if (stringValue.isBlank()) {
            throw new IllegalArgumentException(context + "." + key + " must not be blank");
        }
        return stringValue.trim();
    }

    private Path resolvePipelinePath(Path baseDir, PipelineCompositionPipeline pipeline) {
        Path authoredPath = Path.of(pipeline.path());
        Path resolved = authoredPath.isAbsolute()
            ? authoredPath.normalize()
            : baseDir.resolve(authoredPath).normalize();
        if (!Files.isRegularFile(resolved)) {
            throw new IllegalArgumentException(
                "Pipeline composition pipeline '" + pipeline.id() + "' file does not exist: " + resolved);
        }
        return resolved;
    }

    PipelineCompositionIr validate(PipelineCompositionConfig config, List<PipelineCompositionNode> nodes) {
        Map<String, PipelineCompositionNode> producersByPublication = new LinkedHashMap<>();
        Map<String, List<PipelineCompositionNode>> consumersByPublication = new LinkedHashMap<>();
        List<String> entrypoints = new ArrayList<>();

        for (PipelineCompositionNode node : nodes) {
            if (node.config().steps().isEmpty()) {
                throw new IllegalStateException("Pipeline '" + node.id() + "' must define at least one step");
            }
            node.checkpointPublication().ifPresent(publication -> registerProducer(producersByPublication, publication, node));
            node.subscriptionPublication().ifPresentOrElse(
                publication -> consumersByPublication.computeIfAbsent(publication, ignored -> new ArrayList<>()).add(node),
                () -> entrypoints.add(node.id()));
        }

        List<PipelineCompositionHandoff> handoffs = new ArrayList<>();
        Set<String> consumedPublications = new LinkedHashSet<>();
        for (Map.Entry<String, List<PipelineCompositionNode>> entry : consumersByPublication.entrySet()) {
            String publication = entry.getKey();
            PipelineCompositionNode producer = producersByPublication.get(publication);
            if (producer == null) {
                throw new IllegalStateException(
                    "Pipeline subscription for publication '" + publication + "' has no producer in composition");
            }
            consumedPublications.add(publication);
            for (PipelineCompositionNode consumer : entry.getValue()) {
                validateHandoff(publication, producer, consumer);
                PipelineTemplateStep terminalStep = producer.terminalStep();
                PipelineTemplateStep entryStep = consumer.entryStep();
                handoffs.add(new PipelineCompositionHandoff(
                    publication,
                    producer.id(),
                    consumer.id(),
                    terminalStep.outputTypeName(),
                    entryStep.inputTypeName()));
            }
        }

        List<String> terminalPublications = producersByPublication.keySet().stream()
            .filter(publication -> !consumedPublications.contains(publication))
            .toList();

        return new PipelineCompositionIr(config, nodes, handoffs, entrypoints, terminalPublications);
    }

    private void registerProducer(
        Map<String, PipelineCompositionNode> producersByPublication,
        String publication,
        PipelineCompositionNode producer
    ) {
        PipelineCompositionNode previous = producersByPublication.putIfAbsent(publication, producer);
        if (previous != null) {
            throw new IllegalStateException(
                "Duplicate checkpoint publication '" + publication + "' produced by pipelines '"
                    + previous.id() + "' and '" + producer.id() + "'");
        }
    }

    private void validateHandoff(String publication, PipelineCompositionNode producer, PipelineCompositionNode consumer) {
        PipelineTemplateStep terminalStep = producer.terminalStep();
        PipelineTemplateStep entryStep = consumer.entryStep();
        if (!Objects.equals(terminalStep.outputTypeName(), entryStep.inputTypeName())) {
            throw new IllegalStateException(
                "Pipeline handoff type mismatch for publication '" + publication + "': producer '"
                    + producer.id() + "' outputs '" + terminalStep.outputTypeName() + "' but consumer '"
                    + consumer.id() + "' expects '" + entryStep.inputTypeName() + "'");
        }
        validateFieldCompatibility(publication, producer, consumer, terminalStep.outputFields(), entryStep.inputFields());
    }

    private void validateFieldCompatibility(
        String publication,
        PipelineCompositionNode producer,
        PipelineCompositionNode consumer,
        List<PipelineTemplateField> outputFields,
        List<PipelineTemplateField> inputFields
    ) {
        Map<String, PipelineTemplateField> outputs = byFieldName(outputFields, producer.id(), "output");
        Map<String, PipelineTemplateField> inputs = byFieldName(inputFields, consumer.id(), "input");
        if (!outputs.keySet().equals(inputs.keySet())) {
            throw new IllegalStateException(
                "Pipeline handoff fields mismatch for publication '" + publication + "' from '"
                    + producer.id() + "' to '" + consumer.id() + "': producer fields="
                    + outputs.keySet() + ", consumer fields=" + inputs.keySet());
        }
        for (Map.Entry<String, PipelineTemplateField> entry : outputs.entrySet()) {
            String fieldName = entry.getKey();
            PipelineTemplateField output = entry.getValue();
            PipelineTemplateField input = inputs.get(fieldName);
            assertFieldValue(publication, producer, consumer, fieldName, "number", output.number(), input.number());
            assertFieldValue(publication, producer, consumer, fieldName, "type", output.type(), input.type());
            assertFieldValue(publication, producer, consumer, fieldName, "protoType", output.protoType(), input.protoType());
        }
    }

    private Map<String, PipelineTemplateField> byFieldName(
        List<PipelineTemplateField> fields,
        String pipelineId,
        String direction
    ) {
        Map<String, PipelineTemplateField> byName = new LinkedHashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            PipelineTemplateField field = fields.get(i);
            if (field == null || field.name() == null || field.name().isBlank()) {
                throw new IllegalStateException(
                    "Pipeline '" + pipelineId + "' has invalid " + direction + " field at index " + i);
            }
            PipelineTemplateField previous = byName.putIfAbsent(field.name(), field);
            if (previous != null) {
                throw new IllegalStateException(
                    "Pipeline '" + pipelineId + "' has duplicate " + direction + " field '" + field.name() + "'");
            }
        }
        return byName;
    }

    private void assertFieldValue(
        String publication,
        PipelineCompositionNode producer,
        PipelineCompositionNode consumer,
        String fieldName,
        String attribute,
        Object outputValue,
        Object inputValue
    ) {
        if (!Objects.equals(outputValue, inputValue)) {
            throw new IllegalStateException(
                "Pipeline handoff field " + attribute + " mismatch for publication '" + publication
                    + "' field '" + fieldName + "' from '" + producer.id() + "' to '" + consumer.id()
                    + "': producer=" + outputValue + ", consumer=" + inputValue);
        }
    }
}
