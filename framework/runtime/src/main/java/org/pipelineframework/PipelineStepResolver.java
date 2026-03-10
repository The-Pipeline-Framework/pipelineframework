package org.pipelineframework;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jakarta.enterprise.context.ApplicationScoped;

import org.jboss.logging.Logger;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.config.pipeline.PipelineOrderResourceLoader;

/**
 * Resolves, orders, and instantiates pipeline steps from generated metadata.
 */
@ApplicationScoped
class PipelineStepResolver {

  private static final Logger LOG = Logger.getLogger(PipelineStepResolver.class);

  List<Object> loadPipelineSteps() {
    try {
      java.util.Optional<List<String>> resourceOrder = PipelineOrderResourceLoader.loadOrder();
      if (resourceOrder.isEmpty()) {
        if (PipelineOrderResourceLoader.requiresOrder()) {
          throw new PipelineExecutionService.PipelineConfigurationException(
              "Pipeline order metadata not found. Ensure META-INF/pipeline/order.json is generated at build time.");
        }
        return Collections.emptyList();
      }
      List<String> orderedStepNames = resourceOrder.get();
      if (orderedStepNames.isEmpty()) {
        throw new PipelineExecutionService.PipelineConfigurationException(
            "Pipeline order metadata is empty. Ensure pipeline.yaml defines steps for order generation.");
      }
      if (LOG.isInfoEnabled()) {
        LOG.infof("Loaded pipeline step order (%d steps): %s", orderedStepNames.size(), orderedStepNames);
      }
      return instantiateStepsInOrder(orderedStepNames);
    } catch (PipelineExecutionService.PipelineConfigurationException e) {
      throw e;
    } catch (Exception e) {
      LOG.errorf(e, "Failed to load configuration: %s", e.getMessage());
      throw new PipelineExecutionService.PipelineConfigurationException(
          "Failed to load pipeline configuration: " + e.getMessage(),
          e);
    }
  }

  List<Object> instantiateStepsFromConfig(Map<String, PipelineStepConfig.StepConfig> stepConfigs) {
    List<String> orderedStepNames = stepConfigs.keySet().stream().sorted().toList();

    List<Object> steps = new ArrayList<>();
    List<String> failedSteps = new ArrayList<>();
    for (String stepClassName : orderedStepNames) {
      Object step = createStepFromConfig(stepClassName);
      if (step != null) {
        steps.add(step);
      } else {
        failedSteps.add(stepClassName);
      }
    }

    if (!failedSteps.isEmpty()) {
      String message = String.format("Failed to instantiate %d step(s): %s", failedSteps.size(), String.join(", ", failedSteps));
      LOG.error(message);
      throw new PipelineExecutionService.PipelineConfigurationException(message);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debugf("Loaded %d pipeline steps from application properties", steps.size());
    }
    return steps;
  }

  private List<Object> instantiateStepsInOrder(List<String> orderedStepNames) {
    List<Object> steps = new ArrayList<>();
    List<String> failedSteps = new ArrayList<>();
    for (String stepClassName : orderedStepNames) {
      Object step = createStepFromConfig(stepClassName);
      if (step != null) {
        steps.add(step);
      } else {
        failedSteps.add(stepClassName);
      }
    }

    if (!failedSteps.isEmpty()) {
      String message = String.format("Failed to instantiate %d step(s): %s", failedSteps.size(), String.join(", ", failedSteps));
      LOG.error(message);
      throw new PipelineExecutionService.PipelineConfigurationException(message);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debugf("Loaded %d pipeline steps from generated order metadata", steps.size());
    }
    return steps;
  }

  private Object createStepFromConfig(String stepClassName) {
    try {
      ClassLoader[] candidates = new ClassLoader[] {
          Thread.currentThread().getContextClassLoader(),
          PipelineExecutionService.class.getClassLoader(),
          ClassLoader.getSystemClassLoader() };

      Class<?> stepClass = null;
      for (ClassLoader candidate : candidates) {
        if (candidate == null) {
          continue;
        }
        try {
          stepClass = Class.forName(stepClassName, true, candidate);
          break;
        } catch (ClassNotFoundException ignored) {
          // try next loader
        }
      }

      if (stepClass == null) {
        throw new ClassNotFoundException(stepClassName);
      }
      io.quarkus.arc.InstanceHandle<?> handle = io.quarkus.arc.Arc.container().instance(stepClass);
      if (!handle.isAvailable()) {
        int beanCount = io.quarkus.arc.Arc.container().beanManager().getBeans(stepClass).size();
        ClassLoader loader = stepClass.getClassLoader();
        LOG.errorf("No CDI bean available for pipeline step %s (beans=%d, loader=%s)",
            stepClassName,
            beanCount,
            loader);
        return null;
      }
      return handle.get();
    } catch (Exception e) {
      LOG.errorf(e, "Failed to instantiate pipeline step: %s, error: %s", stepClassName, e.getMessage());
      return null;
    }
  }
}
