package org.pipelineframework.checkout.create_order.e2e;

import java.util.List;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import org.pipelineframework.HealthCheckService;

/**
 * Test stub for E2E runs that bypasses real dependency health checks.
 * Always returns {@code true} so pipeline startup is not blocked by external services.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class TestHealthCheckService extends HealthCheckService {
    @Override
    public boolean checkHealthOfDependentServices(List<Object> steps) {
        return true;
    }
}
