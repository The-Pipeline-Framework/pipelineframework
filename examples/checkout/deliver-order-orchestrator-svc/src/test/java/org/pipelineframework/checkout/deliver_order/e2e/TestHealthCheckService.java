package org.pipelineframework.checkout.deliver_order.e2e;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import org.pipelineframework.HealthCheckService;

/**
 * Test stub for E2E runs that bypasses real dependency health checks.
 * Always returns {@code true} so pipeline startup is not blocked by external services.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class TestHealthCheckService extends HealthCheckService {

    /**
     * Always reports dependent services as healthy to bypass real health checks during end-to-end tests.
     *
     * @param steps a list of health-check steps (ignored by this test implementation)
     * @return true indicating dependent services are considered healthy
     */
    @Override
    public boolean checkHealthOfDependentServices(List<Object> steps) {
        return true;
    }
}