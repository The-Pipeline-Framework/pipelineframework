package org.pipelineframework.checkout.deliver_order.e2e;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.List;
import org.pipelineframework.HealthCheckService;

@Alternative
@Priority(1)
@ApplicationScoped
/**
 * Test stub for E2E runs that bypasses real dependency health checks.
 * Always returns {@code true} so pipeline startup is not blocked by external services.
 */
public class TestHealthCheckService extends HealthCheckService {

    @Override
    public boolean checkHealthOfDependentServices(List<Object> steps) {
        return true;
    }
}
