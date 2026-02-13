package org.pipelineframework.search.index_document.service;

import java.util.Objects;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

@Readiness
@ApplicationScoped
public class IndexReliabilityReadinessCheck implements HealthCheck {

  private final IndexFailureParkingLot parkingLot;
  private final int unhealthyThreshold;

  @Inject
  public IndexReliabilityReadinessCheck(
      IndexFailureParkingLot parkingLot,
      @ConfigProperty(name = "search.index.parking.unhealthy-threshold", defaultValue = "25")
      int unhealthyThreshold) {
    this.parkingLot = Objects.requireNonNull(parkingLot, "parkingLot must not be null");
    if (unhealthyThreshold <= 0) {
      throw new IllegalArgumentException(
          "search.index.parking.unhealthy-threshold must be > 0, but was " + unhealthyThreshold);
    }
    this.unhealthyThreshold = unhealthyThreshold;
  }

  @Override
  public HealthCheckResponse call() {
    int parkedCount = parkingLot.size();
    boolean healthy = parkedCount < unhealthyThreshold;
    HealthCheckResponseBuilder builder = HealthCheckResponse.named("index-reliability")
        .withData("parkedFailures", parkedCount)
        .withData("unhealthyThreshold", unhealthyThreshold);
    return healthy ? builder.up().build() : builder.down().build();
  }
}
