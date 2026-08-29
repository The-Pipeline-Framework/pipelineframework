package org.pipelineframework.connector.query.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

@Entity
public class ReactiveCustomerRiskEntity {
    @Id
    @GeneratedValue
    public Long id;

    public String customerId;
    public String riskBand;
    public int score;
    public String status;

    protected ReactiveCustomerRiskEntity() {
    }

    ReactiveCustomerRiskEntity(String customerId, String riskBand, int score, String status) {
        this.customerId = customerId;
        this.riskBand = riskBand;
        this.score = score;
        this.status = status;
    }
}
