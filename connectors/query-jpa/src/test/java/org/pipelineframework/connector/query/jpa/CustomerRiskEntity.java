package org.pipelineframework.connector.query.jpa;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

@Entity
public class CustomerRiskEntity {
    @Id
    @GeneratedValue
    public Long id;

    public String customerId;
    public String riskBand;
    public int score;

    protected CustomerRiskEntity() {
    }

    public CustomerRiskEntity(String customerId, String riskBand, int score) {
        this.customerId = customerId;
        this.riskBand = riskBand;
        this.score = score;
    }
}
