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
    public String status;
    public Integer updatedAt;
    public String deletedAt;

    protected CustomerRiskEntity() {
    }

    public CustomerRiskEntity(String customerId, String riskBand, int score) {
        this(customerId, riskBand, score, "ACTIVE", 0, null);
    }

    public CustomerRiskEntity(String customerId, String riskBand, int score, String status, Integer updatedAt, String deletedAt) {
        this.customerId = customerId;
        this.riskBand = riskBand;
        this.score = score;
        this.status = status;
        this.updatedAt = updatedAt;
        this.deletedAt = deletedAt;
    }
}
