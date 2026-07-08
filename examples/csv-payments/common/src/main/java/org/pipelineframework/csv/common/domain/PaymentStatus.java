/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.csv.common.domain;

import static java.text.MessageFormat.format;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import jakarta.persistence.Column;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Transient;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@MappedSuperclass
@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
@JsonDeserialize(using = PaymentStatusJsonDeserializer.class)
public abstract sealed class PaymentStatus extends BaseEntity implements Serializable
    permits ApprovedPaymentStatus, UnapprovedPaymentStatus {

  private String customerReference;

  @NonNull
  @Column(nullable = false)
  private String reference;

  @NonNull private String status;
  @NonNull private String message;
  @NonNull private BigDecimal fee;

  @NonNull private UUID conversationId;
  @NonNull private Long statusCode;

  @Transient private PaymentRecord paymentRecord;
  @NonNull private UUID paymentRecordId;

  @PrePersist
  public void useStablePaymentStatusId() {
    CsvPaymentsStableIdSupport.stableId(
            getClass().getSimpleName(), paymentRecordId, reference, status, statusCode, message)
        .ifPresent(stableId -> id = stableId);
  }

  @Override
  public String toString() {
    return format(
        "{0}'{'reference=''{1}'', message=''{2}'', status=''{3}'', fee={4}, recordId={5}'}'",
        getClass().getSimpleName(), reference, message, status, fee, paymentRecordId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PaymentStatus that = (PaymentStatus) o;
    return this.id != null && this.id.equals(that.id);
  }
}
