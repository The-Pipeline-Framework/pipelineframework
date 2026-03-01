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

import jakarta.persistence.Entity;
import jakarta.persistence.Transient;
import java.io.Serializable;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Entity
@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
public class AckPaymentSent extends BaseEntity implements Serializable {
  @NonNull private UUID conversationId;

  private Long status;
  private String message;

  @Transient private PaymentRecord paymentRecord;
  private UUID paymentRecordId;

  public AckPaymentSent(@NonNull UUID conversationId) {
    super();
    this.conversationId = conversationId;
  }

  @Override
  public String toString() {
    return format(
        "AckPaymentSent'{'id=''{0}'', conversationId=''{1}'', status={2}, message={3}, recordId={4}'}'",
        id, conversationId, status, message, paymentRecordId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AckPaymentSent that = (AckPaymentSent) o;
    return this.id != null && this.id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return this.id != null ? this.id.hashCode() : 0;
  }
}
