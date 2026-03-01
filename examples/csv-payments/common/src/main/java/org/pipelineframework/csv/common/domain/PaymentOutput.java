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

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvIgnore;
import com.opencsv.bean.CsvNumber;
import jakarta.persistence.*;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;
import java.util.UUID;
import lombok.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class PaymentOutput extends BaseEntity implements Serializable {

    @CsvIgnore @Transient private PaymentStatus paymentStatus;

    // en-UK locale to match the format of the (mock) payment service
    @CsvBindByName(column = "CSV Id")
    String csvId;

    @CsvBindByName(column = "Recipient")
    String recipient;

    @CsvBindByName(column = "Amount", locale = "en-UK")
    @CsvNumber("#,###.00")
    BigDecimal amount;

    @CsvBindByName(column = "Currency")
    Currency currency;

    @CsvBindByName(column = "Reference")
    UUID conversationId;

    @CsvBindByName(column = "Status")
    Long status;

    @CsvBindByName(column = "Message")
    String message;

    @CsvBindByName(column = "Fee", locale = "en-UK")
    @CsvNumber("#,###.00")
    BigDecimal fee;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PaymentOutput that = (PaymentOutput) o;
    return this.id != null && this.id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

}
