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
import jakarta.persistence.Convert;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.Objects;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@MappedSuperclass
@Getter
@Setter
@NoArgsConstructor
public abstract class AbstractPaymentOutput extends BaseEntity implements Serializable {

  @CsvIgnore private String csvPaymentsOutputFilename;

  @CsvIgnore
  @Convert(converter = PathConverter.class)
  private Path csvPaymentsInputFilePath;

  @CsvBindByName(column = "CSV Id")
  String csvId;

  @CsvBindByName(column = "Recipient")
  String recipient;

  @CsvBindByName(column = "Amount", locale = "en-GB")
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

  @CsvBindByName(column = "Fee", locale = "en-GB")
  @CsvNumber("#,###.00")
  BigDecimal fee;

  @PrePersist
  public void useStablePaymentOutputId() {
    CsvPaymentsStableIdSupport.stableId(
            "PaymentOutput", csvId, recipient, amount, currency, status, message)
        .ifPresent(stableId -> id = stableId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    AbstractPaymentOutput that = (AbstractPaymentOutput) o;
    return this.id != null && this.id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
