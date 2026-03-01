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

import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvNumber;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.Currency;
import java.util.Locale;
import java.util.Objects;
import lombok.*;
import lombok.experimental.Accessors;

@Entity
@Getter
@Setter
@Accessors(chain = true)
@NoArgsConstructor
public class PaymentRecord extends BaseEntity implements Serializable {
  @CsvBindByName(column = "ID")
  private String csvId;

  @CsvBindByName(column = "Recipient")
  private String recipient;

  // en-UK locale to match the format of the csv input files
  @CsvBindByName(column = "Amount", locale = "en-UK")
  @CsvNumber("#,###.00")
  private BigDecimal amount;

  @CsvBindByName(column = "Currency")
  private Currency currency;

  @Convert(converter = PathConverter.class)
  private Path csvPaymentsInputFilePath;

  @Override
  public String toString() {
    return format(
        "PaymentRecord'{'id=''{0}'', recipient=''{1}'', amount={2}, currency={3}, file={4}'}'",
        csvId,
        recipient,
        NumberFormat.getCurrencyInstance(Locale.UK).format(amount),
        currency,
        csvPaymentsInputFilePath != null ? csvPaymentsInputFilePath.toString() : "null");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PaymentRecord that = (PaymentRecord) o;
    return this.id != null && this.id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
