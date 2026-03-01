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

import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import jakarta.persistence.Entity;
import java.io.*;
import java.nio.charset.StandardCharsets;
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
public class CsvPaymentsInputFile extends BaseCsvPaymentsFile implements CsvPaymentsInput {

  public CsvPaymentsInputFile(@NonNull File csvFile) {
    super(csvFile);
  }

  @Override
  public Reader openReader() throws IOException {
    return new BufferedReader(new FileReader(filepath.toFile(), StandardCharsets.UTF_8));
  }

  @Override
  public String getSourceName() {
    return filepath.toString();
  }

  @Override
  public HeaderColumnNameMappingStrategy<PaymentRecord> veryOwnStrategy() {
    var strategy = new FilePathAwareMappingStrategy<PaymentRecord>(this.filepath);
    strategy.setType(PaymentRecord.class);

    return strategy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CsvPaymentsInputFile that = (CsvPaymentsInputFile) o;
    return this.id != null && this.id.equals(that.id);
  }
}
