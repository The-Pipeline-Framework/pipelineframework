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

import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import jakarta.persistence.Entity;
import jakarta.persistence.Transient;
import java.io.*;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
public class CsvPaymentsOutputFile extends BaseCsvPaymentsFile implements AutoCloseable {

  @Transient private List<PaymentOutput> paymentOutputs = new ArrayList<>();

  @Transient StatefulBeanToCsv<PaymentOutput> sbc;

  @Transient private Writer writer;

  public CsvPaymentsOutputFile(@NonNull Path csvPaymentsInputFilepath) throws IOException {
    super(new File(MessageFormat.format("{0}.out", csvPaymentsInputFilepath)));
    // Create the CSV writer
    writer = new BufferedWriter(new FileWriter(String.valueOf(this.filepath)));
    sbc =
        new StatefulBeanToCsvBuilder<PaymentOutput>(writer)
            .withQuotechar('\'')
            .withSeparator(com.opencsv.CSVWriter.DEFAULT_SEPARATOR)
            .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CsvPaymentsOutputFile that = (CsvPaymentsOutputFile) o;
    return this.id != null && this.id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }
}
