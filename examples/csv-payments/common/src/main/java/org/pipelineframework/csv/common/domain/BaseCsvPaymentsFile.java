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

import jakarta.persistence.Convert;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.Transient;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Objects;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
@MappedSuperclass
@NoArgsConstructor
public abstract class BaseCsvPaymentsFile extends BaseEntity implements Serializable {
  @Transient @NonNull protected File csvFile;

  @Convert(converter = PathConverter.class)
  protected Path filepath;

  @Transient private CsvFolder csvFolder;

  @Convert(converter = PathConverter.class)
  private Path csvFolderPath;

  public BaseCsvPaymentsFile(File file) {
    super();
    this.csvFile = file;
    this.filepath = Path.of(file.getPath());
    this.csvFolderPath = Path.of(file.getParent());
    this.csvFolder = new CsvFolder(this.csvFolderPath);
  }

  @Override
  public String toString() {
    return format("CsvPaymentsFile'{'filepath=''{0}'''}'", filepath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filepath);
  }
}
