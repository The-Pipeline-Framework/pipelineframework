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

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Objects;
import jakarta.persistence.Convert;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Transient;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

import static java.text.MessageFormat.format;

@Getter
@Setter
@Accessors(chain = true)
@MappedSuperclass
@NoArgsConstructor
@SuppressWarnings("removal")
public abstract class BaseCsvPaymentsFile extends BaseEntity implements Serializable {
  @Transient @NonNull protected File csvFile;

  @Convert(converter = PathConverter.class)
  protected Path filepath;

  public BaseCsvPaymentsFile(File file) {
    super();
    this.csvFile = Objects.requireNonNull(file, "file must not be null");
    this.filepath = file.toPath();
  }

  @Override
  public String toString() {
    return format("CsvPaymentsFile'{'filepath=''{0}'''}'", filepath);
  }

  @PrePersist
  protected void useStableFileId() {
    CsvPaymentsStableIdSupport.stableId(getClass().getSimpleName(), filepath).ifPresent(stableId -> id = stableId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filepath);
  }
}
