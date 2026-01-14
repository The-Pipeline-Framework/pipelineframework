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

package org.pipelineframework.search.common.domain;

import java.util.UUID;
import jakarta.persistence.*;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@MappedSuperclass
public abstract class BaseEntity {

  @Id
  @Column(name = "doc_id", updatable = false, nullable = false)
  public UUID docId;

  @PrePersist
  @PreUpdate
  protected void ensureDocId() {
    if (docId == null) {
      throw new IllegalStateException("docId is required");
    }
  }
}
