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

package org.pipelineframework.csv.common.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.nio.file.Path;
import java.util.UUID;
import lombok.Builder;
import lombok.Value;

/**
 * Legacy DTO for the direct CSV output-file step.
 *
 * @deprecated Use Object Publish with {@code CsvPaymentOutputPublishMapper}; published files are represented as
 *             framework-owned object writes instead of this DTO.
 */
@Value
@Builder
@JsonDeserialize(builder = CsvPaymentsOutputFileDto.CsvPaymentsInputFileDtoBuilder.class)
@Deprecated(since = "26.6.2", forRemoval = true)
public class CsvPaymentsOutputFileDto {
  UUID id;
  Path filepath;
  Path csvFolderPath;

  // Lombok will generate the builder, but Jackson needs to know how to interpret it
  @JsonPOJOBuilder(withPrefix = "")
  public static class CsvPaymentsInputFileDtoBuilder {}
}
