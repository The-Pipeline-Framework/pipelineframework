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
import lombok.*;

/**
 * DTO for CsvFolder.
 *
 * @deprecated DTO for the legacy folder admission boundary. Object ingest
 *     sources replace this boundary.
 */
@Deprecated(since = "26.6.2", forRemoval = true)
@Value
@Builder
@JsonDeserialize(builder = CsvFolderDto.CsvFolderDtoBuilder.class)
public class CsvFolderDto {
    Path path;

    // Lombok will generate the builder, but Jackson needs to know how to interpret it
    @JsonPOJOBuilder(withPrefix = "")
    public static class CsvFolderDtoBuilder {}
}
