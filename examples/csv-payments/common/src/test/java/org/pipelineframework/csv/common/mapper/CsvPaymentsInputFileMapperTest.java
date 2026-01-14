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

package org.pipelineframework.csv.common.mapper;

import java.nio.file.Path;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.common.dto.CsvPaymentsInputFileDto;
import org.pipelineframework.csv.grpc.ProcessFolderSvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CsvPaymentsInputFileMapperTest {

    private CsvPaymentsInputFileMapper mapper;
    private CommonConverters commonConverters;

    @BeforeEach
    void setUp() {
        commonConverters = new CommonConverters();

        // Create CsvPaymentsInputFileMapperImpl and set its dependencies
        CsvPaymentsInputFileMapperImpl csvPaymentsInputFileMapperImpl =
                new CsvPaymentsInputFileMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    CsvPaymentsInputFileMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(csvPaymentsInputFileMapperImpl, commonConverters);

            mapper = csvPaymentsInputFileMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set CsvPaymentsInputFileMapper dependencies", e);
        }
    }

    @Test
    void testDomainToDto() {
        // Given
        CsvPaymentsInputFile domain = new CsvPaymentsInputFile();
        domain.setId(UUID.randomUUID());
        domain.setFilepath(Path.of("/test/input/file.csv"));
        domain.setCsvFolderPath(Path.of("/test/input"));

        // When
        CsvPaymentsInputFileDto dto = mapper.toDto(domain);

        // Then
        assertNotNull(dto);
        assertEquals(domain.getId(), dto.getId());
        assertEquals(domain.getFilepath(), dto.getFilepath());
        assertEquals(domain.getCsvFolderPath(), dto.getCsvFolderPath());
    }

    @Test
    void testDtoToDomain() {
        // Given
        CsvPaymentsInputFileDto dto =
                CsvPaymentsInputFileDto.builder()
                        .id(UUID.randomUUID())
                        .filepath(Path.of("/test/input/file.csv"))
                        .csvFolderPath(Path.of("/test/input"))
                        .build();

        // When
        CsvPaymentsInputFile domain = mapper.fromDto(dto);

        // Then
        assertNotNull(domain);
        assertEquals(dto.getId(), domain.getId());
        assertEquals(dto.getFilepath(), domain.getFilepath());
        assertEquals(dto.getCsvFolderPath(), domain.getCsvFolderPath());
    }

    @Test
    void testDtoToGrpc() {
        // Given
        CsvPaymentsInputFileDto dto =
                CsvPaymentsInputFileDto.builder()
                        .id(UUID.randomUUID())
                        .filepath(Path.of("/test/input/file.csv"))
                        .csvFolderPath(Path.of("/test/input"))
                        .build();

        // When
        ProcessFolderSvc.CsvPaymentsInputFile grpc = mapper.toGrpc(dto);

        // Then
        assertNotNull(grpc);
        assertEquals(dto.getId().toString(), grpc.getId());
        assertEquals(dto.getFilepath().toString(), grpc.getFilepath());
        assertEquals(dto.getCsvFolderPath().toString(), grpc.getCsvFolderPath());
    }

    @Test
    void testGrpcToDto() {
        // Given
        UUID id = UUID.randomUUID();

        ProcessFolderSvc.CsvPaymentsInputFile grpc =
                ProcessFolderSvc.CsvPaymentsInputFile.newBuilder()
                        .setId(id.toString())
                        .setFilepath("/test/input/file.csv")
                        .setCsvFolderPath("/test/input")
                        .build();

        // When
        CsvPaymentsInputFileDto dto = mapper.fromGrpc(grpc);

        // Then
        assertNotNull(dto);
        assertEquals(id, dto.getId());
        assertEquals(Path.of("/test/input/file.csv"), dto.getFilepath());
        assertEquals(Path.of("/test/input"), dto.getCsvFolderPath());
    }

    @Test
    void testDomainToGrpc() {
        // Given
        CsvPaymentsInputFile domain = new CsvPaymentsInputFile();
        domain.setId(UUID.randomUUID());
        domain.setFilepath(Path.of("/test/input/file.csv"));
        domain.setCsvFolderPath(Path.of("/test/input"));

        // When
        ProcessFolderSvc.CsvPaymentsInputFile grpc = mapper.toDtoToGrpc(domain);

        // Then
        assertNotNull(grpc);
        assertEquals(domain.getId().toString(), grpc.getId());
        assertEquals(domain.getFilepath().toString(), grpc.getFilepath());
        assertEquals(domain.getCsvFolderPath().toString(), grpc.getCsvFolderPath());
    }

    @Test
    void testGrpcToDomain() {
        // Given
        UUID id = UUID.randomUUID();

        ProcessFolderSvc.CsvPaymentsInputFile grpc =
                ProcessFolderSvc.CsvPaymentsInputFile.newBuilder()
                        .setId(id.toString())
                        .setFilepath("/test/input/file.csv")
                        .setCsvFolderPath("/test/input")
                        .build();

        // When
        CsvPaymentsInputFile domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals(id, domain.getId());
        assertEquals(Path.of("/test/input/file.csv"), domain.getFilepath());
        assertEquals(Path.of("/test/input"), domain.getCsvFolderPath());
    }

    @Test
    void testSerializeDeserialize() throws Exception {
        // Build DTO
        CsvPaymentsInputFileDto dto =
                CsvPaymentsInputFileDto.builder()
                        .id(UUID.randomUUID())
                        .filepath(Path.of("/test/input/file.csv"))
                        .csvFolderPath(Path.of("/test/input"))
                        .build();

        ObjectMapper mapper = new ObjectMapper();

        // Serialize to JSON
        String json = mapper.writeValueAsString(dto);

        // Deserialize back
        CsvPaymentsInputFileDto deserialized =
                mapper.readValue(json, CsvPaymentsInputFileDto.class);

        // Assert equality
        assertEquals(dto, deserialized);
    }
}
