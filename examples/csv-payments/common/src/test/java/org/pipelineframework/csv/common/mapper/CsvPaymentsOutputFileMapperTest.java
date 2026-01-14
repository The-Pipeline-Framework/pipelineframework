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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.csv.common.domain.CsvPaymentsOutputFile;
import org.pipelineframework.csv.grpc.ProcessCsvPaymentsOutputFileSvc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CsvPaymentsOutputFileMapperTest {

    private CsvPaymentsOutputFileMapper mapper;
    private CommonConverters commonConverters;

    @BeforeEach
    void setUp() {
        commonConverters = new CommonConverters();

        // Create CsvPaymentsOutputFileMapperImpl and set its dependencies
        CsvPaymentsOutputFileMapperImpl csvPaymentsOutputFileMapperImpl =
                new CsvPaymentsOutputFileMapperImpl();
        try {
            java.lang.reflect.Field commonConvertersField =
                    CsvPaymentsOutputFileMapperImpl.class.getDeclaredField("commonConverters");
            commonConvertersField.setAccessible(true);
            commonConvertersField.set(csvPaymentsOutputFileMapperImpl, commonConverters);

            mapper = csvPaymentsOutputFileMapperImpl;
        } catch (Exception e) {
            throw new RuntimeException("Failed to set CsvPaymentsOutputFileMapper dependencies", e);
        }
    }

    @Test
    void testDomainToGrpc() {
        // Given
        CsvPaymentsOutputFile domain = new CsvPaymentsOutputFile();
        domain.setId(UUID.randomUUID());
        domain.setFilepath(Path.of("/test/output/file.csv"));
        domain.setCsvFolderPath(Path.of("/test/output"));

        // When
        ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile grpc = mapper.toDtoToGrpc(domain);

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

        ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile grpc =
                ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile.newBuilder()
                        .setId(id.toString())
                        .setFilepath("/test/output/file.csv")
                        .setCsvFolderPath("/test/output")
                        .build();

        // When
        CsvPaymentsOutputFile domain = mapper.fromGrpcFromDto(grpc);

        // Then
        assertNotNull(domain);
        assertEquals(id, domain.getId());
        assertEquals(Path.of("/test/output/file.csv"), domain.getFilepath());
        assertEquals(Path.of("/test/output"), domain.getCsvFolderPath());
    }

    // @Test
    // void testSerializeDeserialize() throws Exception {
    //   // Build a simple DTO-like object for testing
    //   ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile.Builder builder =
    //       ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile.newBuilder()
    //           .setId(UUID.randomUUID().toString())
    //           .setFilepath("/test/output/file.csv")
    //           .setCsvFolderPath("/test/output");
    //
    //   ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile file = builder.build();

    //   ObjectMapper mapper = new ObjectMapper();
    //
    //   // Serialize to JSON
    //   String json = mapper.writeValueAsString(file);

    //   // Deserialize back
    //   ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile deserialized = mapper.readValue(json,
    // ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile.class);

    //   // Assert equality
    //   assertEquals(file, deserialized);
    // }
}
