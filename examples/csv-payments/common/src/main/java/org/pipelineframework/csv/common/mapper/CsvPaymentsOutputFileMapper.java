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

import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.ReportingPolicy;
import org.mapstruct.factory.Mappers;
import org.pipelineframework.csv.common.domain.CsvPaymentsOutputFile;
import org.pipelineframework.csv.common.dto.CsvPaymentsOutputFileDto;
import org.pipelineframework.csv.grpc.ProcessCsvPaymentsOutputFileSvc;

@Mapper(
    componentModel = "jakarta",
    uses = {CommonConverters.class},
    unmappedTargetPolicy = ReportingPolicy.WARN)
public interface CsvPaymentsOutputFileMapper extends org.pipelineframework.mapper.Mapper<CsvPaymentsOutputFile, ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile> {

  CsvPaymentsOutputFileMapper INSTANCE = Mappers.getMapper( CsvPaymentsOutputFileMapper.class );

  CsvPaymentsOutputFileDto toDto(CsvPaymentsOutputFile domain);

  CsvPaymentsOutputFile fromDto(CsvPaymentsOutputFileDto dto);

  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "filepath", qualifiedByName = "pathToString")
  @Mapping(target = "csvFolderPath", qualifiedByName = "pathToString")
  ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile toGrpc(CsvPaymentsOutputFileDto dto);

  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "filepath", qualifiedByName = "stringToPath")
  @Mapping(target = "csvFolderPath", qualifiedByName = "stringToPath")
  CsvPaymentsOutputFileDto fromGrpc(ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile proto);

  @Override
  default CsvPaymentsOutputFile fromExternal(ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile external) {
    return fromDto(fromGrpc(external));
  }

  @Override
  default ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile toExternal(CsvPaymentsOutputFile domain) {
    return toGrpc(toDto(domain));
  }

  default ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile toDtoToGrpc(CsvPaymentsOutputFile domain) {
    return toExternal(domain);
  }

  default CsvPaymentsOutputFile fromGrpcFromDto(ProcessCsvPaymentsOutputFileSvc.CsvPaymentsOutputFile grpc) {
    return fromExternal(grpc);
  }
}
