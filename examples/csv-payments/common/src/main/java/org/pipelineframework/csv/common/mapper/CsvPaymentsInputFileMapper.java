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
import org.pipelineframework.csv.common.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.common.dto.CsvPaymentsInputFileDto;
import org.pipelineframework.csv.grpc.PipelineTypes;

@SuppressWarnings("unused")
@Mapper(componentModel = "jakarta", uses = {CommonConverters.class}, unmappedTargetPolicy = ReportingPolicy.WARN)
public interface CsvPaymentsInputFileMapper extends org.pipelineframework.mapper.Mapper<CsvPaymentsInputFile, PipelineTypes.CsvPaymentsInputFile> {

  CsvPaymentsInputFileMapper INSTANCE = Mappers.getMapper( CsvPaymentsInputFileMapper.class );

  CsvPaymentsInputFileDto toDto(CsvPaymentsInputFile entity);

  CsvPaymentsInputFile fromDto(CsvPaymentsInputFileDto dto);

  @Mapping(target = "id", qualifiedByName = "uuidToString")
  @Mapping(target = "filepath", qualifiedByName = "pathToString")
  @Mapping(target = "csvFolderPath", qualifiedByName = "pathToString")
  PipelineTypes.CsvPaymentsInputFile toGrpc(CsvPaymentsInputFileDto entity);

  @Mapping(target = "id", qualifiedByName = "stringToUUID")
  @Mapping(target = "filepath", qualifiedByName = "stringToPath")
  @Mapping(target = "csvFolderPath", qualifiedByName = "stringToPath")
  CsvPaymentsInputFileDto fromGrpc(PipelineTypes.CsvPaymentsInputFile proto);

  @Override
  default CsvPaymentsInputFile fromExternal(PipelineTypes.CsvPaymentsInputFile external) {
    return fromDto(fromGrpc(external));
  }

  @Override
  default PipelineTypes.CsvPaymentsInputFile toExternal(CsvPaymentsInputFile domain) {
    return toGrpc(toDto(domain));
  }

  default org.pipelineframework.csv.grpc.PipelineTypes.CsvPaymentsInputFile toDtoToGrpc(CsvPaymentsInputFile domain) {
    return toExternal(domain);
  }

  default CsvPaymentsInputFile fromGrpcFromDto(org.pipelineframework.csv.grpc.PipelineTypes.CsvPaymentsInputFile grpc) {
    return fromExternal(grpc);
  }
}
