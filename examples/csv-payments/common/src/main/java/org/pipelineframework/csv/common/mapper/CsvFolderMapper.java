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
import org.pipelineframework.csv.common.domain.CsvFolder;
import org.pipelineframework.csv.common.dto.CsvFolderDto;
import org.pipelineframework.csv.grpc.ProcessFolderSvc;

@SuppressWarnings("unused")
@Mapper(componentModel = "jakarta", uses = {CommonConverters.class}, unmappedTargetPolicy = ReportingPolicy.WARN)
public interface CsvFolderMapper extends org.pipelineframework.mapper.Mapper<CsvFolder, ProcessFolderSvc.CsvFolder> {

    CsvFolderMapper INSTANCE = Mappers.getMapper( CsvFolderMapper.class );

    CsvFolderDto toDto(CsvFolder domain);

    CsvFolder fromDto(CsvFolderDto dto);

    @Mapping(target = "path", qualifiedByName = "stringToPath")
    CsvFolderDto fromGrpc(ProcessFolderSvc.CsvFolder grpc);

    @Mapping(target = "path", qualifiedByName = "pathToString")
    ProcessFolderSvc.CsvFolder toGrpc(CsvFolderDto dto);

    @Override
    default CsvFolder fromExternal(ProcessFolderSvc.CsvFolder external) {
        return fromDto(fromGrpc(external));
    }

    @Override
    default ProcessFolderSvc.CsvFolder toExternal(CsvFolder domain) {
        return toGrpc(toDto(domain));
    }
}
