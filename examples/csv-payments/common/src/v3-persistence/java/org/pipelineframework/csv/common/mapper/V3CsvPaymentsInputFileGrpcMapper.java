package org.pipelineframework.csv.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.csv.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.domain.PipelineDomainProtoAdapters;
import org.pipelineframework.csv.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;

/** Bridges the existing object-ingest adapter contract to v3 generated protobuf. */
@ApplicationScoped
public class V3CsvPaymentsInputFileGrpcMapper
    implements Mapper<CsvPaymentsInputFile, PipelineTypes.CsvPaymentsInputFile> {

  @Override
  public CsvPaymentsInputFile fromExternal(PipelineTypes.CsvPaymentsInputFile external) {
    return PipelineDomainProtoAdapters.fromProto(external);
  }

  @Override
  public PipelineTypes.CsvPaymentsInputFile toExternal(CsvPaymentsInputFile domain) {
    return PipelineDomainProtoAdapters.toProto(domain);
  }
}
