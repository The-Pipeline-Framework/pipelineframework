package org.pipelineframework.csv.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.csv.domain.PaymentOutput;
import org.pipelineframework.csv.domain.PipelineDomainProtoAdapters;
import org.pipelineframework.csv.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;

/** Bridges the existing object-publish adapter contract to v3 generated protobuf. */
@ApplicationScoped
public class V3PaymentOutputGrpcMapper implements Mapper<PaymentOutput, PipelineTypes.PaymentOutput> {

  @Override
  public PaymentOutput fromExternal(PipelineTypes.PaymentOutput external) {
    return PipelineDomainProtoAdapters.fromProto(external);
  }

  @Override
  public PipelineTypes.PaymentOutput toExternal(PaymentOutput domain) {
    return PipelineDomainProtoAdapters.toProto(domain);
  }
}
