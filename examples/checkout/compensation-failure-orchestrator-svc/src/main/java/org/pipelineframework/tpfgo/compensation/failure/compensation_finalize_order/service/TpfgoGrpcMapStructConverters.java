package org.pipelineframework.tpfgo.compensation.failure.compensation_finalize_order.service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.mapstruct.Mapper;
import org.mapstruct.Named;
import org.pipelineframework.tpfgo.common.util.GrpcMappingSupport;

@Mapper(componentModel = "jakarta", implementationName = "CompensationFailureGrpcMapStructConvertersImpl")
public class TpfgoGrpcMapStructConverters {
    @Named("stringToUUID")
    public UUID stringToUUID(String value) {
        return GrpcMappingSupport.uuid(value, "uuid");
    }

    @Named("uuidToString")
    public String uuidToString(UUID value) {
        return GrpcMappingSupport.str(value);
    }

    @Named("stringToInstant")
    public Instant stringToInstant(String value) {
        return GrpcMappingSupport.instant(value, "instant");
    }

    @Named("instantToString")
    public String instantToString(Instant value) {
        return GrpcMappingSupport.str(value);
    }

    @Named("stringToBigDecimal")
    public BigDecimal stringToBigDecimal(String value) {
        return GrpcMappingSupport.decimal(value, "decimal");
    }

    @Named("bigDecimalToString")
    public String bigDecimalToString(BigDecimal value) {
        return GrpcMappingSupport.str(value);
    }
}
