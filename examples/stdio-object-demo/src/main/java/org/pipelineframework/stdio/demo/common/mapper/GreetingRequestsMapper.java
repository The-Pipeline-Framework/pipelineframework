package org.pipelineframework.stdio.demo.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.Objects;

import org.pipelineframework.stdio.demo.common.domain.GreetingRequests;
import org.pipelineframework.stdio.demo.common.dto.GreetingRequestsDto;

@ApplicationScoped
public class GreetingRequestsMapper {
    public GreetingRequests fromDto(GreetingRequestsDto dto) {
        Objects.requireNonNull(dto, "dto");
        return new GreetingRequests(Objects.requireNonNull(dto.names(), "dto.names"));
    }

    public GreetingRequestsDto toDto(GreetingRequests domain) {
        return new GreetingRequestsDto(domain.names());
    }
}
