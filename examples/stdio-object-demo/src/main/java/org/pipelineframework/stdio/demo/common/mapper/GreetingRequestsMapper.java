package org.pipelineframework.stdio.demo.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.stdio.demo.common.domain.GreetingRequests;
import org.pipelineframework.stdio.demo.common.dto.GreetingRequestsDto;

@ApplicationScoped
public class GreetingRequestsMapper {
    public GreetingRequests fromDto(GreetingRequestsDto dto) {
        return new GreetingRequests(dto.names());
    }

    public GreetingRequestsDto toDto(GreetingRequests domain) {
        return new GreetingRequestsDto(domain.names());
    }
}
