package org.pipelineframework.stdio.demo.mapper;

import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.stdio.demo.common.domain.GreetingResponse;

/** Local transport does not change the terminal response representation. */
public final class GreetingResponseTerminalMapper implements Mapper<GreetingResponse, GreetingResponse> {
    @Override
    public GreetingResponse fromExternal(GreetingResponse external) {
        return external;
    }

    @Override
    public GreetingResponse toExternal(GreetingResponse domain) {
        return domain;
    }
}
