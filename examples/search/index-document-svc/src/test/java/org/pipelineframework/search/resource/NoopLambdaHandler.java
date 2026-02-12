package org.pipelineframework.search.resource;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import jakarta.inject.Named;

@Named("org.pipelineframework.search.resource.NoopLambdaHandler")
public class NoopLambdaHandler implements RequestHandler<Object, Object> {
  @Override
  public Object handleRequest(Object input, Context context) {
    return input;
  }
}
