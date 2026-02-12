package org.pipelineframework.search.resource;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class NoopLambdaHandler implements RequestHandler<Object, Object> {
  @Override
  public Object handleRequest(Object input, Context context) {
    return input;
  }
}
