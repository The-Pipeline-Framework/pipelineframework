package org.pipelineframework.search.gcp;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import java.io.IOException;

final class NoopGoogleCloudFunction implements HttpFunction {
    @Override
    public void service(HttpRequest request, HttpResponse response) throws IOException {
        response.setStatusCode(204);
    }
}
