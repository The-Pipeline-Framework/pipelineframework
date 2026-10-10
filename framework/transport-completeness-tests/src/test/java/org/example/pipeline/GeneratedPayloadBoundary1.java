package org.example.pipeline;

import io.quarkus.security.Authenticated;
import io.smallrye.common.annotation.Blocking;
import io.vertx.core.http.HttpServerResponse;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.StreamingOutput;
import java.lang.IllegalArgumentException;
import java.lang.IllegalStateException;
import java.lang.SecurityException;
import java.lang.String;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.pipelineframework.config.boundary.PipelineHttpPayloadBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.OwnedPayloadTransfer;
import org.pipelineframework.connector.PayloadBoundaryAuthorizer;
import org.pipelineframework.connector.PayloadBoundaryRuntimeConfig;
import org.pipelineframework.repository.PayloadReference;

@Path("/tpf/payloads/receipt/download")
@Authenticated
@Blocking
public class GeneratedPayloadBoundary1 {
  private static final PipelineHttpPayloadBoundaryConfig BOUNDARY = new PipelineHttpPayloadBoundaryConfig("receipt", PipelineHttpPayloadBoundaryConfig.Direction.DOWNLOAD, "receipts", "ReceiptOutput", "payload_ref", java.util.List.of("application/pdf"), 0L, "receipt.read");

  private static final PipelineYamlConfig CONFIG = PayloadBoundaryRuntimeConfig.load();

  @Inject
  private ConnectorBindingRegistry bindings;

  @Inject
  private ConnectorRuntimeContext runtimeContext;

  @Inject
  private Instance<PayloadBoundaryAuthorizer> authorizers;

  private OwnedPayloadTransfer transfer() {
    if (authorizers.isUnsatisfied() || authorizers.isAmbiguous()) throw new IllegalStateException("exactly one payload boundary authorizer is required");
    return new OwnedPayloadTransfer(bindings, runtimeContext, authorizers.get());
  }

  private static String principal(SecurityContext security) {
    if (security == null || security.getUserPrincipal() == null) throw new WebApplicationException(401);
    return security.getUserPrincipal().getName();
  }

  @POST
  @Consumes("application/json")
  @Operation(
      operationId = "downloadPayloadreceipt"
  )
  public Response download(PayloadReference reference, @HeaderParam("X-Tenant-Id") String tenant,
      @HeaderParam("X-Scope-Id") String scope, @HeaderParam("Range") String range,
      @Context SecurityContext security, @Context HttpServerResponse httpResponse) {
    if (range != null) throw new WebApplicationException(416);
    var source = CONFIG.sources().get(BOUNDARY.objectName());
    if (source == null) throw new IllegalStateException("payload source is unavailable");
    var engine = transfer();
    var principal = principal(security);
    OwnedPayloadTransfer.DownloadLease lease;
    try {
      lease = engine.openDownload(BOUNDARY, source, principal, tenant, scope, reference);
    } catch (SecurityException denied) {
      throw new ForbiddenException();
    } catch (IllegalArgumentException invalid) {
      throw new BadRequestException();
    }
    httpResponse.closeHandler(ignored -> lease.close());
    StreamingOutput stream = output -> { try (lease) { lease.writeTo(output); } };
    var response = Response.ok(stream, reference.contentType()).header("Content-Length", reference.sizeBytes()).header("Cache-Control", "private, no-store").header("Content-Disposition", "attachment; filename=\"payload\"").header("Accept-Ranges", "none");
    if (reference.checksum() != null) response.header("ETag", "\"" + reference.checksum() + "\"");
    return response.build();
  }
}
