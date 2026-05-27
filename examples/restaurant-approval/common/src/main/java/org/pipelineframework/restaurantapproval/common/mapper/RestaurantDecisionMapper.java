package org.pipelineframework.restaurantapproval.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.UUID;
import org.pipelineframework.mapper.Mapper;
import org.pipelineframework.restaurantapproval.common.domain.RestaurantDecision;
import org.pipelineframework.restaurantapproval.common.domain.RestaurantDecisionVisitor;
import org.pipelineframework.restaurantapproval.common.domain.RestaurantOrderAccepted;
import org.pipelineframework.restaurantapproval.common.domain.RestaurantOrderDeclined;
import org.pipelineframework.restaurantapproval.common.dto.RestaurantDecisionDto;
import org.pipelineframework.restaurantapproval.common.dto.RestaurantOrderAcceptedDto;
import org.pipelineframework.restaurantapproval.common.dto.RestaurantOrderDeclinedDto;
import org.pipelineframework.restaurantapproval.grpc.PipelineTypes;

@ApplicationScoped
public class RestaurantDecisionMapper
    implements Mapper<RestaurantDecision, RestaurantDecisionDto> {

  @Override
  public RestaurantDecision fromExternal(RestaurantDecisionDto external) {
    return fromDto(external);
  }

  @Override
  public RestaurantDecisionDto toExternal(RestaurantDecision domain) {
    return toDto(domain);
  }

  public RestaurantDecision fromDto(RestaurantDecisionDto dto) {
    if (dto instanceof RestaurantOrderAcceptedDto acceptedDto) {
      return new RestaurantOrderAccepted(
          acceptedDto.orderId(),
          acceptedDto.decidedAt(),
          acceptedDto.note());
    }
    if (dto instanceof RestaurantOrderDeclinedDto declinedDto) {
      return new RestaurantOrderDeclined(
          declinedDto.orderId(),
          declinedDto.decidedAt(),
          declinedDto.note(),
          declinedDto.declineReason());
    }
    throw new IllegalArgumentException("Unsupported RestaurantDecisionDto variant: " + dto);
  }

  public RestaurantDecisionDto toDto(RestaurantDecision domain) {
    return domain.accept(new RestaurantDecisionVisitor<>() {
      @Override
      public RestaurantDecisionDto accepted(RestaurantOrderAccepted accepted) {
        return new RestaurantOrderAcceptedDto(
            accepted.orderId(),
            accepted.decidedAt(),
            accepted.note());
      }

      @Override
      public RestaurantDecisionDto declined(RestaurantOrderDeclined declined) {
        return new RestaurantOrderDeclinedDto(
            declined.orderId(),
            declined.decidedAt(),
            declined.note(),
            declined.declineReason());
      }
    });
  }

  public RestaurantDecision fromGrpc(PipelineTypes.RestaurantDecision external) {
    return switch (external.getOutcomeCase()) {
      case ACCEPTED -> new RestaurantOrderAccepted(
          uuid(external.getAccepted().getOrderId()),
          instant(external.getAccepted().getDecidedAt()),
          external.getAccepted().getNote());
      case DECLINED -> new RestaurantOrderDeclined(
          uuid(external.getDeclined().getOrderId()),
          instant(external.getDeclined().getDecidedAt()),
          external.getDeclined().getNote(),
          external.getDeclined().getDeclineReason());
      case OUTCOME_NOT_SET -> throw new IllegalArgumentException(
          "RestaurantDecision has no selected variant");
    };
  }

  public PipelineTypes.RestaurantDecision toGrpc(RestaurantDecision domain) {
    PipelineTypes.RestaurantDecision.Builder builder = PipelineTypes.RestaurantDecision.newBuilder();
    return domain.accept(new RestaurantDecisionVisitor<>() {
      @Override
      public PipelineTypes.RestaurantDecision accepted(RestaurantOrderAccepted accepted) {
        return builder.setAccepted(PipelineTypes.RestaurantOrderAccepted.newBuilder()
            .setOrderId(str(accepted.orderId()))
            .setDecidedAt(str(accepted.decidedAt()))
            .setNote(nullToEmpty(accepted.note()))
            .build()).build();
      }

      @Override
      public PipelineTypes.RestaurantDecision declined(RestaurantOrderDeclined declined) {
        return builder.setDeclined(PipelineTypes.RestaurantOrderDeclined.newBuilder()
            .setOrderId(str(declined.orderId()))
            .setDecidedAt(str(declined.decidedAt()))
            .setNote(nullToEmpty(declined.note()))
            .setDeclineReason(nullToEmpty(declined.declineReason()))
            .build()).build();
      }
    });
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  public PipelineTypes.RestaurantDecision toDtoToGrpc(RestaurantDecision domain) {
    return toGrpc(domain);
  }

  @Deprecated(since = "26.5.2", forRemoval = true)
  public RestaurantDecision fromGrpcFromDto(PipelineTypes.RestaurantDecision grpc) {
    return fromGrpc(grpc);
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private static String str(Object value) {
    return value == null ? "" : String.valueOf(value);
  }

  private static UUID uuid(String value) {
    return value == null || value.isBlank() ? null : UUID.fromString(value);
  }

  private static Instant instant(String value) {
    return value == null || value.isBlank() ? null : Instant.parse(value);
  }
}
