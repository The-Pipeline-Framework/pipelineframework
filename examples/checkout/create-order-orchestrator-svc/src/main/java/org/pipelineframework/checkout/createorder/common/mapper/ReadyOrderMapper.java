package org.pipelineframework.checkout.createorder.common.mapper;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import org.pipelineframework.checkout.createorder.common.domain.ReadyOrder;
import org.pipelineframework.checkout.createorder.grpc.OrderReadySvc;
import org.pipelineframework.mapper.Mapper;

@ApplicationScoped
public class ReadyOrderMapper implements Mapper<OrderReadySvc.ReadyOrder, ReadyOrder, ReadyOrder> {
    /**
     * Converts a gRPC OrderReadySvc.ReadyOrder message into a domain ReadyOrder.
     *
     * @param grpc the gRPC ReadyOrder message to convert
     * @return a domain ReadyOrder with orderId, customerId, and readyAt populated from the gRPC message
     * @throws IllegalArgumentException if {@code grpc} is null
     */
    @Override
    public ReadyOrder fromGrpc(OrderReadySvc.ReadyOrder grpc) {
        if (grpc == null) {
            throw new IllegalArgumentException("grpc must not be null");
        }
        return new ReadyOrder(
            OrderRequestMapper.uuid(grpc.getOrderId(), "orderId"),
            OrderRequestMapper.uuid(grpc.getCustomerId(), "customerId"),
            instant(grpc.getReadyAt()));
    }

    /**
     * Convert a domain ReadyOrder to its gRPC OrderReadySvc.ReadyOrder representation.
     *
     * @param dto the domain ReadyOrder to convert
     * @return a gRPC OrderReadySvc.ReadyOrder with orderId, customerId, and readyAt populated
     * @throws IllegalArgumentException if {@code dto} is null
     */
    @Override
    public OrderReadySvc.ReadyOrder toGrpc(ReadyOrder dto) {
        if (dto == null) {
            throw new IllegalArgumentException("dto must not be null");
        }
        return OrderReadySvc.ReadyOrder.newBuilder()
            .setOrderId(OrderRequestMapper.str(dto.orderId()))
            .setCustomerId(OrderRequestMapper.str(dto.customerId()))
            .setReadyAt(str(dto.readyAt()))
            .build();
    }

    /**
     * Return the given DTO unchanged.
     *
     * @param dto the DTO instance to return
     * @return the same DTO instance that was passed in
     */
    @Override
    public ReadyOrder fromDto(ReadyOrder dto) {
        return dto;
    }

    /**
     * Produce the DTO representation of the provided ReadyOrder.
     *
     * @param domain the domain ReadyOrder to convert
     * @return the same ReadyOrder instance provided as input
     */
    @Override
    public ReadyOrder toDto(ReadyOrder domain) {
        return domain;
    }

    /**
     * Parse the 'readyAt' timestamp string into an Instant.
     *
     * @param value the timestamp string for the `readyAt` field
     * @return the corresponding {@link Instant}
     */
    private static Instant instant(String value) {
        return OrderRequestMapper.instant(value, "readyAt");
    }

    /**
     * Convert an Instant to its string representation used for order fields.
     *
     * @param value the Instant to convert
     * @return the Instant formatted as a String
     */
    private static String str(Instant value) {
        return OrderRequestMapper.str(value);
    }
}