package com.example.smoke;

import com.example.smoke.dto.PaymentRecordDto;
import com.example.smoke.dto.PaymentStatusDto;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.pipelineframework.runtime.spring.SpringPipelineRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GeneratedSpringRestSmokeTest {
    @Autowired
    SpringPipelineRunner pipelineRunner;

    @LocalServerPort
    int port;

    @Test
    void generatedRestEndpointRunsThroughSpringPipelineRunner() {
        assertEquals(2, pipelineRunner.stepCount());

        WebTestClient.bindToServer()
            .baseUrl("http://localhost:" + port)
            .responseTimeout(Duration.ofSeconds(5))
            .build()
            .post()
            .uri("/api/v1/payment-status/")
            .bodyValue(new PaymentRecordDto("pay-123", 42))
            .exchange()
            .expectStatus().isOk()
            .expectBody(PaymentStatusDto.class)
            .value(response -> {
                assertEquals("pay-123", response.paymentId());
                assertEquals("APPROVED:42:DELEGATED", response.status());
            });
    }
}
