package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;

class QueryCapturePayloadCodecTest {
    private final QueryCapturePayloadCodec codec = new QueryCapturePayloadCodec(PipelineJson.mapper());

    @Test
    void roundTripsGeneratedLikeSealedUnionByCommittedCaseAndDiscriminator() {
        Decision original = new Decision.Call(new CallPayload("payments", "charge.create"));

        String captured = codec.encode(original, Decision.class);
        Decision decoded = codec.decode(captured, Decision.class);

        Decision.Call call = assertInstanceOf(Decision.Call.class, decoded);
        assertEquals(original, call);
        assertEquals("call", call.discriminator());
    }

    sealed interface Decision permits Decision.Call, Decision.Complete {
        String discriminator();

        record Call(CallPayload value) implements Decision {
            @Override
            public String discriminator() {
                return "call";
            }
        }

        record Complete(CompletePayload value) implements Decision {
            @Override
            public String discriminator() {
                return "complete";
            }
        }
    }

    record CallPayload(String binding, String operation) {
    }

    record CompletePayload(String status) {
    }
}
