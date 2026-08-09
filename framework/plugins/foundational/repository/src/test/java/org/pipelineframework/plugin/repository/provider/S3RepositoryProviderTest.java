package org.pipelineframework.plugin.repository.provider;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3RepositoryProviderTest {

    @Test
    void distinguishesMissingObjectFromMissingBucket() {
        S3Exception unqualifiedNotFound = exception(null);
        S3Exception missingObject = exception("NoSuchKey");
        S3Exception missingBucket = exception("NoSuchBucket");

        assertTrue(S3RepositoryProvider.isMissingObject(unqualifiedNotFound));
        assertTrue(S3RepositoryProvider.isMissingObject(missingObject));
        assertFalse(S3RepositoryProvider.isMissingObject(missingBucket));
    }

    private S3Exception exception(String errorCode) {
        S3Exception exception = mock(S3Exception.class);
        when(exception.statusCode()).thenReturn(404);
        if (errorCode != null) {
            AwsErrorDetails details = mock(AwsErrorDetails.class);
            when(details.errorCode()).thenReturn(errorCode);
            when(exception.awsErrorDetails()).thenReturn(details);
        }
        return exception;
    }
}
