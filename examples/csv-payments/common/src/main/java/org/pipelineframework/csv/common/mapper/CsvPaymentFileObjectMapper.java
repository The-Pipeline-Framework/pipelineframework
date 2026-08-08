package org.pipelineframework.csv.common.mapper;

import java.nio.file.Path;

import org.pipelineframework.csv.domain.CsvPaymentsInputFile;
import org.pipelineframework.objectingest.ObjectSnapshot;
import org.pipelineframework.objectingest.ObjectSnapshotMapper;

/**
 * Projects a framework object snapshot into the CSV Payments first business input.
 */
public final class CsvPaymentFileObjectMapper implements ObjectSnapshotMapper<CsvPaymentsInputFile> {

    @Override
    public CsvPaymentsInputFile map(ObjectSnapshot snapshot) {
        String localPath = snapshot.localPath();
        if (localPath == null || localPath.isBlank()) {
            throw new IllegalArgumentException("CSV filesystem object snapshot must expose localPath");
        }
        Path file = Path.of(localPath);
        return new CsvPaymentsInputFile(file, file.getParent());
    }
}
