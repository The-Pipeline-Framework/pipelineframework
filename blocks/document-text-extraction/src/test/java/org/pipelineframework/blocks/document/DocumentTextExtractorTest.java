package org.pipelineframework.blocks.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DocumentTextExtractorTest {
    @TempDir Path directory;

    private final DocumentTextExtractor extractor = new DocumentTextExtractor();

    @Test void defaultsAllowTenMebibytesPerDocument() {
        assertEquals(10 * 1024 * 1024, DocumentTextExtractor.DEFAULT_MAX_INPUT_BYTES);
        assertEquals(10 * 1024 * 1024, DocumentTextExtractor.DEFAULT_MAX_EXTRACTED_CHARACTERS);
    }

    @Test void extractsStrictUtf8TextUsingContentType() throws Exception {
        Path file = directory.resolve("notes.txt");
        Files.writeString(file, "Jenny is 82.\r\nSecond line.");

        ExtractedText result = extractor.extract(request(file, "text/plain; charset=UTF-8"));

        assertEquals("Jenny is 82.\nSecond line.", result.text());
        assertEquals(DocumentFormat.PLAIN_TEXT, result.diagnostics().format());
        assertEquals(FormatSelection.CONTENT_TYPE, result.diagnostics().selectedBy());
        assertEquals(Files.size(file), result.diagnostics().inputBytes());
        assertEquals(result.text().length(), result.diagnostics().extractedCharacters());
        assertTrue(result.diagnostics().notes().isEmpty());
    }

    @Test void treatsMarkdownAsUtf8AndCanSelectItByExtension() throws Exception {
        Path file = directory.resolve("guide.md");
        Files.writeString(file, "# Guide\n\nJenny is **82**.");

        ExtractedText result = extractor.extract(request(file, DocumentTextExtractor.UNKNOWN_CONTENT_TYPE));

        assertEquals("# Guide\n\nJenny is **82**.", result.text());
        assertEquals(DocumentFormat.MARKDOWN, result.diagnostics().format());
        assertEquals(FormatSelection.EXTENSION, result.diagnostics().selectedBy());
        assertFalse(result.diagnostics().notes().isEmpty());
    }

    @Test void extractsTextFromSmallPdfFixture() throws Exception {
        Path file = directory.resolve("profile.pdf");
        try (PDDocument document = new PDDocument()) {
            PDPage page = new PDPage();
            document.addPage(page);
            try (PDPageContentStream content = new PDPageContentStream(document, page)) {
                content.beginText();
                content.setFont(new PDType1Font(Standard14Fonts.FontName.HELVETICA), 12);
                content.newLineAtOffset(72, 720);
                content.showText("Jenny is 82.");
                content.endText();
            }
            document.save(file.toFile());
        }

        ExtractedText result = extractor.extract(request(file, "application/pdf"));

        assertEquals("Jenny is 82.", result.text());
        assertEquals(DocumentFormat.PDF, result.diagnostics().format());
    }

    @Test void extractsParagraphsAndTablesFromSmallDocxFixture() throws Exception {
        Path file = directory.resolve("profile.docx");
        try (XWPFDocument document = new XWPFDocument()) {
            document.createParagraph().createRun().setText("Jenny is 82.");
            var table = document.createTable(1, 2);
            table.getRow(0).getCell(0).setText("City");
            table.getRow(0).getCell(1).setText("Madrid");
            try (var output = Files.newOutputStream(file)) {
                document.write(output);
            }
        }

        ExtractedText result = extractor.extract(request(file, "application/zip"));

        assertEquals("Jenny is 82.\nCity\tMadrid", result.text());
        assertEquals(DocumentFormat.DOCX, result.diagnostics().format());
        assertEquals(FormatSelection.EXTENSION, result.diagnostics().selectedBy());
    }

    @Test void appliesTheCharacterLimitAfterDiscardingTrailingDocumentSeparators() throws Exception {
        Path file = directory.resolve("exact-limit.docx");
        try (XWPFDocument document = new XWPFDocument()) {
            document.createParagraph().createRun().setText("12345");
            try (var output = Files.newOutputStream(file)) {
                document.write(output);
            }
        }

        ExtractedText result = new DocumentTextExtractor(64 * 1024, 5)
            .extract(request(file, "application/zip"));

        assertEquals("12345", result.text());
        assertEquals(5, result.diagnostics().extractedCharacters());
    }

    @Test void rejectsDocxExpansionBeforePoiConstructsTheDocument() throws Exception {
        Path file = directory.resolve("compressed-expansion.docx");
        try (XWPFDocument document = new XWPFDocument()) {
            document.createParagraph().createRun().setText("x".repeat(200_000));
            try (var output = Files.newOutputStream(file)) {
                document.write(output);
            }
        }
        int decompressedBudget = 64 * 1024;
        assertTrue(Files.size(file) < decompressedBudget);

        var failure = assertThrows(DocumentExtractionLimitException.class,
            () -> new DocumentTextExtractor(decompressedBudget, 300_000)
                .extract(request(file, "application/zip")));

        assertEquals("document exceeds the 65536 byte decompressed DOCX limit: compressed-expansion.docx",
            failure.getMessage());
    }

    @Test void rejectsDocxArchivesWithExcessiveEntryCountsBeforePoiParsing() throws Exception {
        Path file = directory.resolve("too-many-entries.docx");
        try (ZipOutputStream archive = new ZipOutputStream(Files.newOutputStream(file))) {
            for (int index = 0; index <= DocumentTextExtractor.MAX_DOCX_ARCHIVE_ENTRIES; index++) {
                archive.putNextEntry(new ZipEntry("entry-" + index));
                archive.closeEntry();
            }
        }

        var failure = assertThrows(DocumentExtractionLimitException.class,
            () -> new DocumentTextExtractor(4 * 1024 * 1024, 100)
                .extract(request(file, "application/zip")));

        assertEquals("document exceeds the 10000 entry DOCX archive limit: too-many-entries.docx",
            failure.getMessage());
    }

    @Test void rejectsPdfWithoutDeterministicallyExtractableText() throws Exception {
        Path file = directory.resolve("scan.pdf");
        try (PDDocument document = new PDDocument()) {
            document.addPage(new PDPage());
            document.save(file.toFile());
        }

        assertThrows(IllegalArgumentException.class,
            () -> extractor.extract(request(file, "application/pdf")));
    }

    @Test void rejectsMalformedUtf8() throws Exception {
        Path file = directory.resolve("broken.txt");
        Files.write(file, new byte[] {(byte) 0xc3, 0x28});
        assertThrows(IllegalArgumentException.class,
            () -> extractor.extract(request(file, "text/plain")));
    }

    @Test void rejectsInputAndExtractedTextBeyondConfiguredBounds() throws Exception {
        Path input = directory.resolve("large.txt");
        Files.writeString(input, "12345");
        assertThrows(DocumentExtractionLimitException.class,
            () -> new DocumentTextExtractor(4, 10).extract(request(input, "text/plain")));

        Path expansion = directory.resolve("expansion.txt");
        Files.writeString(expansion, "123456");
        assertThrows(DocumentExtractionLimitException.class,
            () -> new DocumentTextExtractor(10, 5).extract(request(expansion, "text/plain")));
    }

    @Test void rejectsConflictingAndUnsupportedFormatSignals() throws Exception {
        Path conflict = directory.resolve("document.pdf");
        Files.writeString(conflict, "not a PDF");
        assertThrows(IllegalArgumentException.class,
            () -> extractor.extract(request(conflict, "text/plain")));

        Path unsupported = directory.resolve("document.rtf");
        Files.writeString(unsupported, "unsupported");
        assertThrows(IllegalArgumentException.class,
            () -> extractor.extract(request(unsupported, DocumentTextExtractor.UNKNOWN_CONTENT_TYPE)));
    }

    private static DocumentExtractionRequest request(Path file, String contentType) {
        return new DocumentExtractionRequest(file, file.getFileName().toString(), contentType);
    }
}
