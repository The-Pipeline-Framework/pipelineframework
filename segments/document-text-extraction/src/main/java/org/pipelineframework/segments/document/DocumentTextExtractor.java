package org.pipelineframework.segments.document;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipInputStream;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.xwpf.usermodel.IBodyElement;
import org.apache.poi.xwpf.usermodel.XWPFDocument;
import org.apache.poi.xwpf.usermodel.XWPFParagraph;
import org.apache.poi.xwpf.usermodel.XWPFTable;
import org.apache.poi.xwpf.usermodel.XWPFTableCell;
import org.apache.poi.xwpf.usermodel.XWPFTableRow;

/** Bounded deterministic text extraction for the formats admitted by the turnkey RAG indexer. */
public final class DocumentTextExtractor {
    public static final int DEFAULT_MAX_INPUT_BYTES = 10 * 1024 * 1024;
    public static final int DEFAULT_MAX_EXTRACTED_CHARACTERS = 10 * 1024 * 1024;
    public static final int MAX_DOCX_ARCHIVE_ENTRIES = 10_000;
    public static final String UNKNOWN_CONTENT_TYPE = "application/octet-stream";

    private static final String DOCX_CONTENT_TYPE =
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    private static final Map<String, DocumentFormat> CONTENT_TYPES = Map.of(
        "text/plain", DocumentFormat.PLAIN_TEXT,
        "text/markdown", DocumentFormat.MARKDOWN,
        "text/x-markdown", DocumentFormat.MARKDOWN,
        "application/pdf", DocumentFormat.PDF,
        DOCX_CONTENT_TYPE, DocumentFormat.DOCX);
    private static final Map<String, DocumentFormat> EXTENSIONS = Map.of(
        "txt", DocumentFormat.PLAIN_TEXT,
        "md", DocumentFormat.MARKDOWN,
        "pdf", DocumentFormat.PDF,
        "docx", DocumentFormat.DOCX);

    private final int maxInputBytes;
    private final int maxExtractedCharacters;

    public DocumentTextExtractor() {
        this(DEFAULT_MAX_INPUT_BYTES, DEFAULT_MAX_EXTRACTED_CHARACTERS);
    }

    public DocumentTextExtractor(int maxInputBytes, int maxExtractedCharacters) {
        if (maxInputBytes < 1) throw new IllegalArgumentException("max input bytes must be positive");
        if (maxInputBytes == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("max input bytes must leave room for limit detection");
        }
        if (maxExtractedCharacters < 1) {
            throw new IllegalArgumentException("max extracted characters must be positive");
        }
        this.maxInputBytes = maxInputBytes;
        this.maxExtractedCharacters = maxExtractedCharacters;
    }

    public ExtractedText extract(DocumentExtractionRequest request) {
        Objects.requireNonNull(request, "document extraction request must not be null");
        FormatDecision decision = selectFormat(request.fileName(), request.contentType());
        byte[] bytes = readBounded(request);
        String text = switch (decision.format()) {
            case PLAIN_TEXT, MARKDOWN -> decodeUtf8(bytes, request.fileName());
            case PDF -> extractPdf(bytes, request.fileName());
            case DOCX -> extractDocx(bytes, request.fileName());
        };
        String normalized = normalize(text);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("document contains no extractable text: " + request.fileName());
        }
        if (normalized.length() > maxExtractedCharacters) {
            throw extractedTextLimit(request.fileName());
        }
        return new ExtractedText(normalized, new DocumentExtractionDiagnostics(
            decision.format(), decision.selectedBy(), decision.contentType(), bytes.length,
            normalized.length(), decision.notes()));
    }

    private FormatDecision selectFormat(String fileName, String declaredContentType) {
        String contentType = normalizeContentType(declaredContentType);
        Optional<DocumentFormat> byContentType = Optional.ofNullable(CONTENT_TYPES.get(contentType));
        Optional<DocumentFormat> byExtension = extension(fileName).map(EXTENSIONS::get);

        if (byContentType.isPresent() && byExtension.isPresent()
                && !compatible(byContentType.orElseThrow(), byExtension.orElseThrow())) {
            throw new IllegalArgumentException("document content type '" + contentType
                + "' conflicts with file extension: " + fileName);
        }
        if (byContentType.isPresent()) {
            DocumentFormat format = byContentType.orElseThrow();
            if (byExtension.filter(candidate -> candidate == DocumentFormat.MARKDOWN).isPresent()
                    && format == DocumentFormat.PLAIN_TEXT) {
                return new FormatDecision(DocumentFormat.MARKDOWN, FormatSelection.EXTENSION, contentType,
                    List.of("content type text/plain is compatible with Markdown; selected Markdown by extension"));
            }
            return new FormatDecision(format, FormatSelection.CONTENT_TYPE, contentType, List.of());
        }
        if (byExtension.isPresent()) {
            return new FormatDecision(byExtension.orElseThrow(), FormatSelection.EXTENSION, contentType,
                List.of("unrecognized content type; selected format by file extension"));
        }
        throw new IllegalArgumentException("unsupported document content type and extension: "
            + contentType + ", " + fileName);
    }

    private byte[] readBounded(DocumentExtractionRequest request) {
        try {
            long declaredSize = Files.size(request.content());
            if (declaredSize > maxInputBytes) throw inputLimit(request.fileName());
            try (InputStream input = Files.newInputStream(request.content())) {
                byte[] bytes = input.readNBytes(maxInputBytes + 1);
                if (bytes.length > maxInputBytes) throw inputLimit(request.fileName());
                return bytes;
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException("could not read document: " + request.fileName(), failure);
        }
    }

    private String decodeUtf8(byte[] bytes, String fileName) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException failure) {
            throw new IllegalArgumentException("document is not valid UTF-8: " + fileName, failure);
        }
    }

    private String extractPdf(byte[] bytes, String fileName) {
        var streamCache = MemoryUsageSetting.setupMainMemoryOnly(maxInputBytes).streamCache;
        try (PDDocument document = Loader.loadPDF(bytes, "", null, null, streamCache)) {
            PDFTextStripper stripper = new PDFTextStripper();
            stripper.setLineSeparator("\n");
            stripper.setPageEnd("\n");
            BoundedTextWriter output = new BoundedTextWriter(maxExtractedCharacters, fileName);
            stripper.writeText(document, output);
            return output.toString();
        } catch (TextLimitIOException failure) {
            throw extractedTextLimit(fileName);
        } catch (IOException failure) {
            throw new IllegalArgumentException("could not extract PDF text: " + fileName, failure);
        }
    }

    private String extractDocx(byte[] bytes, String fileName) {
        validateDocxExpansion(bytes, fileName);
        try (XWPFDocument document = new XWPFDocument(new ByteArrayInputStream(bytes))) {
            BoundedText output = new BoundedText(maxExtractedCharacters, fileName);
            appendBody(document.getBodyElements(), output);
            return output.toString();
        } catch (IOException failure) {
            throw new IllegalArgumentException("could not extract DOCX text: " + fileName, failure);
        }
    }

    private void validateDocxExpansion(byte[] bytes, String fileName) {
        long archiveBytes = 0;
        int archiveEntries = 0;
        byte[] buffer = new byte[8192];
        try (ZipInputStream archive = new ZipInputStream(new ByteArrayInputStream(bytes))) {
            while (archive.getNextEntry() != null) {
                archiveEntries++;
                if (archiveEntries > MAX_DOCX_ARCHIVE_ENTRIES) {
                    throw new DocumentExtractionLimitException("document exceeds the "
                        + MAX_DOCX_ARCHIVE_ENTRIES + " entry DOCX archive limit: " + fileName);
                }
                long entryBytes = 0;
                int read;
                while ((read = archive.read(buffer)) != -1) {
                    entryBytes += read;
                    archiveBytes += read;
                    if (entryBytes > maxInputBytes || archiveBytes > maxInputBytes) {
                        throw decompressedDocxLimit(fileName);
                    }
                }
            }
        } catch (IOException failure) {
            throw new IllegalArgumentException("could not inspect DOCX entries: " + fileName, failure);
        }
    }

    private void appendBody(List<IBodyElement> elements, BoundedText output) {
        for (IBodyElement element : elements) {
            if (element instanceof XWPFParagraph paragraph) {
                output.line(paragraph.getText());
            } else if (element instanceof XWPFTable table) {
                appendTable(table, output);
            }
        }
    }

    private void appendTable(XWPFTable table, BoundedText output) {
        for (XWPFTableRow row : table.getRows()) {
            boolean first = true;
            for (XWPFTableCell cell : row.getTableCells()) {
                if (!first) output.append("\t");
                output.append(cell.getText());
                first = false;
            }
            output.append("\n");
        }
    }

    private static String normalize(String text) {
        return text.replace("\r\n", "\n").replace('\r', '\n').strip();
    }

    private static String normalizeContentType(String value) {
        int parameter = value.indexOf(';');
        String mediaType = parameter < 0 ? value : value.substring(0, parameter);
        return mediaType.strip().toLowerCase(Locale.ROOT);
    }

    private static Optional<String> extension(String fileName) {
        int slash = Math.max(fileName.lastIndexOf('/'), fileName.lastIndexOf('\\'));
        int dot = fileName.lastIndexOf('.');
        if (dot <= slash || dot == fileName.length() - 1) return Optional.empty();
        return Optional.of(fileName.substring(dot + 1).toLowerCase(Locale.ROOT));
    }

    private static boolean compatible(DocumentFormat contentType, DocumentFormat extension) {
        if (contentType == extension) return true;
        return (contentType == DocumentFormat.PLAIN_TEXT || contentType == DocumentFormat.MARKDOWN)
            && (extension == DocumentFormat.PLAIN_TEXT || extension == DocumentFormat.MARKDOWN);
    }

    private DocumentExtractionLimitException inputLimit(String fileName) {
        return new DocumentExtractionLimitException("document exceeds the " + maxInputBytes
            + " byte extraction limit: " + fileName);
    }

    private DocumentExtractionLimitException extractedTextLimit(String fileName) {
        return new DocumentExtractionLimitException("document exceeds the " + maxExtractedCharacters
            + " character extraction limit: " + fileName);
    }

    private DocumentExtractionLimitException decompressedDocxLimit(String fileName) {
        return new DocumentExtractionLimitException("document exceeds the " + maxInputBytes
            + " byte decompressed DOCX limit: " + fileName);
    }

    private record FormatDecision(DocumentFormat format, FormatSelection selectedBy, String contentType,
                                  List<String> notes) {
        private FormatDecision {
            notes = List.copyOf(notes);
        }
    }

    private static final class BoundedText {
        private final BoundedNormalizedText text;

        private BoundedText(int limit, String fileName) {
            this.text = new BoundedNormalizedText(limit, fileName);
        }

        private void line(String value) {
            append(value);
            append("\n");
        }

        private void append(String value) {
            Objects.requireNonNull(value, "extracted document text segment must not be null");
            text.append(value);
        }

        @Override public String toString() {
            return text.toString();
        }
    }

    private static final class BoundedTextWriter extends Writer {
        private final BoundedNormalizedText text;

        private BoundedTextWriter(int limit, String fileName) {
            this.text = new BoundedNormalizedText(limit, fileName);
        }

        @Override public void write(char[] characters, int offset, int length) throws IOException {
            try {
                text.append(characters, offset, length);
            } catch (DocumentExtractionLimitException failure) {
                throw new TextLimitIOException(failure.getMessage());
            }
        }

        @Override public void flush() { }

        @Override public void close() { }

        @Override public String toString() {
            return text.toString();
        }
    }

    /** Accumulates already-normalized text while excluding leading and trailing whitespace from the limit. */
    private static final class BoundedNormalizedText {
        private final StringBuilder text = new StringBuilder();
        private final StringBuilder pendingWhitespace = new StringBuilder();
        private final int limit;
        private final String fileName;
        private long pendingWhitespaceLength;
        private boolean previousCarriageReturn;

        private BoundedNormalizedText(int limit, String fileName) {
            this.limit = limit;
            this.fileName = fileName;
        }

        private void append(String value) {
            append(value.toCharArray(), 0, value.length());
        }

        private void append(char[] characters, int offset, int length) {
            for (int index = offset; index < offset + length; index++) {
                char character = characters[index];
                if (previousCarriageReturn && character == '\n') {
                    previousCarriageReturn = false;
                    continue;
                }
                previousCarriageReturn = character == '\r';
                accept(character == '\r' ? '\n' : character);
            }
        }

        private void accept(char character) {
            if (Character.isWhitespace(character)) {
                if (!text.isEmpty()) {
                    pendingWhitespaceLength++;
                    if ((long) text.length() + pendingWhitespace.length() < limit) {
                        pendingWhitespace.append(character);
                    }
                }
                return;
            }
            if ((long) text.length() + pendingWhitespaceLength + 1 > limit) {
                throw new DocumentExtractionLimitException("document exceeds the " + limit
                    + " character extraction limit: " + fileName);
            }
            text.append(pendingWhitespace).append(character);
            pendingWhitespace.setLength(0);
            pendingWhitespaceLength = 0;
        }

        @Override public String toString() {
            return text.toString();
        }
    }

    private static final class TextLimitIOException extends IOException {
        private TextLimitIOException(String message) {
            super(message);
        }
    }
}
