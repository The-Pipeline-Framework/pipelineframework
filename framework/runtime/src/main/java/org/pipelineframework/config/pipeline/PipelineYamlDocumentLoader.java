/*
 * Copyright (c) 2023-2025 Mariano Barcia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.pipelineframework.config.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * Loads a {@code pipeline.yaml} document through the framework's shared secure YAML grammar.
 */
public final class PipelineYamlDocumentLoader {
    private static final int MAX_CODE_POINTS = 3_000_000;
    private static final Pattern V3_DOCUMENT = Pattern.compile(
        "(?m)^\\s*version:\\s*(?:3|\"3\"|'3')\\s*(?:#.*)?$");
    private static final Pattern V3_COMPACT_OPTIONAL_NAME = Pattern.compile(
        "(?m)(\\[\\s*)([A-Za-z_][A-Za-z0-9_]*\\?)(\\s*,)");
    private static final Pattern V3_COMPACT_NULLABLE_TYPE = Pattern.compile(
        "(?m)(,\\s*)([A-Za-z_][A-Za-z0-9_.:-]*\\?)(\\s*\\])");
    private static final Pattern YAML_TYPES_BLOCK = Pattern.compile("^types\\s*:\\s*(?:#.*)?$");
    private static final Pattern YAML_FIELDS_BLOCK = Pattern.compile("^fields\\s*:(.*)$");
    private static final Pattern YAML_COMPACT_FIELD_ITEM = Pattern.compile("^-\\s*\\[");

    /** Load a pipeline YAML document from a UTF-8 file. */
    public Object load(Path configPath) {
        try (Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            return parse(readSource(reader));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config: " + configPath, e);
        }
    }

    /** Load a pipeline YAML document from a UTF-8 input stream, closing the stream after reading. */
    public Object load(InputStream inputStream) {
        try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            return parse(readSource(reader));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config from input stream", e);
        }
    }

    /** Load a pipeline YAML document from a reader without closing the caller-owned reader. */
    public Object load(Reader reader) {
        try {
            return parse(readSource(reader));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read pipeline config from reader", e);
        }
    }

    private Object parse(String source) {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(MAX_CODE_POINTS);
        loaderOptions.setMaxAliasesForCollections(50);
        loaderOptions.setAllowDuplicateKeys(false);
        return new Yaml(new SafeConstructor(loaderOptions)).load(normalizeV3QuestionMarkers(source));
    }

    private String readSource(Reader reader) throws IOException {
        StringBuilder source = new StringBuilder();
        char[] buffer = new char[8_192];
        int codePoints = 0;
        int read;
        while ((read = reader.read(buffer)) >= 0) {
            int addedCodePoints = Character.codePointCount(buffer, 0, read);
            if (!source.isEmpty()
                && read > 0
                && Character.isHighSurrogate(source.charAt(source.length() - 1))
                && Character.isLowSurrogate(buffer[0])) {
                addedCodePoints--;
            }
            if (codePoints + addedCodePoints > MAX_CODE_POINTS) {
                throw new YAMLException("Pipeline YAML exceeds the " + MAX_CODE_POINTS + " character limit");
            }
            source.append(buffer, 0, read);
            codePoints += addedCodePoints;
        }
        return source.toString();
    }

    private String normalizeV3QuestionMarkers(String source) {
        if (!V3_DOCUMENT.matcher(source).find()) {
            return source;
        }
        StringBuilder normalized = new StringBuilder(source.length());
        int typesIndent = -1;
        int typeIndent = -1;
        int memberIndent = -1;
        int fieldsIndent = -1;
        for (String line : source.split("(?<=\\n)", -1)) {
            String content = line.stripLeading();
            String structural = content.stripTrailing();
            if (structural.isBlank() || structural.startsWith("#")) {
                normalized.append(line);
                continue;
            }
            int indent = line.length() - content.length();
            if (typesIndent < 0) {
                if (indent == 0 && YAML_TYPES_BLOCK.matcher(structural).matches()) {
                    typesIndent = indent;
                }
                normalized.append(line);
                continue;
            }
            if (indent <= typesIndent) {
                typesIndent = -1;
                typeIndent = -1;
                memberIndent = -1;
                fieldsIndent = -1;
                normalized.append(line);
                continue;
            }
            if (typeIndent < 0 || indent <= typeIndent) {
                typeIndent = indent;
                memberIndent = -1;
                fieldsIndent = -1;
                normalized.append(line);
                continue;
            }
            if (fieldsIndent >= 0 && indent > fieldsIndent) {
                normalized.append(YAML_COMPACT_FIELD_ITEM.matcher(structural).find()
                    ? normalizeV3FieldTuple(line)
                    : line);
                continue;
            }
            fieldsIndent = -1;
            if (memberIndent < 0 || indent < memberIndent) {
                memberIndent = indent;
            }
            java.util.regex.Matcher fields = YAML_FIELDS_BLOCK.matcher(structural);
            if (indent == memberIndent && fields.matches()) {
                fieldsIndent = indent;
                normalized.append(fields.group(1).stripLeading().startsWith("[")
                    ? normalizeV3FieldTuple(line)
                    : line);
            } else {
                normalized.append(line);
            }
        }
        return normalized.toString();
    }

    private String normalizeV3FieldTuple(String source) {
        String namesQuoted = V3_COMPACT_OPTIONAL_NAME.matcher(source).replaceAll("$1\"$2\"$3");
        return V3_COMPACT_NULLABLE_TYPE.matcher(namesQuoted).replaceAll("$1\"$2\"$3");
    }
}
