package org.pipelineframework.search.crawl_source.service;

import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.pipelineframework.search.common.domain.CrawlRequest;

public final class FetchOptionsNormalizer {

  private FetchOptionsNormalizer() {
  }

  public static String normalize(CrawlRequest request) {
    if (request == null) {
      return null;
    }

    StringJoiner joiner = new StringJoiner("|");
    add(joiner, "method", normalizeMethod(request.fetchMethod));
    add(joiner, "accept", normalizeToken(request.accept));
    add(joiner, "acceptLanguage", normalizeToken(request.acceptLanguage));
    add(joiner, "authScope", normalizeToken(request.authScope));

    String headers = normalizeHeaders(request.fetchHeaders);
    add(joiner, "headers", headers);

    String normalized = joiner.toString();
    return normalized.isBlank() ? null : normalized;
  }

  private static String normalizeMethod(String method) {
    String token = normalizeToken(method);
    return token == null ? null : token.toUpperCase(Locale.ROOT);
  }

  private static String normalizeHeaders(Map<String, String> headers) {
    if (headers == null || headers.isEmpty()) {
      return null;
    }
    Map<String, String> normalized = new TreeMap<>();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry == null) {
        continue;
      }
      String key = normalizeToken(entry.getKey());
      String value = normalizeToken(entry.getValue());
      if (key != null && value != null) {
        normalized.put(key.toLowerCase(Locale.ROOT), value);
      }
    }
    if (normalized.isEmpty()) {
      return null;
    }
    StringJoiner joiner = new StringJoiner(",");
    for (Map.Entry<String, String> entry : normalized.entrySet()) {
      joiner.add(entry.getKey() + "=" + entry.getValue());
    }
    return joiner.toString();
  }

  private static String normalizeToken(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }

  private static void add(StringJoiner joiner, String key, String value) {
    if (value == null || value.isBlank()) {
      return;
    }
    joiner.add(key + "=" + value);
  }
}
