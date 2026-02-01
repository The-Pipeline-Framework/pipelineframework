package org.pipelineframework.search.common.util;

import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.pipelineframework.search.common.domain.CrawlRequest;
import org.pipelineframework.search.common.dto.CrawlRequestDto;

public final class FetchOptionsNormalizer {

  private FetchOptionsNormalizer() {
  }

  public static String normalize(CrawlRequest request) {
    if (request == null) {
      return null;
    }
    return normalize(
        request.fetchMethod,
        request.accept,
        request.acceptLanguage,
        request.authScope,
        request.fetchHeaders);
  }

  public static String normalize(CrawlRequestDto request) {
    if (request == null) {
      return null;
    }
    return normalize(
        request.getFetchMethod(),
        request.getAccept(),
        request.getAcceptLanguage(),
        request.getAuthScope(),
        request.getFetchHeaders());
  }

  public static String normalize(
      String fetchMethod,
      String accept,
      String acceptLanguage,
      String authScope,
      Map<String, String> fetchHeaders) {
    StringJoiner joiner = new StringJoiner("|");
    add(joiner, "method", normalizeMethod(fetchMethod));
    add(joiner, "accept", normalizeToken(accept));
    add(joiner, "acceptLanguage", normalizeToken(acceptLanguage));
    add(joiner, "authScope", normalizeToken(authScope));

    String headers = normalizeHeaders(fetchHeaders);
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
