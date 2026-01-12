package org.pipelineframework.search.common.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import org.pipelineframework.search.common.domain.CrawlRequest;

public final class CrawlRequestOptions {

  private String fetchMethod;
  private String accept;
  private String acceptLanguage;
  private String authScope;
  private final Map<String, String> fetchHeaders = new LinkedHashMap<>();

  public static CrawlRequestOptions builder() {
    return new CrawlRequestOptions();
  }

  public CrawlRequestOptions fetchMethod(String fetchMethod) {
    this.fetchMethod = fetchMethod;
    return this;
  }

  public CrawlRequestOptions accept(String accept) {
    this.accept = accept;
    return this;
  }

  public CrawlRequestOptions acceptLanguage(String acceptLanguage) {
    this.acceptLanguage = acceptLanguage;
    return this;
  }

  public CrawlRequestOptions authScope(String authScope) {
    this.authScope = authScope;
    return this;
  }

  public CrawlRequestOptions header(String name, String value) {
    if (name != null && !name.isBlank() && value != null && !value.isBlank()) {
      fetchHeaders.put(name, value);
    }
    return this;
  }

  public CrawlRequestOptions headers(Map<String, String> headers) {
    if (headers == null || headers.isEmpty()) {
      return this;
    }
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      if (entry != null) {
        header(entry.getKey(), entry.getValue());
      }
    }
    return this;
  }

  public CrawlRequest applyTo(CrawlRequest request) {
    if (request == null) {
      return null;
    }
    request.fetchMethod = fetchMethod;
    request.accept = accept;
    request.acceptLanguage = acceptLanguage;
    request.authScope = authScope;
    request.fetchHeaders = fetchHeaders.isEmpty() ? null : new LinkedHashMap<>(fetchHeaders);
    return request;
  }

  public CrawlRequest build(String sourceUrl) {
    return build(null, sourceUrl);
  }

  public CrawlRequest build(UUID docId, String sourceUrl) {
    CrawlRequest request = new CrawlRequest();
    request.docId = docId;
    request.sourceUrl = sourceUrl;
    return applyTo(request);
  }
}
