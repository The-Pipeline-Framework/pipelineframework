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

package org.pipelineframework.csv.common.domain;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CsvPaymentsStableIdSupport {

  private CsvPaymentsStableIdSupport() {}

  static Optional<UUID> stableId(String namespace, Object... parts) {
    if (isBlank(namespace) || parts == null || parts.length == 0) {
      return Optional.empty();
    }
    List<String> normalized = Stream.of(parts).map(CsvPaymentsStableIdSupport::normalize).toList();
    if (normalized.stream().anyMatch(String::isBlank)) {
        return Optional.empty();
    }
    String normalizedParts = normalized.stream().collect(Collectors.joining("|"));
    return Optional.of(
        UUID.nameUUIDFromBytes(
            (namespace.strip() + "|" + normalizedParts).getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * Stable business identifier used before a payment record crosses the await boundary.
   */
  public static UUID paymentRecordId(
      Path inputFile, String csvId, String recipient, BigDecimal amount, Currency currency) {
    return stableId("PaymentRecord", inputFile, csvId, recipient, amount, currency)
        .orElseThrow(() -> new IllegalArgumentException("Payment record needs complete stable-id fields"));
  }

  private static String normalize(Object part) {
    if (part == null) {
      return "";
    }
    if (part instanceof BigDecimal decimal) {
      return decimal.stripTrailingZeros().toPlainString();
    }
    if (part instanceof Currency currency) {
      return currency.getCurrencyCode();
    }
    if (part instanceof Path path) {
      return path.normalize().toString();
    }
    return part.toString().strip();
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }
}
