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

package org.pipelineframework.csv.common.mapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Currency;
import java.util.UUID;
import org.mapstruct.Mapper;
import org.mapstruct.Named;

@Mapper(componentModel = "jakarta")
public class CommonConverters {
  @Named("stringToUUID")
  public UUID toUUID(String id) {
    return id == null || id.isBlank() ? null : UUID.fromString(id);
  }

  @Named("uuidToString")
  public String toString(UUID id) {
    return id == null ? "" : id.toString();
  }

  @Named("bigDecimalToString")
  public String bigDecimalToString(BigDecimal value) {
    return value != null ? value.toPlainString() : "0";
  }

  @Named("stringToBigDecimal")
  public BigDecimal stringToBigDecimal(String str) {
    return (str != null && !str.isEmpty()) ? new BigDecimal(str) : BigDecimal.ZERO;
  }

  @Named("longToString")
  public String longToString(Long value) {
    return value != null ? value.toString() : "0";
  }

  @Named("stringToLong")
  public Long stringToLong(String str) {
    return (str != null && !str.isEmpty()) ? Long.parseLong(str) : 0L;
  }

  @Named("currencyToString")
  public String currencyToString(Currency currency) {
    return currency != null ? currency.getCurrencyCode() : "";
  }

  @Named("stringToCurrency")
  public Currency stringToCurrency(String str) {
    return (str != null && !str.isEmpty()) ? Currency.getInstance(str) : null;
  }

  @Named("pathToString")
  public String pathToString(Path path) {
    return path != null ? path.toString() : null;
  }

  @Named("stringToPath")
  public Path stringToPath(String path) {
    if (path == null) {
      return null;
    }
    if (path.startsWith("file:")) {
      try {
        return Path.of(new URI(path));
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Invalid file URI path: " + path, e);
      }
    }
    return Path.of(path);
  }
}
