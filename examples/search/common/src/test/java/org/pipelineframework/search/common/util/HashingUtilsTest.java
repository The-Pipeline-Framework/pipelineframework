package org.pipelineframework.search.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HashingUtilsTest {

  @Test
  void returnsNullForBlank() {
    assertNull(HashingUtils.sha256Base64Url(" "));
  }

  @Test
  void hashesContentDeterministically() {
    assertEquals(
        "LPJNul-wow4m6DsqxbninhsWHlwfp0JecwQzYpOLmCQ",
        HashingUtils.sha256Base64Url("hello"));
  }
}
