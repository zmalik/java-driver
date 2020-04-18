/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.ByteOrderedToken;
import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;

public final class TokenUtils {

  private TokenUtils() {}

  /**
   * Formats the given {@link TokenRange} as {@code ]start,end]}.
   *
   * @param range the {@link TokenRange} to format.
   * @return the formatted token range.
   */
  public static String tokenRangeAsString(@NonNull TokenRange range) {
    return String.format("]%s,%s]", tokenAsString(range.getStart()), tokenAsString(range.getEnd()));
  }

  /**
   * Formats the given {@link Token} and prints its raw value.
   *
   * @param token the {@link Token} to format.
   * @return the formatted token.
   */
  public static String tokenAsString(@NonNull Token token) {
    if (token instanceof Murmur3Token) {
      return Long.toString(((Murmur3Token) token).getValue());
    } else if (token instanceof RandomToken) {
      return ((RandomToken) token).getValue().toString();
    } else if (token instanceof ByteOrderedToken) {
      return Bytes.toHexString(((ByteOrderedToken) token).getValue());
    } else {
      return token.toString();
    }
  }
}
