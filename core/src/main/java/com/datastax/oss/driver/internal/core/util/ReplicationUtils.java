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

import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;

public final class ReplicationUtils {

  /** The fully-qualified class name of the SimpleStrategy replication strategy. */
  public static final String SIMPLE_STRATEGY_FQCN = "org.apache.cassandra.locator.SimpleStrategy";

  /** The fully-qualified class name of the NetworkTopologyStrategy replication strategy. */
  public static final String NETWORK_TOPOLOGY_STRATEGY_FQCN =
      "org.apache.cassandra.locator.NetworkTopologyStrategy";

  private ReplicationUtils() {}

  /**
   * Determines if the given {@linkplain KeyspaceMetadata keyspace} was configured with
   * SimpleStrategy.
   *
   * @param keyspace the keyspace to inspect.
   * @return true if the keyspace is configured with SimpleStrategy, false otherwise.
   */
  public static boolean isSimpleStrategy(@NonNull KeyspaceMetadata keyspace) {
    return SIMPLE_STRATEGY_FQCN.equals(getReplicationStrategyClass(keyspace));
  }

  /**
   * Determines if the given {@linkplain KeyspaceMetadata keyspace} was configured with
   * NetworkTopologyStrategy.
   *
   * @param keyspace the keyspace to inspect.
   * @return true if the keyspace is configured with NetworkTopologyStrategy, false otherwise.
   */
  public static boolean isNetworkTopologyStrategy(@NonNull KeyspaceMetadata keyspace) {
    return NETWORK_TOPOLOGY_STRATEGY_FQCN.equals(getReplicationStrategyClass(keyspace));
  }

  /**
   * Returns the fully-qualified class name of the ReplicationStrategy implementation associated
   * with the given {@linkplain KeyspaceMetadata keyspace}.
   *
   * @param keyspace the keyspace to inspect.
   * @return the fully-qualified class name of the ReplicationStrategy implementation.
   */
  public static String getReplicationStrategyClass(@NonNull KeyspaceMetadata keyspace) {
    return keyspace.getReplication().get("class");
  }

  /**
   * Parses a string as a replication factor, and returns the replication factor as an int.
   *
   * <p>In case of transient replication, this method return the number of full replicas, excluding
   * the transient ones.
   *
   * @param rawReplicationFactor the string to parse.
   * @return the parsed replication factor.
   * @see <a href=
   *     "https://cassandra.apache.org/blog/2018/12/03/introducing-transient-replication.html">Introducing
   *     Transient Replication</a>
   */
  public static int parseReplicationFactor(@NonNull String rawReplicationFactor) {
    if (rawReplicationFactor.contains("/")) {
      // Transient replication (Cassandra 4.0): return the number of full replicas
      int slash = rawReplicationFactor.indexOf('/');
      String allPart = rawReplicationFactor.substring(0, slash);
      String transientPart = rawReplicationFactor.substring(slash + 1);
      return Integer.parseInt(allPart) - Integer.parseInt(transientPart);
    } else {
      return Integer.parseInt(rawReplicationFactor);
    }
  }
}
