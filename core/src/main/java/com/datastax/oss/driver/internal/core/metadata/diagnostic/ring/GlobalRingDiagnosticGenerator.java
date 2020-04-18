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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.ring;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.diagnostic.RingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.ConsistencyUtils;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link RingDiagnosticGenerator} that checks if the configured consistency level is achievable
 * globally, on the whole cluster.
 *
 * <p>This reporter is suitable only for SimpleStrategy replications, or for when the configured
 * consistency level is intended to be achieved globally, such as with {@link
 * ConsistencyLevel#QUORUM}.
 */
public class GlobalRingDiagnosticGenerator extends AbstractRingDiagnosticGenerator {

  protected final ConsistencyLevel consistencyLevel;

  protected final int requiredReplicas;

  /**
   * Creates a new instance of {@link GlobalRingDiagnosticGenerator}.
   *
   * @param session the {@linkplain Session session} to use.
   * @param keyspace the {@linkplain KeyspaceMetadata keyspace} to check.
   * @param consistencyLevel the {@linkplain ConsistencyLevel consistency level} that we want to
   *     achieve.
   * @param replicationFactor the replication factor.
   */
  public GlobalRingDiagnosticGenerator(
      @NonNull Session session,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      int replicationFactor) {
    super(session, keyspace);
    Objects.requireNonNull(consistencyLevel, "consistencyLevel cannot be null");
    Preconditions.checkArgument(
        consistencyLevel != ConsistencyLevel.EACH_QUORUM,
        "GlobalRingDiagnosticGenerator is not compatible with EACH_QUORUM");
    this.consistencyLevel = consistencyLevel;
    this.requiredReplicas = ConsistencyUtils.requiredReplicas(consistencyLevel, replicationFactor);
  }

  @Override
  protected TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> aliveReplicas) {
    return new SimpleTokenRangeDiagnostic(
        range, keyspace, consistencyLevel, requiredReplicas, aliveReplicas.size());
  }

  @Override
  protected RingDiagnostic generateRingDiagnostic(
      final Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    return new DefaultRingDiagnostic(keyspace, consistencyLevel, tokenRangeDiagnostics);
  }
}
