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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link RingDiagnosticGenerator} that checks if the configured consistency level is achievable
 * locally, on a single datacenter.
 *
 * <p>This report is suitable only for when the configured consistency level is intended to be
 * {@linkplain ConsistencyLevel#isDcLocal() achieved locally} on a specific datacenter, such as with
 * {@link ConsistencyLevel#LOCAL_QUORUM}.
 */
public class LocalRingDiagnosticGenerator extends GlobalRingDiagnosticGenerator {

  private final String datacenter;

  public LocalRingDiagnosticGenerator(
      @NonNull Session session,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @NonNull String datacenter,
      int replicationFactor) {
    super(session, keyspace, consistencyLevel, replicationFactor);
    Objects.requireNonNull(datacenter, "datacenter cannot be null");
    Preconditions.checkArgument(
        consistencyLevel.isDcLocal(),
        "LocalRingDiagnosticGenerator is not compatible with " + consistencyLevel);
    this.datacenter = datacenter;
  }

  @Override
  protected TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> aliveReplicas) {
    int aliveReplicasInDc =
        (int) aliveReplicas.stream().map(Node::getDatacenter).filter(datacenter::equals).count();
    return new SimpleTokenRangeDiagnostic(
        range, keyspace, consistencyLevel, requiredReplicas, aliveReplicasInDc);
  }

  @Override
  protected RingDiagnostic generateRingDiagnostic(Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    return new DefaultLocalRingDiagnostic(
        keyspace, consistencyLevel, datacenter, tokenRangeDiagnostics);
  }
}
