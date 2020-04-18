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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * A {@link RingDiagnosticGenerator} that checks if the configured consistency level is achievable
 * on all datacenters individually.
 *
 * <p>This reporter is only suitable for the special consistency level {@link
 * ConsistencyLevel#EACH_QUORUM}.
 */
public class EachQuorumRingDiagnosticGenerator extends AbstractRingDiagnosticGenerator {

  private final Map<String, Integer> requiredReplicasByDc;

  public EachQuorumRingDiagnosticGenerator(
      @NonNull Session session,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull Map<String, Integer> replicationFactorsByDc) {
    super(session, keyspace);
    Objects.requireNonNull(replicationFactorsByDc, "replicationFactorsByDc cannot be null");
    this.requiredReplicasByDc = createRequiredReplicasByDcMap(replicationFactorsByDc);
  }

  private Map<String, Integer> createRequiredReplicasByDcMap(
      Map<String, Integer> replicationFactorsByDc) {
    Map<String, Integer> requiredReplicasByDc = new TreeMap<>();
    for (String datacenter : replicationFactorsByDc.keySet()) {
      int requiredReplicas =
          ConsistencyUtils.requiredReplicas(
              ConsistencyLevel.EACH_QUORUM, replicationFactorsByDc.get(datacenter));
      requiredReplicasByDc.put(datacenter, requiredReplicas);
    }
    return Collections.unmodifiableMap(requiredReplicasByDc);
  }

  @Override
  protected TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> aliveReplicas) {
    CompositeTokenRangeDiagnostic.Builder diagnostic =
        new CompositeTokenRangeDiagnostic.Builder(range, keyspace, ConsistencyLevel.EACH_QUORUM);
    Map<String, Integer> aliveReplicasByDc = getAliveReplicasByDc(aliveReplicas);
    for (String datacenter : this.requiredReplicasByDc.keySet()) {
      int requiredReplicasInDc = this.requiredReplicasByDc.get(datacenter);
      int aliveReplicasInDc = aliveReplicasByDc.getOrDefault(datacenter, 0);
      TokenRangeDiagnostic childDiagnostic =
          new SimpleTokenRangeDiagnostic(
              range,
              keyspace,
              ConsistencyLevel.EACH_QUORUM,
              requiredReplicasInDc,
              aliveReplicasInDc);
      diagnostic.addChildDiagnostic(datacenter, childDiagnostic);
    }
    return diagnostic.build();
  }

  @Override
  protected RingDiagnostic generateRingDiagnostic(Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    return new DefaultRingDiagnostic(keyspace, ConsistencyLevel.EACH_QUORUM, tokenRangeDiagnostics);
  }

  private Map<String, Integer> getAliveReplicasByDc(Set<Node> aliveReplicas) {
    return aliveReplicas.stream()
        .collect(Collectors.toMap(Node::getDatacenter, (replica) -> 1, Integer::sum));
  }
}
