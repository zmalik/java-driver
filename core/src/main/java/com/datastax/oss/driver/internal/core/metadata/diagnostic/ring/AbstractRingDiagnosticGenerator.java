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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.diagnostic.RingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractRingDiagnosticGenerator implements RingDiagnosticGenerator {

  protected final Session session;

  protected final KeyspaceMetadata keyspace;

  protected AbstractRingDiagnosticGenerator(
      @NonNull Session session, @NonNull KeyspaceMetadata keyspace) {
    Objects.requireNonNull(session, "session cannot be null");
    Objects.requireNonNull(keyspace, "keyspace cannot be null");
    this.session = session;
    this.keyspace = keyspace;
  }

  @NonNull
  @Override
  public RingDiagnostic generate() {
    Set<TokenRangeDiagnostic> tokenRangeDiagnostics;
    if (session.getMetadata().getTokenMap().isPresent()) {
      TokenMap tokenMap = session.getMetadata().getTokenMap().get();
      tokenRangeDiagnostics = generateTokenRangeDiagnostics(tokenMap);
    } else {
      tokenRangeDiagnostics = Collections.emptyNavigableSet();
    }
    return generateRingDiagnostic(tokenRangeDiagnostics);
  }

  protected Set<TokenRangeDiagnostic> generateTokenRangeDiagnostics(TokenMap tokenMap) {
    Set<TokenRangeDiagnostic> reports = new HashSet<>();
    for (TokenRange range : tokenMap.getTokenRanges()) {
      Set<Node> allReplicas = tokenMap.getReplicas(keyspace.getName(), range);
      Set<Node> aliveReplicas =
          allReplicas.stream()
              .filter(
                  node -> node.getState() == NodeState.UP || node.getState() == NodeState.UNKNOWN)
              .collect(Collectors.toSet());
      TokenRangeDiagnostic report = generateTokenRangeDiagnostic(range, aliveReplicas);
      reports.add(report);
    }
    return reports;
  }

  protected abstract TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> aliveReplicas);

  protected abstract RingDiagnostic generateRingDiagnostic(
      Set<TokenRangeDiagnostic> tokenRangeDiagnostics);
}
