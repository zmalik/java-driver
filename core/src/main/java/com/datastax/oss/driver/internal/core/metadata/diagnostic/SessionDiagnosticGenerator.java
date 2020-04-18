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
package com.datastax.oss.driver.internal.core.metadata.diagnostic;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.diagnostic.SessionDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.metadata.diagnostic.ring.RingDiagnosticGenerator;
import com.datastax.oss.driver.internal.core.metadata.diagnostic.ring.RingDiagnosticGeneratorFactory;
import com.datastax.oss.driver.internal.core.metadata.diagnostic.topology.TopologyDiagnosticGenerator;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;

public class SessionDiagnosticGenerator {

  private final TopologyDiagnosticGenerator topologyDiagnosticGenerator;
  private final RingDiagnosticGeneratorFactory ringDiagnosticGeneratorFactory;

  public SessionDiagnosticGenerator(@NonNull Session session) {
    Objects.requireNonNull(session, "session must not be null");
    topologyDiagnosticGenerator = new TopologyDiagnosticGenerator(session::getMetadata);
    ringDiagnosticGeneratorFactory = new RingDiagnosticGeneratorFactory(session);
  }

  @NonNull
  public SessionDiagnostic generate() {
    return new DefaultSessionDiagnostic(
        topologyDiagnosticGenerator.generate(),
        ringDiagnosticGeneratorFactory
            .maybeCreate()
            .map(RingDiagnosticGenerator::generate)
            .orElse(null));
  }

  @NonNull
  public SessionDiagnostic generate(
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @Nullable String datacenter) {
    Objects.requireNonNull(keyspace, "keyspace must not be null");
    Objects.requireNonNull(consistencyLevel, "consistencyLevel must not be null");
    return new DefaultSessionDiagnostic(
        topologyDiagnosticGenerator.generate(),
        ringDiagnosticGeneratorFactory
            .maybeCreate(keyspace, consistencyLevel, datacenter)
            .map(RingDiagnosticGenerator::generate)
            .orElse(null));
  }
}
