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
package com.datastax.oss.driver.api.core.metadata.diagnostic;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/**
 * A health {@link Diagnostic} for the session, obtained via {@link Session#generateDiagnostic()}.
 *
 * <p>A session health diagnostic is comprised of two main sections:
 *
 * <ol>
 *   <li>{@linkplain #getTopologyDiagnostic() Cluster topology diagnostic}; this section is always
 *       present.
 *   <li>An optional {@linkplain #getRingDiagnostic() token range availability diagnostic}
 * </ol>
 */
public interface SessionDiagnostic extends Diagnostic {

  /**
   * Returns a health diagnostic for the cluster topology. See the javadocs of {@link
   * TopologyDiagnostic} for more information.
   */
  @NonNull
  TopologyDiagnostic getTopologyDiagnostic();

  /**
   * Returns a health diagnostic for the token ring. See the javadocs of {@link RingDiagnostic} for
   * more information.
   *
   * <p>Token range diagnostics are not always feasible. In order for the driver to be able to
   * generate a {@link RingDiagnostic}, the following conditions apply:
   *
   * <ol>
   *   <li>The session must be bound to a keyspace, or a keyspace must be explicitly specified;
   *   <li>The keyspace must have supported replication strategy;
   *   <li>Schema metadata must be enabled on the session;
   *   <li>Token metadata must be enabled on the session;
   *   <li>The local datacenter name must be inferred from the session, or explicitly provided
   *       (required only for local consistency levels).
   * </ol>
   */
  @NonNull
  Optional<RingDiagnostic> getRingDiagnostic();

  @NonNull
  @Override
  default Status getStatus() {
    Status status = getTopologyDiagnostic().getStatus();
    if (getRingDiagnostic().isPresent()) {
      status = status.mergeWith(getRingDiagnostic().get().getStatus());
    }
    return status;
  }

  @NonNull
  @Override
  default Map<String, Object> getDetails() {
    ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder()
            .put("topology", getTopologyDiagnostic().getDetails());
    if (getRingDiagnostic().isPresent()) {
      builder.put("ring", getRingDiagnostic().get().getDetails());
    }
    return builder.build();
  }
}
