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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.ConsistencyUtils;
import com.datastax.oss.driver.internal.core.util.DatacenterUtils;
import com.datastax.oss.driver.internal.core.util.ReplicationUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** A factory for {@link RingDiagnosticGenerator} instances. */
public class RingDiagnosticGeneratorFactory {

  private static final Log LOG = LogFactory.getLog(RingDiagnosticGeneratorFactory.class);

  private final Session session;

  /**
   * Creates a new {@link RingDiagnosticGeneratorFactory}.
   *
   * @param session the {@link Session} to use to access token, topology and schema metadata.
   */
  public RingDiagnosticGeneratorFactory(@NonNull Session session) {
    Objects.requireNonNull(session, "session must not be null");
    this.session = session;
  }

  /**
   * Creates a {@link RingDiagnosticGenerator} for the configured session, inferring all required
   * information from the session itself.
   *
   * <p>This method returns a non-empty {@link Optional} if the following conditions are met:
   *
   * <ol>
   *   <li>The session is bound to a keyspace;
   *   <li>Schema metadata is enabled on the session;
   *   <li>Token metadata is enabled on the session;
   *   <li>The keyspace has a supported replication strategy;
   *   <li>The local datacenter name can be inferred from the session (required only for local
   *       consistency levels).
   * </ol>
   *
   * <p>If the above conditions are not met, this method returns a {@link Optional#empty()}.
   *
   * @return a {@link RingDiagnosticGenerator} instance, or {@link Optional#empty()} if none could
   *     be created.
   */
  @NonNull
  public Optional<RingDiagnosticGenerator> maybeCreate() {
    if (isTokenMetadataDisabled() || isSchemaMetadataDisabled()) {
      return Optional.empty();
    }
    if (!session.getKeyspace().isPresent()) {
      LOG.warn(
          "The session is not bound to a keyspace. Token ring health reports will be disabled");
      return Optional.empty();
    }
    CqlIdentifier keyspaceName = session.getKeyspace().get();
    Metadata metadata = session.refreshSchema();
    if (!metadata.getKeyspace(keyspaceName).isPresent()) {
      LOG.warn(
          String.format(
              "Keyspace %s does not exist or its metadata could not be retrieved. Token ring health reports will be disabled",
              keyspaceName.asCql(true)));
      return Optional.empty();
    }
    KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName).get();
    ConsistencyLevel consistencyLevel = ConsistencyUtils.getConsistencyLevel(session);
    String localDatacenter =
        consistencyLevel.isDcLocal()
            ? DatacenterUtils.inferLocalDatacenter(session).orElse(null)
            : null;
    return maybeCreate(keyspace, consistencyLevel, localDatacenter);
  }

  /**
   * Creates a {@link RingDiagnosticGenerator} for the given {@linkplain KeyspaceMetadata keyspace},
   * {@linkplain ConsistencyLevel consistency level} and, optionally, local datacenter.
   *
   * <p>This method returns a non-empty {@link Optional} if the following conditions are met:
   *
   * <ol>
   *   <li>Token metadata is enabled on the session;
   *   <li>The keyspace has a supported replication strategy;
   *   <li>The local datacenter name is non-null (required only for local consistency levels).
   * </ol>
   *
   * <p>If the above conditions are not met, this method returns a {@link Optional#empty()}.
   *
   * @param keyspace the {@link KeyspaceMetadata keyspace} to use.
   * @param consistencyLevel the {@link ConsistencyLevel consistency level} to use.
   * @param localDatacenter the local datacenter name; only required for {@linkplain
   *     ConsistencyLevel#isDcLocal() datacenter-local consistency levels}, may be null otherwise.
   * @return a {@link RingDiagnosticGenerator} instance, or {@link Optional#empty()} if none could
   *     be created.
   */
  @NonNull
  public Optional<RingDiagnosticGenerator> maybeCreate(
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @Nullable String localDatacenter) {
    Objects.requireNonNull(keyspace, "keyspace must not be null");
    Objects.requireNonNull(consistencyLevel, "consistencyLevel must not be null");
    if (isTokenMetadataDisabled()) {
      return Optional.empty();
    }
    if (ReplicationUtils.isSimpleStrategy(keyspace)) {
      return createForSimpleStrategy(keyspace, consistencyLevel);
    }
    if (ReplicationUtils.isNetworkTopologyStrategy(keyspace)) {
      return createForNetworkTopologyStrategy(keyspace, consistencyLevel, localDatacenter);
    }
    LOG.warn(
        String.format(
            "Unsupported replication strategy %s for keyspace %s. Token ring health reports will be disabled",
            ReplicationUtils.getReplicationStrategyClass(keyspace),
            keyspace.getName().asCql(true)));
    return Optional.empty();
  }

  private boolean isTokenMetadataDisabled() {
    DriverExecutionProfile defaultProfile = session.getContext().getConfig().getDefaultProfile();
    if (!defaultProfile.getBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED)) {
      LOG.warn(
          "Token metadata computation has been disabled. Token ring health reports will be disabled");
      return true;
    }
    return false;
  }

  private boolean isSchemaMetadataDisabled() {
    if (!session.isSchemaMetadataEnabled()) {
      LOG.warn(
          "Schema metadata computation has been disabled. Token ring health reports will be disabled");
      return true;
    }
    return false;
  }

  private Optional<RingDiagnosticGenerator> createForSimpleStrategy(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel) {
    ConsistencyLevel filteredConsistencyLevel =
        ConsistencyUtils.filterForSimpleStrategy(consistencyLevel);
    if (filteredConsistencyLevel != consistencyLevel) {
      LOG.warn(
          String.format(
              "Consistency level %s is not compatible with the SimpleStrategy replication configured for keyspace %s."
                  + "Token ring health reports will assume %s instead.",
              consistencyLevel, keyspace.getName().asCql(true), filteredConsistencyLevel));
    }
    int replicationFactor =
        ReplicationUtils.parseReplicationFactor(
            keyspace.getReplication().get("replication_factor"));
    GlobalRingDiagnosticGenerator generator =
        new GlobalRingDiagnosticGenerator(
            session, keyspace, filteredConsistencyLevel, replicationFactor);
    return Optional.of(generator);
  }

  private Optional<RingDiagnosticGenerator> createForNetworkTopologyStrategy(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel, String localDatacenter) {
    if (consistencyLevel.isDcLocal()) {
      return createForLocalCL(keyspace, consistencyLevel, localDatacenter);
    }
    if (consistencyLevel == ConsistencyLevel.EACH_QUORUM) {
      return createForEachQuorum(keyspace);
    }
    return createForNonLocalCL(keyspace, consistencyLevel);
  }

  private Optional<RingDiagnosticGenerator> createForLocalCL(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel, String localDatacenter) {
    if (localDatacenter == null) {
      // If the consistency level is local, we also need the local DC name
      LOG.warn(
          String.format(
              "No local datacenter was provided, but the consistency level is local (%s). Token ring health reports will be disabled",
              consistencyLevel));
      return Optional.empty();
    }
    Map<String, String> replication = keyspace.getReplication();
    if (!replication.containsKey(localDatacenter)) {
      // Bad config: the specified local DC is not listed in the replication options
      LOG.warn(
          String.format(
              "The local datacenter (%s) does not have a corresponding entry in replication options for keyspace %s. Token ring health reports will be disabled",
              localDatacenter, keyspace.getName().asCql(true)));
      return Optional.empty();
    }
    int replicationFactor =
        ReplicationUtils.parseReplicationFactor(replication.get(localDatacenter));
    LocalRingDiagnosticGenerator generator =
        new LocalRingDiagnosticGenerator(
            session, keyspace, consistencyLevel, localDatacenter, replicationFactor);
    return Optional.of(generator);
  }

  private Optional<RingDiagnosticGenerator> createForEachQuorum(KeyspaceMetadata keyspace) {
    Map<String, Integer> replicationFactorsByDc = new HashMap<>();
    for (Entry<String, String> entry : keyspace.getReplication().entrySet()) {
      if (entry.getKey().equals("class")) {
        continue;
      }
      String datacenter = entry.getKey();
      int replicationFactor = ReplicationUtils.parseReplicationFactor(entry.getValue());
      replicationFactorsByDc.put(datacenter, replicationFactor);
    }
    EachQuorumRingDiagnosticGenerator generator =
        new EachQuorumRingDiagnosticGenerator(session, keyspace, replicationFactorsByDc);
    return Optional.of(generator);
  }

  private Optional<RingDiagnosticGenerator> createForNonLocalCL(
      KeyspaceMetadata keyspace, ConsistencyLevel consistencyLevel) {
    int sumOfReplicationFactors = 0;
    for (Entry<String, String> entry : keyspace.getReplication().entrySet()) {
      if (entry.getKey().equals("class")) {
        continue;
      }
      int replicationFactor = ReplicationUtils.parseReplicationFactor(entry.getValue());
      sumOfReplicationFactors += replicationFactor;
    }
    GlobalRingDiagnosticGenerator generator =
        new GlobalRingDiagnosticGenerator(
            session, keyspace, consistencyLevel, sumOfReplicationFactors);
    return Optional.of(generator);
  }
}
