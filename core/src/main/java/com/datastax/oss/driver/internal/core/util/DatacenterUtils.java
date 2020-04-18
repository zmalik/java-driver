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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class DatacenterUtils {

  private DatacenterUtils() {}

  /**
   * Attempts to determine the local datacenter to which the given {@link CqlSession} is bound.
   *
   * <p>Determining a local datacenter is not a trivial task, because the very notion of "local
   * datacenter" depends on the {@link LoadBalancingPolicy} in use, and how it categorizes nodes and
   * datacenters. Since the driver does not control which policy is being used, guessing the local
   * datacenter becomes an heuristic that only wokrs for certain common configurations.
   *
   * <p>This method first attempts to identify a local datacenter in the driver configuration; if
   * that fails, it uses an internal driver component to attempt a datacenter discovery from the
   * initial contact points. If that fails too, this method returns {@link Optional#empty()}.
   *
   * @param session the {@link CqlSession} to inspect.
   * @return the local datacenter if it could be inferred, or {@link Optional#empty()} otherwise.
   */
  public static Optional<String> inferLocalDatacenter(@NonNull Session session) {
    InternalDriverContext internalDriverContext = (InternalDriverContext) session.getContext();
    DriverExecutionProfile defaultProfile = session.getContext().getConfig().getDefaultProfile();
    // local DC was set programmatically
    String localDatacenter = internalDriverContext.getLocalDatacenter(defaultProfile.getName());
    if (localDatacenter == null) {
      // local DC was specified in the config
      localDatacenter =
          defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
      if (localDatacenter == null) {
        // infer the DC from the contact points, if they all share the same DC
        Set<String> datacenters =
            internalDriverContext.getMetadataManager().getContactPoints().stream()
                .map(Node::getDatacenter)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        if (datacenters.size() == 1) {
          localDatacenter = datacenters.iterator().next();
        }
      }
    }
    return Optional.ofNullable(localDatacenter);
  }
}
