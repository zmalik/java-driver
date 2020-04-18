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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * A health {@link Diagnostic} for a group of nodes, detailing how many nodes were found in total,
 * how many were up, and how many were down.
 *
 * <p>The breadth of the report's scope depends on how it was created; it may refer to the entire
 * cluster, or to just a datacenter.
 */
public interface NodeGroupDiagnostic extends Diagnostic {

  /** @return the total number of nodes in this group. */
  int getTotal();

  /**
   * Returns the number of nodes in this group that are known to be up.
   *
   * <p>Nodes are considered up if their {@linkplain
   * com.datastax.oss.driver.api.core.metadata.Node#getState() state} is either {@link
   * com.datastax.oss.driver.api.core.metadata.NodeState#UP UP} or {@link
   * com.datastax.oss.driver.api.core.metadata.NodeState#UNKNOWN UNKNOWN}. This is because node
   * states are determined from Gossip events, and these are not 100% reliable.
   *
   * @return the number of nodes in this group that are known to be up.
   */
  int getUp();

  /**
   * Returns the number of nodes in this group that are known to be down.
   *
   * <p>Nodes are considered down if their {@linkplain
   * com.datastax.oss.driver.api.core.metadata.Node#getState() state} is neither {@link
   * com.datastax.oss.driver.api.core.metadata.NodeState#UP UP} nor {@link
   * com.datastax.oss.driver.api.core.metadata.NodeState#UNKNOWN UNKNOWN}. This is because node
   * states are determined from Gossip events, and these are not 100% reliable.
   *
   * @return the number of nodes in this group that are known to be up.
   */
  int getDown();

  @NonNull
  @Override
  default Status getStatus() {
    if (getTotal() == 0) {
      return Status.UNAVAILABLE;
    } else if (getDown() == 0) {
      return Status.AVAILABLE;
    } else {
      return Status.PARTIALLY_AVAILABLE;
    }
  }

  @NonNull
  @Override
  default Map<String, Object> getDetails() {
    return ImmutableMap.of("total", getTotal(), "up", getUp(), "down", getDown());
  }
}
