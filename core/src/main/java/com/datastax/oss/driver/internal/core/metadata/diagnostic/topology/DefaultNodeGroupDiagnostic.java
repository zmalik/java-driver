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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.topology;

import com.datastax.oss.driver.api.core.metadata.diagnostic.NodeGroupDiagnostic;

public class DefaultNodeGroupDiagnostic implements NodeGroupDiagnostic {

  private final int total;

  private final int up;

  private final int down;

  public DefaultNodeGroupDiagnostic(int total, int up, int down) {
    this.total = total;
    this.up = up;
    this.down = down;
  }

  @Override
  public int getTotal() {
    return this.total;
  }

  @Override
  public int getUp() {
    return this.up;
  }

  @Override
  public int getDown() {
    return this.down;
  }

  public static class Builder {

    private int total;

    private int up;

    private int down;

    public void incrementTotal() {
      this.total++;
    }

    public void incrementUp() {
      this.up++;
    }

    public void incrementDown() {
      this.down++;
    }

    public DefaultNodeGroupDiagnostic build() {
      return new DefaultNodeGroupDiagnostic(total, up, down);
    }
  }
}
