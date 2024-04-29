/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker.windmill.work.refresh;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.joda.time.Duration;
import org.joda.time.Instant;

public final class DispatchedActiveWorkRefresher extends ActiveWorkRefresher {

  private final Consumer<Map<String, List<Windmill.HeartbeatRequest>>> activeWorkRefresherFn;

  public DispatchedActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<String, List<Windmill.HeartbeatRequest>>> activeWorkRefresherFn,
      ScheduledExecutorService scheduledExecutorService) {
    super(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        scheduledExecutorService);
    this.activeWorkRefresherFn = activeWorkRefresherFn;
  }

  public static DispatchedActiveWorkRefresher create(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      DataflowExecutionStateSampler sampler,
      Consumer<Map<String, List<Windmill.HeartbeatRequest>>> activeWorkRefresherFn,
      ScheduledExecutorService scheduledExecutorService) {
    return new DispatchedActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        sampler,
        activeWorkRefresherFn,
        scheduledExecutorService);
  }

  @Override
  protected synchronized void refreshActiveWork() {
    Map<String, List<Windmill.HeartbeatRequest>> heartbeats = new HashMap<>();
    Instant refreshDeadline = clock.get().minus(Duration.millis(activeWorkRefreshPeriodMillis));

    for (ComputationState computationState : computations.get()) {
      heartbeats.put(
          computationState.getComputationId(),
          HeartbeatRequests.getRefreshableKeyHeartbeats(
              computationState.getActiveWork(), refreshDeadline, sampler));
    }

    activeWorkRefresherFn.accept(heartbeats);
  }
}
