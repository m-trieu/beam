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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import com.google.auto.value.AutoValue;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * Represents the current state of connections to Streaming Engine. Connections are updated when
 * backend workers assigned to the key ranges being processed by this user worker change during
 * pipeline execution. For example, changes can happen via autoscaling, load-balancing, or other
 * backend updates.
 */
@AutoValue
abstract class StreamEngineConnectionState {
  static final StreamEngineConnectionState EMPTY = builder().build();

  static Builder builder() {
    return new AutoValue_StreamEngineConnectionState.Builder()
        .setWindmillConnections(ImmutableMap.of())
        .setWindmillStreams(ImmutableMap.of())
        .setGlobalDataStreams(ImmutableMap.of())
        .setGlobalDataEndpoints(ImmutableMap.of());
  }

  abstract ImmutableMap<WindmillEndpoints.Endpoint, WindmillConnection> windmillConnections();

  abstract ImmutableMap<WindmillConnection, WindmillStreamSender> windmillStreams();

  abstract ImmutableMap<String, WindmillEndpoints.Endpoint> globalDataEndpoints();

  abstract ImmutableMap<WindmillEndpoints.Endpoint, Supplier<WindmillStream.GetDataStream>>
      globalDataStreams();

  @AutoValue.Builder
  abstract static class Builder {
    public abstract Builder setWindmillConnections(
        ImmutableMap<WindmillEndpoints.Endpoint, WindmillConnection> value);

    public abstract Builder setWindmillStreams(
        ImmutableMap<WindmillConnection, WindmillStreamSender> value);

    public abstract Builder setGlobalDataEndpoints(
        ImmutableMap<String, WindmillEndpoints.Endpoint> value);

    public abstract Builder setGlobalDataStreams(
        ImmutableMap<WindmillEndpoints.Endpoint, Supplier<WindmillStream.GetDataStream>> value);

    public abstract StreamEngineConnectionState build();
  }
}
