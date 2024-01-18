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

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.LOCALHOST;
import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.localhostChannel;

import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillStubFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages endpoints and stubs for connecting to the Windmill Dispatcher. */
@ThreadSafe
class GrpcDispatcherClient {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDispatcherClient.class);
  private final WindmillStubFactory windmillStubFactory;

  @GuardedBy("this")
  private final AtomicReference<DispatcherStubs> dispatcherStubs;

  @GuardedBy("this")
  private final Random rand;

  private GrpcDispatcherClient(
      WindmillStubFactory windmillStubFactory,
      AtomicReference<DispatcherStubs> dispatcherStubs,
      Random rand) {
    this.windmillStubFactory = windmillStubFactory;
    this.rand = rand;
    this.dispatcherStubs = dispatcherStubs;
  }

  static GrpcDispatcherClient create(WindmillStubFactory windmillStubFactory) {
    return new GrpcDispatcherClient(
        windmillStubFactory, new AtomicReference<>(DispatcherStubs.empty()), new Random());
  }

  @VisibleForTesting
  static GrpcDispatcherClient forTesting(
      WindmillStubFactory windmillGrpcStubFactory,
      List<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs,
      List<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs,
      Set<HostAndPort> dispatcherEndpoints) {
    Preconditions.checkArgument(
        dispatcherEndpoints.size() == windmillServiceStubs.size()
            && windmillServiceStubs.size() == windmillMetadataServiceStubs.size());
    return new GrpcDispatcherClient(
        windmillGrpcStubFactory,
        new AtomicReference<>(
            DispatcherStubs.create(
                dispatcherEndpoints, windmillServiceStubs, windmillMetadataServiceStubs)),
        new Random());
  }

  synchronized CloudWindmillServiceV1Alpha1Stub getWindmillServiceStub() {
    ImmutableList<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs =
        dispatcherStubs.get().windmillServiceStubs();
    Preconditions.checkState(
        !windmillServiceStubs.isEmpty(), "windmillServiceEndpoint has not been set");

    return (windmillServiceStubs.size() == 1
        ? windmillServiceStubs.get(0)
        : windmillServiceStubs.get(rand.nextInt(windmillServiceStubs.size())));
  }

  synchronized CloudWindmillMetadataServiceV1Alpha1Stub getWindmillMetadataServiceStub() {
    ImmutableList<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs =
        dispatcherStubs.get().windmillMetadataServiceStubs();
    Preconditions.checkState(
        !windmillMetadataServiceStubs.isEmpty(), "windmillServiceEndpoint has not been set");

    return (windmillMetadataServiceStubs.size() == 1
        ? windmillMetadataServiceStubs.get(0)
        : windmillMetadataServiceStubs.get(rand.nextInt(windmillMetadataServiceStubs.size())));
  }

  synchronized boolean isReady() {
    return dispatcherStubs.get().isReady();
  }

  synchronized void consumeWindmillDispatcherEndpoints(
      ImmutableSet<HostAndPort> dispatcherEndpoints) {
    ImmutableSet<HostAndPort> currentDispatcherEndpoints =
        dispatcherStubs.get().dispatcherEndpoints();
    Preconditions.checkArgument(
        dispatcherEndpoints != null && !dispatcherEndpoints.isEmpty(),
        "Cannot set dispatcher endpoints to nothing.");
    if (currentDispatcherEndpoints.equals(dispatcherEndpoints)) {
      // The endpoints are equal don't recreate the stubs.
      return;
    }

    LOG.info("Creating a new windmill stub, endpoints: {}", dispatcherEndpoints);
    if (!currentDispatcherEndpoints.isEmpty()) {
      LOG.info("Previous windmill stub endpoints: {}", currentDispatcherEndpoints);
    }

    LOG.info("Initializing Streaming Engine GRPC client for endpoints: {}", dispatcherEndpoints);
    dispatcherStubs.set(DispatcherStubs.create(dispatcherEndpoints, windmillStubFactory));
  }

  @AutoValue
  abstract static class DispatcherStubs {

    private static DispatcherStubs empty() {
      return new AutoValue_GrpcDispatcherClient_DispatcherStubs(
          ImmutableSet.of(), ImmutableList.of(), ImmutableList.of());
    }

    private static DispatcherStubs create(
        Set<HostAndPort> endpoints,
        List<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs,
        List<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs) {
      return new AutoValue_GrpcDispatcherClient_DispatcherStubs(
          ImmutableSet.copyOf(endpoints),
          ImmutableList.copyOf(windmillServiceStubs),
          ImmutableList.copyOf(windmillMetadataServiceStubs));
    }

    private static DispatcherStubs create(
        ImmutableSet<HostAndPort> newDispatcherEndpoints, WindmillStubFactory windmillStubFactory) {
      ImmutableList.Builder<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs =
          ImmutableList.builder();
      ImmutableList.Builder<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs =
          ImmutableList.builder();

      for (HostAndPort endpoint : newDispatcherEndpoints) {
        windmillServiceStubs.add(createWindmillServiceStub(endpoint, windmillStubFactory));
        windmillMetadataServiceStubs.add(
            createWindmillMetadataServiceStub(endpoint, windmillStubFactory));
      }

      return new AutoValue_GrpcDispatcherClient_DispatcherStubs(
          newDispatcherEndpoints,
          windmillServiceStubs.build(),
          windmillMetadataServiceStubs.build());
    }

    private static CloudWindmillServiceV1Alpha1Stub createWindmillServiceStub(
        HostAndPort endpoint, WindmillStubFactory windmillStubFactory) {
      if (LOCALHOST.equals(endpoint.getHost())) {
        return CloudWindmillServiceV1Alpha1Grpc.newStub(localhostChannel(endpoint.getPort()));
      }

      return windmillStubFactory.createWindmillServiceStub(WindmillServiceAddress.create(endpoint));
    }

    private static CloudWindmillMetadataServiceV1Alpha1Stub createWindmillMetadataServiceStub(
        HostAndPort endpoint, WindmillStubFactory windmillStubFactory) {
      if (LOCALHOST.equals(endpoint.getHost())) {
        return CloudWindmillMetadataServiceV1Alpha1Grpc.newStub(
            localhostChannel(endpoint.getPort()));
      }

      return windmillStubFactory.createWindmillMetadataServiceStub(
          WindmillServiceAddress.create(endpoint));
    }

    private boolean isReady() {
      return !windmillServiceStubs().isEmpty()
          && !windmillMetadataServiceStubs().isEmpty()
          && !dispatcherEndpoints().isEmpty();
    }

    abstract ImmutableSet<HostAndPort> dispatcherEndpoints();

    abstract ImmutableList<CloudWindmillServiceV1Alpha1Stub> windmillServiceStubs();

    abstract ImmutableList<CloudWindmillMetadataServiceV1Alpha1Stub> windmillMetadataServiceStubs();
  }
}
