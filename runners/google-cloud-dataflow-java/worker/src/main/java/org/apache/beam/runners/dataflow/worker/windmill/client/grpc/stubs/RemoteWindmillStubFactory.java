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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs;

import static org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.WindmillChannelFactory.remoteChannel;

import com.google.auth.Credentials;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillMetadataServiceV1Alpha1Grpc.CloudWindmillMetadataServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.auth.VendoredCredentialsAdapter;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.auth.MoreCallCredentials;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates remote stubs to talk to Streaming Engine. */
@Internal
@ThreadSafe
public final class RemoteWindmillStubFactory implements WindmillStubFactory, ChannelCache {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteWindmillStubFactory.class);
  private final int rpcChannelTimeoutSec;
  private final Credentials gcpCredentials;
  private final boolean useIsolatedChannels;

  private final ConcurrentMap<WindmillServiceAddress, ManagedChannel> channelCache;

  public RemoteWindmillStubFactory(
      int rpcChannelTimeoutSec, Credentials gcpCredentials, boolean useIsolatedChannels) {
    this.rpcChannelTimeoutSec = rpcChannelTimeoutSec;
    this.gcpCredentials = gcpCredentials;
    this.useIsolatedChannels = useIsolatedChannels;
    this.channelCache = new ConcurrentHashMap<>();
  }

  @Override
  public CloudWindmillServiceV1Alpha1Stub createWindmillServiceStub(
      WindmillServiceAddress serviceAddress) {
    CloudWindmillServiceV1Alpha1Stub windmillServiceStub =
        CloudWindmillServiceV1Alpha1Grpc.newStub(get(serviceAddress));
    return serviceAddress.getKind() != WindmillServiceAddress.Kind.AUTHENTICATED_GCP_SERVICE_ADDRESS
        ? windmillServiceStub.withCallCredentials(
            MoreCallCredentials.from(new VendoredCredentialsAdapter(gcpCredentials)))
        : windmillServiceStub;
  }

  @Override
  public CloudWindmillMetadataServiceV1Alpha1Stub createWindmillMetadataServiceStub(
      WindmillServiceAddress serviceAddress) {
    return CloudWindmillMetadataServiceV1Alpha1Grpc.newStub(get(serviceAddress))
        .withCallCredentials(
            MoreCallCredentials.from(new VendoredCredentialsAdapter(gcpCredentials)));
  }

  private ManagedChannel createChannel(WindmillServiceAddress serviceAddress) {
    Supplier<ManagedChannel> channelFactory =
        () -> remoteChannel(serviceAddress, rpcChannelTimeoutSec);
    // IsolationChannel will create and manage separate RPC channels to the same serviceAddress via
    // calling the channelFactory, else just directly return the RPC channel.
    return useIsolatedChannels ? IsolationChannel.create(channelFactory) : channelFactory.get();
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.write("Active GRPC Channels:<br>");
    for (Map.Entry<WindmillServiceAddress, ManagedChannel> addressAndChannel :
        channelCache.entrySet()) {
      writer.format(
          "Address: [%s]; Channel: [%s]; ChannelState: [%s]",
          addressAndChannel.getKey(),
          addressAndChannel.getValue(),
          addressAndChannel.getValue().getState(false));
      writer.write("<br>");
    }
  }

  @Override
  public ManagedChannel get(WindmillServiceAddress windmillServiceAddress) {
    return channelCache.computeIfAbsent(windmillServiceAddress, this::createChannel);
  }

  @Override
  public void remove(WindmillServiceAddress windmillServiceAddress) {
    Optional.ofNullable(channelCache.remove(windmillServiceAddress))
        .ifPresent(
            channel -> {
              channel.shutdown();
              try {
                channel.awaitTermination(10, TimeUnit.SECONDS);
              } catch (InterruptedException e) {
                LOG.warn(
                    "Error occurred trying to shutdown channel for address={}, channel={}",
                    windmillServiceAddress,
                    channel,
                    e);
              }
              if (channel.isShutdown()) {
                channel.shutdownNow();
              }
            });
  }

  public ChannelCache asChannelCache() {
    return this;
  }
}
