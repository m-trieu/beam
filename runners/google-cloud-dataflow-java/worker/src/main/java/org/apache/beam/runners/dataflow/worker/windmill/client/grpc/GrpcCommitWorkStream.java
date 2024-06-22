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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.CommitStatus;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitRequestChunk;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitResponse;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.StreamingCommitWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItemCommitRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.AbstractWindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.observers.StreamObserverFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
@ThreadSafe
public final class GrpcCommitWorkStream
    extends AbstractWindmillStream<StreamingCommitWorkRequest, StreamingCommitResponse>
    implements CommitWorkStream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcCommitWorkStream.class);
  private static final long HEARTBEAT_REQUEST_ID = Long.MAX_VALUE;

  private final Map<Long, PendingRequest> pendingRequests;
  private final AtomicLong idGenerator;
  private final JobHeader jobHeader;
  private final ThrottleTimer commitWorkThrottleTimer;
  private final int streamingRpcBatchLimit;

  private GrpcCommitWorkStream(
      String streamId,
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer commitWorkThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit) {
    super(
        startCommitWorkRpcFn,
        backoff,
        streamObserverFactory,
        streamRegistry,
        logEveryNStreamFailures,
        streamId);
    pendingRequests = new ConcurrentHashMap<>();
    this.idGenerator = idGenerator;
    this.jobHeader = jobHeader;
    this.commitWorkThrottleTimer = commitWorkThrottleTimer;
    this.streamingRpcBatchLimit = streamingRpcBatchLimit;
  }

  public static GrpcCommitWorkStream create(
      String streamId,
      Function<StreamObserver<StreamingCommitResponse>, StreamObserver<StreamingCommitWorkRequest>>
          startCommitWorkRpcFn,
      BackOff backoff,
      StreamObserverFactory streamObserverFactory,
      Set<AbstractWindmillStream<?, ?>> streamRegistry,
      int logEveryNStreamFailures,
      ThrottleTimer commitWorkThrottleTimer,
      JobHeader jobHeader,
      AtomicLong idGenerator,
      int streamingRpcBatchLimit) {
    GrpcCommitWorkStream commitWorkStream =
        new GrpcCommitWorkStream(
            "CommitWorkStream-" + streamId,
            startCommitWorkRpcFn,
            backoff,
            streamObserverFactory,
            streamRegistry,
            logEveryNStreamFailures,
            commitWorkThrottleTimer,
            jobHeader,
            idGenerator,
            streamingRpcBatchLimit);
    commitWorkStream.startStream();
    return commitWorkStream;
  }

  @Override
  public void appendSpecificHtml(PrintWriter writer) {
    writer.format("CommitWorkStream: %d pending", pendingRequests.size());
  }

  @Override
  protected synchronized void onNewStream() {
    send(StreamingCommitWorkRequest.newBuilder().setHeader(jobHeader).build());
    try (Batcher resendBatcher = new Batcher()) {
      for (Map.Entry<Long, PendingRequest> entry : pendingRequests.entrySet()) {
        if (!resendBatcher.canAccept(entry.getValue().getBytes())) {
          resendBatcher.flush();
        }
        resendBatcher.add(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Returns a builder that can be used for sending requests if the stream is not closed.
   *
   * @implNote Each builder is not thread-safe but different builders for the same stream may be
   *     used simultaneously.
   */
  @Override
  public Optional<RequestBatcher> newBatcher() {
    return isClosed() ? Optional.empty() : Optional.of(new Batcher());
  }

  @Override
  protected boolean hasPendingRequests() {
    return !pendingRequests.isEmpty();
  }

  @Override
  public void sendHealthCheck() {
    if (hasPendingRequests()) {
      StreamingCommitWorkRequest.Builder builder = StreamingCommitWorkRequest.newBuilder();
      builder.addCommitChunkBuilder().setRequestId(HEARTBEAT_REQUEST_ID);
      send(builder.build());
    }
  }

  @Override
  protected void onResponse(StreamingCommitResponse response) {
    commitWorkThrottleTimer.stop();
    @Nullable RuntimeException finalException = null;
    for (int i = 0; i < response.getRequestIdCount() && !isClosed(); ++i) {
      long requestId = response.getRequestId(i);
      if (requestId == HEARTBEAT_REQUEST_ID) {
        continue;
      }
      @Nullable PendingRequest pendingRequest = pendingRequests.remove(requestId);
      if (pendingRequest == null) {
        LOG.warn(
            "Got unknown commit request ID: [{}] in response: [{}]. Current pending requests: {}",
            requestId,
            response,
            pendingRequests);
      } else {
        try {
          CommitStatus commitStatus =
              i < response.getStatusCount() ? response.getStatus(i) : CommitStatus.OK;
          pendingRequest.ackResponse(commitStatus);
        } catch (RuntimeException e) {
          // Catch possible exceptions to ensure that an exception for one commit does not prevent
          // other commits from being processed.
          LOG.warn("Exception while processing commit response.", e);
          finalException = e;
        }
      }
    }
    if (finalException != null) {
      throw finalException;
    }
  }

  @Override
  protected void startThrottleTimer() {
    commitWorkThrottleTimer.start();
  }

  private void flushInternal(Map<Long, PendingRequest> requests) {
    boolean isClosed = isClosed();
    if (requests.isEmpty() || isClosed) {
      LOG.debug("Skipping commit stream flush. isClosed={}", isClosed);
      return;
    }

    if (requests.size() == 1) {
      Map.Entry<Long, PendingRequest> elem = requests.entrySet().iterator().next();
      if (elem.getValue().request.getSerializedSize()
          > AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        issueMultiChunkRequest(elem.getKey(), elem.getValue());
      } else {
        issueSingleRequest(elem.getKey(), elem.getValue());
      }
    } else {
      issueBatchedRequest(requests);
    }
  }

  private void issueSingleRequest(long id, PendingRequest pendingRequest) {
    StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
    requestBuilder
        .addCommitChunkBuilder()
        .setComputationId(pendingRequest.computation)
        .setRequestId(id)
        .setShardingKey(pendingRequest.request.getShardingKey())
        .setSerializedWorkItemCommit(pendingRequest.request.toByteString());
    StreamingCommitWorkRequest chunk = requestBuilder.build();
    synchronized (this) {
      pendingRequests.put(id, pendingRequest);
      try {
        send(chunk);
      } catch (IllegalStateException e) {
        // Stream was broken, request will be retried when stream is reopened.
      }
    }
  }

  private void issueBatchedRequest(Map<Long, PendingRequest> requests) {
    StreamingCommitWorkRequest.Builder requestBuilder = StreamingCommitWorkRequest.newBuilder();
    String lastComputation = null;
    for (Map.Entry<Long, PendingRequest> entry : requests.entrySet()) {
      PendingRequest request = entry.getValue();
      StreamingCommitRequestChunk.Builder chunkBuilder = requestBuilder.addCommitChunkBuilder();
      if (lastComputation == null || !lastComputation.equals(request.computation)) {
        chunkBuilder.setComputationId(request.computation);
        lastComputation = request.computation;
      }
      chunkBuilder.setRequestId(entry.getKey());
      chunkBuilder.setShardingKey(request.request.getShardingKey());
      chunkBuilder.setSerializedWorkItemCommit(request.request.toByteString());
    }
    StreamingCommitWorkRequest request = requestBuilder.build();
    synchronized (this) {
      pendingRequests.putAll(requests);
      try {
        send(request);
      } catch (IllegalStateException e) {
        // Stream was broken, request will be retried when stream is reopened.
      }
    }
  }

  private void issueMultiChunkRequest(long id, PendingRequest pendingRequest) {
    checkNotNull(pendingRequest.computation);
    final ByteString serializedCommit = pendingRequest.request.toByteString();

    synchronized (this) {
      pendingRequests.put(id, pendingRequest);
      for (int i = 0;
          i < serializedCommit.size();
          i += AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE) {
        int end = i + AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE;
        ByteString chunk = serializedCommit.substring(i, Math.min(end, serializedCommit.size()));

        StreamingCommitRequestChunk.Builder chunkBuilder =
            StreamingCommitRequestChunk.newBuilder()
                .setRequestId(id)
                .setSerializedWorkItemCommit(chunk)
                .setComputationId(pendingRequest.computation)
                .setShardingKey(pendingRequest.request.getShardingKey());
        int remaining = serializedCommit.size() - end;
        if (remaining > 0) {
          chunkBuilder.setRemainingBytesForWorkItem(remaining);
        }

        StreamingCommitWorkRequest requestChunk =
            StreamingCommitWorkRequest.newBuilder().addCommitChunk(chunkBuilder).build();
        try {
          send(requestChunk);
        } catch (IllegalStateException e) {
          // Stream was broken, request will be retried when stream is reopened.
          break;
        }
      }
    }
  }

  private static class PendingRequest {
    private final String computation;
    private final WorkItemCommitRequest request;
    private final Consumer<CommitStatus> onDone;

    PendingRequest(
        String computation, WorkItemCommitRequest request, Consumer<CommitStatus> onDone) {
      this.computation = computation;
      this.request = request;
      this.onDone = onDone;
    }

    long getBytes() {
      return (long) request.getSerializedSize() + computation.length();
    }

    private void ackResponse(CommitStatus status) {
      onDone.accept(status);
    }
  }

  private class Batcher implements CommitWorkStream.RequestBatcher {
    private final Map<Long, PendingRequest> queue;
    private long queuedBytes;

    private Batcher() {
      this.queuedBytes = 0;
      this.queue = new HashMap<>();
    }

    @Override
    public boolean commitWorkItem(
        String computation, WorkItemCommitRequest commitRequest, Consumer<CommitStatus> onDone) {
      if (!canAccept(commitRequest.getSerializedSize() + computation.length())) {
        return false;
      }
      PendingRequest request = new PendingRequest(computation, commitRequest, onDone);
      add(idGenerator.incrementAndGet(), request);
      return true;
    }

    /** Flushes any pending work items to the wire. */
    @Override
    public void flush() {
      flushInternal(queue);
      queuedBytes = 0;
      queue.clear();
    }

    void add(long id, PendingRequest request) {
      assert (canAccept(request.getBytes()));
      queuedBytes += request.getBytes();
      queue.put(id, request);
    }

    private boolean canAccept(long requestBytes) {
      return queue.isEmpty()
          || (queue.size() < streamingRpcBatchLimit
              && (requestBytes + queuedBytes) < AbstractWindmillStream.RPC_STREAM_CHUNK_SIZE);
    }
  }
}
