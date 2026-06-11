/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.java.impl;

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.EndTransactionRequest;
import apache.rocketmq.v2.EndTransactionResponse;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.NotifyClientTerminationResponse;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecallMessageRequest;
import apache.rocketmq.v2.RecallMessageResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.ReceiveMessageResponse;
import apache.rocketmq.v2.SendMessageRequest;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.TelemetryCommand;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExecutorServices;
import org.apache.rocketmq.client.java.misc.MetadataUtils;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.Context;
import org.apache.rocketmq.client.java.rpc.RpcClient;
import org.apache.rocketmq.client.java.rpc.RpcClientImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @see ClientManager
 */
public class ClientManagerImpl extends ClientManager {
    public static final Duration RPC_CLIENT_MAX_IDLE_DURATION = Duration.ofMinutes(30);

    public static final Duration RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY = Duration.ofSeconds(5);
    public static final Duration RPC_CLIENT_IDLE_CHECK_PERIOD = Duration.ofMinutes(1);

    public static final Duration HEART_BEAT_INITIAL_DELAY = Duration.ofSeconds(1);
    public static final Duration HEART_BEAT_PERIOD = Duration.ofSeconds(10);

    public static final Duration LOG_STATS_INITIAL_DELAY = Duration.ofSeconds(1);
    public static final Duration LOG_STATS_PERIOD = Duration.ofSeconds(60);

    public static final Duration SYNC_SETTINGS_DELAY = Duration.ofSeconds(1);
    public static final Duration SYNC_SETTINGS_PERIOD = Duration.ofMinutes(5);

    private static final Logger log = LoggerFactory.getLogger(ClientManagerImpl.class);

    private final Client client;
    // RpcClient 内部封装的 gRPC stub 会对 Endpoints 进行负载均衡
    // 默认会选择第一个可用的地址并在需要时重连到其他地址。
    @GuardedBy("rpcClientTableLock")
    private final Map<Endpoints, RpcClient> rpcClientTable;
    private final ReadWriteLock rpcClientTableLock;

    /**
     * In charge of all scheduled tasks.
     * 用于发送失败是，根据退避算法计算出的重试间隔，进行消息发送重试
     * 客户端相关的定时任务：定时发送心跳，定时清理idle client，定时更新 topic 路由
     */
    private final ScheduledExecutorService scheduler;// availableProcessors

    /**
     * Public executor for all async RPCs, <strong>should never submit a heavy task.</strong>
     */
    private final ExecutorService asyncWorker; // availableProcessors, 50000 队列

    public ClientManagerImpl(Client client) {
        this.client = client;
        // RpcClient 内部封装的 gRPC stub 会对 Endpoints 进行负载均衡
        // 默认会选择第一个可用的地址并在需要时重连到其他地址。
        // 这里封装客户端所需打交道的所有 endpoints (包括 nameServer, brokers)
        this.rpcClientTable = new HashMap<>();
        this.rpcClientTableLock = new ReentrantReadWriteLock();
        final long clientIndex = client.getClientId().getIndex();
        // 用于发送失败是，根据退避算法计算出的重试间隔，进行消息发送重试
        this.scheduler = new ScheduledThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            new ThreadFactoryImpl("ClientScheduler", clientIndex));
        // executor for all async RPCs
        this.asyncWorker = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(50000),
            new ThreadFactoryImpl("ClientAsyncWorker", clientIndex));
    }

    /**
     * It is well-founded that a {@link RpcClient} is deprecated if it is idle for a long time, so it is essential to
     * clear it.
     *
     * @throws InterruptedException if the thread has been interrupted
     */
    private void clearIdleRpcClients() throws InterruptedException {
        rpcClientTableLock.writeLock().lock();
        try {
            final Iterator<Map.Entry<Endpoints, RpcClient>> it = rpcClientTable.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Endpoints, RpcClient> entry = it.next();
                final Endpoints endpoints = entry.getKey();
                final RpcClient rpcClient = entry.getValue();

                final Duration idleDuration = rpcClient.idleDuration();
                // idleDuration 超过 30 分钟就关闭
                if (idleDuration.compareTo(RPC_CLIENT_MAX_IDLE_DURATION) > 0) {
                    it.remove();
                    rpcClient.shutdown();
                    log.info("Rpc client has been idle for a long time, endpoints={}, idleDuration={}, " +
                            "rpcClientMaxIdleDuration={}, clientId={}", endpoints, idleDuration,
                        RPC_CLIENT_MAX_IDLE_DURATION, client.getClientId());
                }
            }
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
    }

    /**
     * Obtain the RPC client by remote {@link Endpoints}, if it does not already exist, it will be created
     * automatically.
     *
     * @param endpoints remote endpoints.
     * @return RPC client.
     */
    private RpcClient getRpcClient(Endpoints endpoints) throws ClientException {
        RpcClient rpcClient;
        rpcClientTableLock.readLock().lock();
        try {
            rpcClient = rpcClientTable.get(endpoints);
            if (null != rpcClient) {
                return rpcClient;
            }
        } finally {
            rpcClientTableLock.readLock().unlock();
        }
        rpcClientTableLock.writeLock().lock();
        try {
            rpcClient = rpcClientTable.get(endpoints);
            if (null != rpcClient) {
                return rpcClient;
            }
            try {
                rpcClient = new RpcClientImpl(endpoints, client.isSslEnabled());
            } catch (SSLException e) {
                log.error("Failed to get RPC client, endpoints={}, clientId={}", endpoints, client.getClientId(), e);
                throw new ClientException("Failed to generate RPC client", e);
            }
            rpcClientTable.put(endpoints, rpcClient);
            return rpcClient;
        } finally {
            rpcClientTableLock.writeLock().unlock();
        }
    }

    @Override
    public RpcFuture<QueryRouteRequest, QueryRouteResponse> queryRoute(Endpoints endpoints, QueryRouteRequest request,
        Duration duration) {
        try {
            // gRPC 类封装客户端的相关元信息 (gRPC  headers) 会一起发送到 proxy 端
            // see: org.apache.rocketmq.proxy.grpc.GrpcServerBuilder#configInterceptor
            // see : org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<QueryRouteResponse> future = rpcClient.queryRoute(metadata, request, asyncWorker,
                duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<HeartbeatRequest, HeartbeatResponse> heartbeat(Endpoints endpoints, HeartbeatRequest request,
        Duration duration) {
        try {
            // gRPC 类封装客户端的相关元信息 (gRPC  headers) 会一起发送到 proxy 端
            // see: org.apache.rocketmq.proxy.grpc.GrpcServerBuilder#configInterceptor
            // see : org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            ListenableFuture<HeartbeatResponse> future = rpcClient.heartbeat(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<SendMessageRequest, SendMessageResponse> sendMessage(Endpoints endpoints,
        SendMessageRequest request, Duration duration) {
        try {
            // gRPC 类封装客户端的相关元信息 (gRPC  headers) 会一起发送到 proxy 端
            // see: org.apache.rocketmq.proxy.grpc.GrpcServerBuilder#configInterceptor
            // see : org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<SendMessageResponse> future =
                rpcClient.sendMessage(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<QueryAssignmentRequest, QueryAssignmentResponse> queryAssignment(Endpoints endpoints,
        QueryAssignmentRequest request, Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<QueryAssignmentResponse> future =
                rpcClient.queryAssignment(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<ReceiveMessageRequest, List<ReceiveMessageResponse>> receiveMessage(Endpoints endpoints,
        ReceiveMessageRequest request, Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            // duration : longPollingTimeout + RequestTimeout
            final ListenableFuture<List<ReceiveMessageResponse>> future =
                rpcClient.receiveMessage(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }
    // 向 reviveTopic 发送 ack 消息， tag 为 ACK_TAG， ack 之后的消息就不会复活了
    // 但是这里 ack 消息并不会推进原来 topic 对应 queue 的 commitOffset
    // pop 消息的 offset 由 broker 的 popMessageProcessor 负责推进，拉一批往前推一批
    // 由于是消费者组并发 pop,所以需要保证其他消费者可以 pop 的接下来的消息，offset 只能一直向前推
    // 而 ack pop message 只能保证它不会被重试
    @Override
    public RpcFuture<AckMessageRequest, AckMessageResponse> ackMessage(Endpoints endpoints, AckMessageRequest request,
        Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<AckMessageResponse> future =
                rpcClient.ackMessage(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }
    // 向 reviveTopic 添加一个新的 checkpoint , 修改它的 InvisibleTim
    // ack 原来的消息，防止原来的消息被重新投递，从而达到修改消息 InvisibleTim 的逻辑
    @Override
    public RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse>
    changeInvisibleDuration(Endpoints endpoints, ChangeInvisibleDurationRequest request,
        Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<ChangeInvisibleDurationResponse> future =
                rpcClient.changeInvisibleDuration(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse>
    forwardMessageToDeadLetterQueue(Endpoints endpoints, ForwardMessageToDeadLetterQueueRequest request,
        Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<ForwardMessageToDeadLetterQueueResponse> future =
                rpcClient.forwardMessageToDeadLetterQueue(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<EndTransactionRequest, EndTransactionResponse> endTransaction(Endpoints endpoints,
        EndTransactionRequest request, Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<EndTransactionResponse> future =
                rpcClient.endTransaction(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<NotifyClientTerminationRequest, NotifyClientTerminationResponse>
    notifyClientTermination(Endpoints endpoints, NotifyClientTerminationRequest request,
        Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<NotifyClientTerminationResponse> future =
                rpcClient.notifyClientTermination(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public RpcFuture<RecallMessageRequest, RecallMessageResponse> recallMessage(Endpoints endpoints,
        RecallMessageRequest request, Duration duration) {
        try {
            final Metadata metadata = client.sign();
            final Context context = new Context(endpoints, metadata);
            final RpcClient rpcClient = getRpcClient(endpoints);
            final ListenableFuture<RecallMessageResponse> future =
                rpcClient.recallMessage(metadata, request, asyncWorker, duration);
            return new RpcFuture<>(context, request, future);
        } catch (Throwable t) {
            return new RpcFuture<>(t);
        }
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints, Duration duration,
        StreamObserver<TelemetryCommand> responseObserver) throws ClientException {
        try {
            final Metadata metadata = client.sign();
            final RpcClient rpcClient = getRpcClient(endpoints);
            return rpcClient.telemetry(metadata, asyncWorker, duration, responseObserver);
        } catch (Throwable t) {
            throw new InternalErrorException(t);
        }
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return this.scheduler;
    }

    @Override
    protected void startUp() {
        // hostName@processId@index@System.nanoTime()
        final ClientId clientId = client.getClientId();
        log.info("Begin to start the client manager, clientId={}", clientId);
        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    // 每 1 分钟
                    clearIdleRpcClients();
                } catch (Throwable t) {
                    log.error("Exception raised during the clearing of idle rpc clients, clientId={}", clientId, t);
                }
            },
            RPC_CLIENT_IDLE_CHECK_INITIAL_DELAY.toNanos(),
            RPC_CLIENT_IDLE_CHECK_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );

        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    // 10s
                    client.doHeartbeat();
                } catch (Throwable t) {
                    log.error("Exception raised during heartbeat, clientId={}", clientId, t);
                }
            },
            HEART_BEAT_INITIAL_DELAY.toNanos(),
            HEART_BEAT_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );

        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    log.info("Start to log statistics, clientVersion={}, clientWrapperVersion={}, "
                            + "clientEndpoints={}, os description=[{}], java description=[{}], clientId={}",
                        MetadataUtils.getVersion(), MetadataUtils.getWrapperVersion(), client.getEndpoints(),
                        Utilities.getOsDescription(), Utilities.getJavaDescription(), clientId);
                    client.doStats();
                } catch (Throwable t) {
                    log.error("Exception raised during statistics logging, clientId={}", clientId, t);
                }
            },
            LOG_STATS_INITIAL_DELAY.toNanos(),
            LOG_STATS_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );
        // 向远端 broker 同步 producer, consumer 相关配置
        // 这里应该先 syncSettings 然后在启动定时任务
        // 因为后续创建 consumerService 的时候需要知道 topic 是否为 fifo(这个信息是通过 syncSetting 获取的)
        scheduler.scheduleWithFixedDelay(
            () -> {
                try {
                    // 每 5 分钟
                    // PublishingSettings
                    // see : org.apache.rocketmq.client.java.impl.producer.ProducerImpl.ProducerImpl
                    // 从远程 broker 获取到的 consumerGroup 订阅关系配置（由 admin 创建消费者组的时候在指定 broker 填充）
                    // 用远程 broker 中的配置填充 pushSubscriptionSettings
                    // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
                    // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
                    // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
                    // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
                    client.syncSettings();
                } catch (Throwable t) {
                    log.error("Exception raised during the setting synchronization, clientId={}", clientId, t);
                }
            },
            SYNC_SETTINGS_DELAY.toNanos(),
            SYNC_SETTINGS_PERIOD.toNanos(),
            TimeUnit.NANOSECONDS
        );
        log.info("The client manager starts successfully, clientId={}", clientId);
    }

    @Override
    protected void shutDown() throws IOException {
        final ClientId clientId = client.getClientId();
        log.info("Begin to shutdown the client manager, clientId={}", clientId);
        scheduler.shutdown();
        try {
            if (!ExecutorServices.awaitTerminated(scheduler)) {
                log.error("[Bug] Timeout to shutdown the client scheduler, clientId={}", clientId);
            } else {
                log.info("Shutdown the client scheduler successfully, clientId={}", clientId);
            }
            rpcClientTableLock.writeLock().lock();
            try {
                final Iterator<Map.Entry<Endpoints, RpcClient>> it = rpcClientTable.entrySet().iterator();
                while (it.hasNext()) {
                    final Map.Entry<Endpoints, RpcClient> entry = it.next();
                    final RpcClient rpcClient = entry.getValue();
                    it.remove();
                    rpcClient.shutdown();
                }
            } finally {
                rpcClientTableLock.writeLock().unlock();
            }
            log.info("Shutdown all rpc client(s) successfully, clientId={}", clientId);
            asyncWorker.shutdown();
            if (!ExecutorServices.awaitTerminated(asyncWorker)) {
                log.error("[Bug] Timeout to shutdown the client async worker, clientId={}", clientId);
            } else {
                log.info("Shutdown the client async worker successfully, clientId={}", clientId);
            }
        } catch (InterruptedException e) {
            log.error("[Bug] Unexpected exception raised while shutdown client manager, clientId={}", clientId, e);
            throw new IOException(e);
        }
        log.info("Shutdown the client manager successfully, clientId={}", clientId);
    }

    @SuppressWarnings("NullableProblems")
    @Override
    protected String serviceName() {
        return super.serviceName() + "-" + client.getClientId().getIndex();
    }
}
