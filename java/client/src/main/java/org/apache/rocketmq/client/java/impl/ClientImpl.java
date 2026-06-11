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

import static com.google.common.base.Preconditions.checkNotNull;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.HeartbeatResponse;
import apache.rocketmq.v2.MessageQueue;
import apache.rocketmq.v2.NotifyClientTerminationRequest;
import apache.rocketmq.v2.PrintThreadStackTraceCommand;
import apache.rocketmq.v2.QueryRouteRequest;
import apache.rocketmq.v2.QueryRouteResponse;
import apache.rocketmq.v2.RecoverOrphanedTransactionCommand;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.ThreadStackTrace;
import apache.rocketmq.v2.VerifyMessageCommand;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.common.base.Function;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.exception.InternalErrorException;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.hook.CompositedMessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.impl.producer.ClientSessionHandler;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.metrics.ClientMeterManager;
import org.apache.rocketmq.client.java.metrics.MessageMeterInterceptor;
import org.apache.rocketmq.client.java.metrics.Metric;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExecutorServices;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.apache.rocketmq.client.java.rpc.Signature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractIdleService 帮助你专注于服务的启动 (startUp) 和关闭 (shutDown) 逻辑
 *
 * 启动 (startUp) 与关闭 (shutDown) 的异步执行：
 *
 * startUp() 和 shutDown() 方法是 protected abstract 的，你需要在这两个方法里编写实际的业务逻辑。
 *
 * 这两个方法各自在一个独立的线程中运行，这意味着任何耗时的初始化或资源清理工作都不会阻塞调用 startAsync() 或 stopAsync() 的主线程
 * */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
public abstract class ClientImpl extends AbstractIdleService implements Client, ClientSessionHandler,
    MessageInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ClientImpl.class);
    /**
     * The telemetry timeout should not be too long, otherwise
     * <a href="https://github.com/grpc/grpc-java/issues/7351">this issue</a> may be triggered in JDK8 + macOS.
     */
    private static final Duration TELEMETRY_TIMEOUT = Duration.ofDays(60 * 365);

    protected final ClientConfiguration clientConfiguration;
    // 将配置中指定的 endPoints 字符串转换为 addresses 类型
    protected final Endpoints endpoints;
    protected final Set<String> topics;
    // Thread-safe set.存储所有发送失败的 endpoint
    // Isolate endpoints because of sending failure.
    protected final Set<Endpoints> isolated;
    // 执行消息发送 future 的 callback
    protected final ExecutorService clientCallbackExecutor;
    protected final ClientMeterManager clientMeterManager;
    /**
     * Telemetry command executor, which aims to execute commands from the remote.
     */
    protected final ThreadPoolExecutor telemetryCommandExecutor;
    // hostName@processId@index@System.nanoTime()
    protected final ClientId clientId;
    // 封装客户端的各种 message 操作
    private final ClientManager clientManager;
    private volatile ScheduledFuture<?> updateRouteCacheFuture;
    // topic 下所有副本集下的所有 broker（包括主从） 拥有的所有 messageQueue(包括各种权限)
    private final ConcurrentMap<String, TopicRouteData> topicRouteCache;
    // topic 下正在获取 topic 路由的 future
    @GuardedBy("inflightRouteFutureLock")
    private final Map<String /* topic */, Set<SettableFuture<TopicRouteData>>> inflightRouteFutureTable;
    private final Lock inflightRouteFutureLock;

    @GuardedBy("sessionsLock")
    private final Map<Endpoints, ClientSessionImpl> sessionsTable;
    private final ReadWriteLock sessionsLock;
    // push consumer : inflightRequestCountInterceptor
    private final CompositedMessageInterceptor compositedMessageInterceptor;

    public ClientImpl(ClientConfiguration clientConfiguration, Set<String> topics) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        // localhost:8081,proxy grpcServer 监听 8081
        // 将配置中指定的 endPoints 字符串转换为 addresses 类型
        this.endpoints = new Endpoints(clientConfiguration.getEndpoints());
        // 用于预取 topic 路由
        this.topics = topics;
        // Generate client id firstly.
        // hostName@processId@index@System.nanoTime()
        this.clientId = new ClientId();
        // topic 路由
        this.topicRouteCache = new ConcurrentHashMap<>();
        // 正在请求的 topic 路由 future
        this.inflightRouteFutureTable = new ConcurrentHashMap<>();
        this.inflightRouteFutureLock = new ReentrantLock();

        this.sessionsTable = new HashMap<>();
        this.sessionsLock = new ReentrantReadWriteLock();
        // Thread-safe set.存储所有发送失败的 endpoint
        // Isolate endpoints because of sending failure.
        this.isolated = Collections.newSetFromMap(new ConcurrentHashMap<>());

        this.clientManager = new ClientManagerImpl(this);

        final long clientIdIndex = clientId.getIndex();
        // 执行消息发送 future 的 callback
        this.clientCallbackExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("ClientCallbackWorker", clientIdIndex));

        this.clientMeterManager = new ClientMeterManager(clientId, clientConfiguration);

        this.compositedMessageInterceptor =
            new CompositedMessageInterceptor(Collections.singletonList(new MessageMeterInterceptor(this,
                clientMeterManager)));

        this.telemetryCommandExecutor = new ThreadPoolExecutor(
            1,
            1,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("CommandExecutor", clientIdIndex));
    }


    /**
     * Start the rocketmq client and do some preparatory work.
     */
    @Override
    protected void startUp() throws Exception {
        log.info("Begin to start the rocketmq client, clientId={}", clientId);
        // 回调 org.apache.rocketmq.client.java.impl.ClientManagerImpl.startUp
        // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
        // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
        // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
        // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
        this.clientManager.startAsync().awaitRunning();
        // Fetch topic route from remote.
        log.info("Begin to fetch topic(s) route data from remote during client startup, clientId={}, topics={}",
            clientId, topics);
        // topic 路由预取
        for (String topic : topics) {
            // 从 name server 中获取 topic 下所有副本集下所有 broker (包括主从) 下的所有 messageQueue(所有权限)
            // 更新 updatePublishingLoadBalancer 过滤出 topic 下的 master broker 下的 writable messageQueue
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            future.get();
        }
        log.info("Fetch topic route data from remote successfully during startup, clientId={}, topics={}",
            clientId, topics);
        // Update route cache periodically.
        final ScheduledExecutorService scheduler = clientManager.getScheduler();
        this.updateRouteCacheFuture = scheduler.scheduleWithFixedDelay(() -> {
            try {
                // 每隔 10s 获取 topic 路由
                updateRouteCache();
            } catch (Throwable t) {
                log.error("Exception raised while updating topic route cache, clientId={}", clientId, t);
            }
        }, 10, 30, TimeUnit.SECONDS);
        log.info("The rocketmq client starts successfully, clientId={}", clientId);
    }

    /**
     * Shutdown the rocketmq client and release related resources.
     */
    @Override
    protected void shutDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq client, clientId={}", clientId);
        notifyClientTermination();
        if (null != this.updateRouteCacheFuture) {
            updateRouteCacheFuture.cancel(false);
        }
        telemetryCommandExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(telemetryCommandExecutor)) {
            log.error("[Bug] Timeout to shutdown the telemetry command executor, clientId={}", clientId);
        } else {
            log.info("Shutdown the telemetry command executor successfully, clientId={}", clientId);
        }
        log.info("Begin to release all telemetry sessions, clientId={}", clientId);
        releaseClientSessions();
        log.info("Release all telemetry sessions successfully, clientId={}", clientId);
        clientManager.stopAsync().awaitTerminated();
        clientCallbackExecutor.shutdown();
        if (!ExecutorServices.awaitTerminated(clientCallbackExecutor)) {
            log.error("[Bug] Timeout to shutdown the client callback executor, clientId={}", clientId);
        }
        clientMeterManager.shutdown();
        log.info("Shutdown the rocketmq client successfully, clientId={}", clientId);
    }

    protected void addMessageInterceptor(MessageInterceptor messageInterceptor) {
        if (!this.isRunning()) {
            // inflightRequestCountInterceptor
            compositedMessageInterceptor.addInterceptor(messageInterceptor);
        }
    }

    @Override
    public void doBefore(MessageInterceptorContext context, List<GeneralMessage> generalMessages) {
        try {
            // inflightRequestCountInterceptor
            compositedMessageInterceptor.doBefore(context, generalMessages);
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while handling messages, clientId={}", clientId, t);
        }
    }

    @Override
    public void doAfter(MessageInterceptorContext context, List<GeneralMessage> generalMessages) {
        try {
            compositedMessageInterceptor.doAfter(context, generalMessages);
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while handling messages, clientId={}", clientId, t);
        }
    }

    @Override
    public TelemetryCommand settingsCommand() {
        final apache.rocketmq.v2.Settings settings = this.getSettings().toProtobuf();
        return TelemetryCommand.newBuilder().setSettings(settings).build();
    }

    @Override
    public StreamObserver<TelemetryCommand> telemetry(Endpoints endpoints,
        StreamObserver<TelemetryCommand> observer) throws ClientException {
        try {
            // 双向流
            // rpc Telemetry(stream TelemetryCommand) returns (stream TelemetryCommand) {}
            return clientManager.telemetry(endpoints, TELEMETRY_TIMEOUT, observer);
        } catch (ClientException e) {
            throw e;
        } catch (Throwable t) {
            throw new InternalErrorException(t);
        }
    }

    @Override
    public boolean isEndpointsDeprecated(Endpoints endpoints) {
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        return !totalRouteEndpoints.contains(endpoints);
    }

    /**
     * This method is invoked while request of printing thread stack trace is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of printing thread stack trace from remote.
     */
    @Override
    public void onPrintThreadStackTraceCommand(Endpoints endpoints, PrintThreadStackTraceCommand command) {
        final String nonce = command.getNonce();
        Runnable task = () -> {
            try {
                final String stackTrace = Utilities.stackTrace();
                Status status = Status.newBuilder().setCode(Code.OK).build();
                ThreadStackTrace threadStackTrace = ThreadStackTrace.newBuilder().setThreadStackTrace(stackTrace)
                    .setNonce(command.getNonce()).build();
                TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder()
                    .setThreadStackTrace(threadStackTrace)
                    .setStatus(status)
                    .build();
                telemetry(endpoints, telemetryCommand);
            } catch (Throwable t) {
                log.error("Failed to send thread stack trace to remote, endpoints={}, nonce={}, clientId={}",
                    endpoints, nonce, clientId, t);
            }
        };
        try {
            telemetryCommandExecutor.submit(task);
        } catch (Throwable t) {
            log.error("[Bug] Exception raised while submitting task to print thread stack trace, endpoints={}, "
                + "nonce={}, clientId={}", endpoints, nonce, clientId, t);
        }
    }

    public abstract Settings getSettings();

    /**
     * Apply setting from remote.
     *
     * @param endpoints remote endpoints.
     * @param settings  settings received from remote.
     */
    // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
    // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
    // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
    // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
    @Override
    public final void onSettingsCommand(Endpoints endpoints, apache.rocketmq.v2.Settings settings) {
        final Metric metric = new Metric(settings.getMetric());
        clientMeterManager.reset(metric);
        // 从远程 broker 获取到的 consumerGroup 订阅关系配置（由 admin 创建消费者组的时候在指定 broker 填充）
        // org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager#mergeSubscriptionData(apache.rocketmq.v2.Settings, org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig)
        // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
        // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
        // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
        // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定

        // 每个 proxy 的 setting 响应都会调用到这里
        this.getSettings().sync(settings);
    }

    /**
     * @see Client#syncSettings()
     */
    // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
    // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
    // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
    // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
    @Override
    public void syncSettings() {
        // procuder : PublishingSettings
        // see : org.apache.rocketmq.client.java.impl.producer.ProducerImpl.ProducerImpl

        // pushConsumer : pushSubscriptionSettings 本地消费者指定的订阅配置
        final apache.rocketmq.v2.Settings settings = getSettings().toProtobuf();
        final TelemetryCommand command = TelemetryCommand.newBuilder().setSettings(settings).build();
        // 获取订阅的 topic 路由中所有 messageQueue 所在 broker 的 endpoints
        final Set<Endpoints> totalRouteEndpoints = getTotalRouteEndpoints();
        // 从远程 broker 获取到的 consumerGroup 订阅关系配置（由 admin 创建消费者组的时候在指定 broker 填充）
        // 用远程 broker 中的配置填充 pushSubscriptionSettings
        // 这里的 endpoints 其实是所有 proxy 的 endpoints，挨个调用 proxy 的目的就是填充每个 proxy 关于客户端 setting 的缓存
        // 而 proxy 向 broker 获取消费者组配置 —— subcriptionGroupConfig 时，是在集群中随机选取一个副本集
        // 然后向这个随机副本集中的 master 获取 subcriptionGroupConfig
        for (Endpoints endpoints : totalRouteEndpoints) {
            try {
                // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
                // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
                // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
                // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
                telemetry(endpoints, command);
            } catch (Throwable t) {
                log.error("Failed to telemeter settings, clientId={}, endpoints={}", clientId, endpoints, t);
            }
        }
    }

    public void telemetry(Endpoints endpoints, TelemetryCommand command) {
        try {
            // 因为 telemetry 定义的是双向流，所以 gRPC 请求方式和之前的 unary rpc 不同
            // 这里用 ClientSessionImpl 来封装双向流的 request , response StreamObserver
            final ClientSessionImpl clientSession = getClientSession(endpoints);
            // 用 requestStreamObserver 发送 TelemetryCommand
            clientSession.write(command);
        } catch (Throwable t) {
            log.error("Failed to fire write telemetry command, clientId={}, endpoints={}", clientId, endpoints, t);
        }
    }

    private void releaseClientSessions() {
        sessionsLock.readLock().lock();
        try {
            sessionsTable.values().forEach(ClientSessionImpl::release);
        } finally {
            sessionsLock.readLock().unlock();
        }
    }

    public void removeClientSession(Endpoints endpoints, ClientSessionImpl clientSession) {
        sessionsLock.writeLock().lock();
        try {
            log.info("Remove client session, clientId={}, endpoints={}", clientId, endpoints);
            sessionsTable.remove(endpoints, clientSession);
        } finally {
            sessionsLock.writeLock().unlock();
        }
    }

    private ClientSessionImpl getClientSession(Endpoints endpoints) throws ClientException {
        sessionsLock.readLock().lock();
        try {
            final ClientSessionImpl session = sessionsTable.get(endpoints);
            if (null != session) {
                return session;
            }
        } finally {
            sessionsLock.readLock().unlock();
        }
        sessionsLock.writeLock().lock();
        try {
            ClientSessionImpl session = sessionsTable.get(endpoints);
            if (null != session) {
                return session;
            }
            // 里边封装了需要发送数据的 requestObserver
            session = new ClientSessionImpl(this, clientConfiguration.getRequestTimeout(), endpoints);
            sessionsTable.put(endpoints, session);
            return session;
        } finally {
            sessionsLock.writeLock().unlock();
        }
    }

    /**
     * Triggered when {@link TopicRouteData} is fetched from remote.
     */
    public ListenableFuture<TopicRouteData> onTopicRouteDataFetched(String topic,
    TopicRouteData topicRouteData) throws ClientException {
        // proxy address list
        final Set<Endpoints> routeEndpoints = topicRouteData
            .getMessageQueues().stream()
            .map(mq -> mq.getBroker().getEndpoints())
            .collect(Collectors.toSet());
        final Set<Endpoints> existRouteEndpoints = getTotalRouteEndpoints();
        final Set<Endpoints> newEndpoints = new HashSet<>(Sets.difference(routeEndpoints, existRouteEndpoints));
        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (Endpoints endpoints : newEndpoints) {
            final ClientSessionImpl clientSession = getClientSession(endpoints);
            futures.add(clientSession.syncSettings());
        }
        final ListenableFuture<?> future = Futures.allAsList(futures);
        return Futures.transform(future, (Function<Object, TopicRouteData>) input -> {
            topicRouteCache.put(topic, topicRouteData);
            // 从 topic 下所有副本集中过滤出 master broker,并且 messageQueue 是 writable
            onTopicRouteDataUpdate0(topic, topicRouteData);
            return topicRouteData;
        }, MoreExecutors.directExecutor());
    }

    public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
    }

    /**
     * This method is invoked while request of message consume verification is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of message consume verification from remote.
     */
    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand command) {
        log.warn("Ignore verify message command from remote, which is not expected, clientId={}, command={}",
            clientId, command);
        final String nonce = command.getNonce();
        final Status status = Status.newBuilder().setCode(Code.NOT_IMPLEMENTED).build();
        VerifyMessageResult verifyMessageResult = VerifyMessageResult.newBuilder().setNonce(nonce).build();
        TelemetryCommand telemetryCommand = TelemetryCommand.newBuilder()
            .setVerifyMessageResult(verifyMessageResult)
            .setStatus(status)
            .build();
        try {
            telemetry(endpoints, telemetryCommand);
        } catch (Throwable t) {
            log.warn("Failed to send message verification result, clientId={}", clientId, t);
        }
    }

    /**
     * This method is invoked while request of orphaned transaction recovery is received from remote.
     *
     * @param endpoints remote endpoints.
     * @param command   request of orphaned transaction recovery from remote.
     */
    @Override
    public void onRecoverOrphanedTransactionCommand(Endpoints endpoints, RecoverOrphanedTransactionCommand command) {
        log.warn("Ignore orphaned transaction recovery command from remote, which is not expected, clientId={}, "
            + "command={}", clientId, command);
    }

    private void updateRouteCache() {
        log.info("Start to update route cache for a new round, clientId={}", clientId);
        topicRouteCache.keySet().forEach(topic -> {
            // 从 name server 中获取 topic 下所有副本集下所有 broker (包括主从) 下的所有 messageQueue(所有权限)
            // 更新 updatePublishingLoadBalancer 过滤出 topic 下的 master broker 下的 writable messageQueue
            final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
            Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
                @Override
                public void onSuccess(TopicRouteData topicRouteData) {
                }

                @Override
                public void onFailure(Throwable t) {
                    log.error("Failed to fetch topic route for update cache, topic={}, clientId={}", topic,
                        clientId, t);
                }
            }, MoreExecutors.directExecutor());
        });
    }

    /**
     * Wrap notify client termination request.
     */
    public abstract NotifyClientTerminationRequest wrapNotifyClientTerminationRequest();

    /**
     * Notify remote that current client is prepared to be terminated.
     */
    private void notifyClientTermination() {
        log.info("Notify remote that client is terminated, clientId={}", clientId);
        final Set<Endpoints> routeEndpointsSet = getTotalRouteEndpoints();
        final NotifyClientTerminationRequest notifyClientTerminationRequest = wrapNotifyClientTerminationRequest();
        try {
            for (Endpoints endpoints : routeEndpointsSet) {
                clientManager.notifyClientTermination(endpoints, notifyClientTerminationRequest,
                    clientConfiguration.getRequestTimeout());
            }
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while notifying client's termination, clientId={}", clientId, t);
        }
    }

    public ClientManager getClientManager() {
        return clientManager;
    }

    @Override
    public Endpoints getEndpoints() {
        return endpoints;
    }

    /**
     * @see Client#getClientId()
     */
    @Override
    public ClientId getClientId() {
        return clientId;
    }

    /**
     * @see Client#doHeartbeat()
     */
    @Override
    public void doHeartbeat() {
        // 获取 topic 路由中所有 messageQueue 所在 broker 的 endpoints(这里对应的依然是 proxy endpoints 其实，由 proxy 转发到具体的 broker)
        final Set<Endpoints> totalEndpoints = getTotalRouteEndpoints();
        // gRPC HeartbeatRequest
        final HeartbeatRequest request = wrapHeartbeatRequest();
        for (Endpoints endpoints : totalEndpoints) {
            doHeartbeat(request, endpoints);
        }
    }

    /**
     * Real-time signature generation
     */
    // gRPC 类封装客户端的相关元信息 (gRPC  headers) 会一起发送到 proxy 端
    // see: org.apache.rocketmq.proxy.grpc.GrpcServerBuilder#configInterceptor
    // see : org.apache.rocketmq.proxy.grpc.pipeline.ContextInitPipeline
    @Override
    public Metadata sign() throws NoSuchAlgorithmException, InvalidKeyException {
        return Signature.sign(clientConfiguration, clientId);
    }

    @Override
    public boolean isSslEnabled() {
        return clientConfiguration.isSslEnabled();
    }

    /**
     * Send heartbeat data to the appointed endpoint
     *
     * @param request   heartbeat data request
     * @param endpoints endpoint to send heartbeat data
     */
    private void doHeartbeat(HeartbeatRequest request, final Endpoints endpoints) {
        try {
            final RpcFuture<HeartbeatRequest, HeartbeatResponse> future = clientManager.heartbeat(endpoints,
                request, clientConfiguration.getRequestTimeout());
            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    final Status status = response.getStatus();
                    final Code code = status.getCode();
                    if (Code.OK != code) {
                        log.warn("Failed to send heartbeat, code={}, status message=[{}], endpoints={}, clientId={}",
                            code, status.getMessage(), endpoints, clientId);
                        return;
                    }
                    log.info("Send heartbeat successfully, endpoints={}, clientId={}", endpoints, clientId);
                    // Thread-safe set.存储所有发送失败的 endpoint
                    // Isolate endpoints because of sending failure.
                    // 心跳消息发送成功了，就从 isolated 集合中删除 endpoints（有效了，不需要隔离）
                    final boolean removed = isolated.remove(endpoints);
                    if (removed) {
                        log.info("Rejoin endpoints which is isolated before, clientId={}, endpoints={}", clientId,
                            endpoints);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("Failed to send heartbeat, endpoints={}, clientId={}", endpoints, clientId, t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Throwable t) {
            // Should never reach here.
            log.error("[Bug] Exception raised while preparing heartbeat, endpoints={}, clientId={}", endpoints,
                clientId, t);
        }
    }

    /**
     * Wrap heartbeat request
     */
    public abstract HeartbeatRequest wrapHeartbeatRequest();

    /**
     * @see Client#doStats()
     */
    @Override
    public void doStats() {
    }

    private ListenableFuture<TopicRouteData> fetchTopicRoute(final String topic) {
        // 向 name server 查询 topic 下所有副本集中所有 broker(主从全包括)下的所有 messageQueue
        // 用 TopicRouteData 封装
        final ListenableFuture<TopicRouteData> future0 = fetchTopicRoute0(topic);
        final ListenableFuture<TopicRouteData> future = Futures.transformAsync(future0,
            // updatePublishingLoadBalancer
            topicRouteData -> onTopicRouteDataFetched(topic, topicRouteData), MoreExecutors.directExecutor());
        Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
            @Override
            public void onSuccess(TopicRouteData topicRouteData) {
                log.info("Fetch topic route successfully, clientId={}, topic={}, topicRouteData={}", clientId,
                    topic, topicRouteData);
            }

            @Override
            public void onFailure(Throwable t) {
                log.error("Failed to fetch topic route, clientId={}, topic={}", clientId, topic, t);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    protected ListenableFuture<TopicRouteData> fetchTopicRoute0(final String topic) {
        Resource topicResource = Resource.newBuilder()
            .setResourceNamespace(clientConfiguration.getNamespace())
            .setName(topic)
            .build();
        final QueryRouteRequest request = QueryRouteRequest.newBuilder().setTopic(topicResource)
            .setEndpoints(endpoints.toProtobuf()).build();
        // 向 name server 查询 topic 下所有副本集中所有 broker(主从全包括)下的所有 messageQueue
        final RpcFuture<QueryRouteRequest, QueryRouteResponse> future =
            clientManager.queryRoute(endpoints, request, clientConfiguration.getRequestTimeout());
        return Futures.transformAsync(future, response -> {
            final Status status = response.getStatus();
            StatusChecker.check(status, future);
            // topic 下所有副本集中所有 broker(主从全包括)下的所有 messageQueue
            final List<MessageQueue> messageQueuesList = response.getMessageQueuesList();
            // gRpc MessageQueue list 转换为 TopicRouteData 中的 MessageQueueImpl list
            final TopicRouteData topicRouteData = new TopicRouteData(messageQueuesList);
            return Futures.immediateFuture(topicRouteData);
        }, MoreExecutors.directExecutor());
    }
    // 获取 topic 路由中所有 messageQueue 所在 broker 的 endpoints(这里对应的依然是 proxy endpoints 其实，由 proxy 转发到具体的 broker)
    protected Set<Endpoints> getTotalRouteEndpoints() {
        Set<Endpoints> totalRouteEndpoints = new HashSet<>();
        for (TopicRouteData topicRouteData : topicRouteCache.values()) {
            totalRouteEndpoints.addAll(topicRouteData.getTotalEndpoints());
        }
        return totalRouteEndpoints;
    }

    protected ListenableFuture<TopicRouteData> getRouteData(final String topic) {
        SettableFuture<TopicRouteData> future0 = SettableFuture.create();
        TopicRouteData topicRouteData = topicRouteCache.get(topic);
        // If route result was cached before, get it directly.
        if (null != topicRouteData) {
            future0.set(topicRouteData);
            return future0;
        }
        inflightRouteFutureLock.lock();
        try {
            // If route was fetched by last in-flight request, get it directly.
            topicRouteData = topicRouteCache.get(topic);
            if (null != topicRouteData) {
                future0.set(topicRouteData);
                return future0;
            }
            // topic 下正在获取 topic 路由的 future
            Set<SettableFuture<TopicRouteData>> inflightFutures = inflightRouteFutureTable.get(topic);
            // Request is in-flight, return future directly.
            if (null != inflightFutures) {
                inflightFutures.add(future0);
                return future0;
            }
            inflightFutures = new HashSet<>();
            inflightFutures.add(future0);
            inflightRouteFutureTable.put(topic, inflightFutures);
        } finally {
            inflightRouteFutureLock.unlock();
        }
        // 从 name server 中获取 topic 下所有副本集下所有 broker (包括主从) 下的所有 messageQueue(所有权限)
        // 更新 updatePublishingLoadBalancer 过滤出 topic 下的 master broker 下的 writable messageQueue
        final ListenableFuture<TopicRouteData> future = fetchTopicRoute(topic);
        Futures.addCallback(future, new FutureCallback<TopicRouteData>() {
            @Override
            public void onSuccess(TopicRouteData topicRouteData) {
                inflightRouteFutureLock.lock();
                try {
                    // 获取路由成功，通知所有正在等待路由的 future
                    final Set<SettableFuture<TopicRouteData>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic,
                            clientId);
                        return;
                    }
                    log.debug("Fetch topic route successfully, topic={}, in-flight route future "
                        + "size={}, clientId={}", topic, newFutureSet.size(), clientId);
                    for (SettableFuture<TopicRouteData> newFuture : newFutureSet) {
                        newFuture.set(topicRouteData);
                    }
                } catch (Throwable t) {
                    // Should never reach here.
                    log.error("[Bug] Exception raised while update route data, topic={}, clientId={}", topic,
                        clientId, t);
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }

            @Override
            public void onFailure(Throwable t) {
                inflightRouteFutureLock.lock();
                try {
                    final Set<SettableFuture<TopicRouteData>> newFutureSet =
                        inflightRouteFutureTable.remove(topic);
                    if (null == newFutureSet) {
                        // Should never reach here.
                        log.error("[Bug] in-flight route futures was empty, topic={}, clientId={}", topic, clientId);
                        return;
                    }
                    log.debug("Failed to fetch topic route, topic={}, in-flight route future " +
                        "size={}, clientId={}", topic, newFutureSet.size(), clientId, t);
                    for (SettableFuture<TopicRouteData> future : newFutureSet) {
                        future.setException(t);
                    }
                } finally {
                    inflightRouteFutureLock.unlock();
                }
            }
        }, MoreExecutors.directExecutor());
        return future0;
    }

    public ScheduledExecutorService getScheduler() {
        return clientManager.getScheduler();
    }

    protected <T> T handleClientFuture(ListenableFuture<T> future) throws ClientException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ClientException) {
                throw (ClientException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new ClientException(null == cause ? e : cause);
        }
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    @Override
    protected String serviceName() {
        return super.serviceName() + "-" + clientId.getIndex();
    }
}
