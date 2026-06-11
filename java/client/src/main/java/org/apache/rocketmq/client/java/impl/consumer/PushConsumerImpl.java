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

package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.HeartbeatRequest;
import apache.rocketmq.v2.QueryAssignmentRequest;
import apache.rocketmq.v2.QueryAssignmentResponse;
import apache.rocketmq.v2.Status;
import apache.rocketmq.v2.TelemetryCommand;
import apache.rocketmq.v2.VerifyMessageCommand;
import apache.rocketmq.v2.VerifyMessageResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.java.exception.StatusChecker;
import org.apache.rocketmq.client.java.hook.InflightRequestCountInterceptor;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContext;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.metrics.GaugeObserver;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.misc.ExecutorServices;
import org.apache.rocketmq.client.java.misc.ThreadFactoryImpl;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link PushConsumer}
 *
 * <p>It is worth noting that in the implementation of push consumer, the message is not actively pushed by the server
 * to the client, but is obtained by the client actively going to the server.
 *
 * @see PushConsumer
 */
@SuppressWarnings({"UnstableApiUsage", "NullableProblems"})
class PushConsumerImpl extends ConsumerImpl implements PushConsumer {
    private static final Logger log = LoggerFactory.getLogger(PushConsumerImpl.class);
    // 成功消费消息总数
    final AtomicLong consumptionOkQuantity;
    // 消息消费失败总数
    final AtomicLong consumptionErrorQuantity;

    private final ClientConfiguration clientConfiguration;
    private final PushSubscriptionSettings pushSubscriptionSettings;
    private final String consumerGroup;
    // 消费者组订阅的所有 topic 以及对应的 FilterExpression
    private final Map<String /* topic */, FilterExpression> subscriptionExpressions;
    // 向 proxy 获取 topic 所在副本集中的所有可读 queue
    // 如果是 fifo 则收集所有副本集中的所有可读 queue
    // 非 fifo 则每个副本集只收集一个 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
    private final ConcurrentMap<String /* topic */, Assignments> cacheAssignments;
    // 由 consumptionExecutor 并发执行
    // 注意这里是由 push consumer sdk 内部的 20 个 consume 线程并发调用 MessageListener 的
    // 消息 pop 下来之后会一个一个的提交给这 20 个线程并发执行
    // FIFO 消息是提交一个执行完之后，在向提交第二个，一个一个的提交执行
    private final MessageListener messageListener;
    // 1024 , 这个是缓存消息的总量（针对所有 messageQueue(所有订阅 topic)）
    private final int maxCacheMessageCount;
    // 64M
    private final int maxCacheMessageSizeInBytes;
    // false
    private final boolean enableFifoConsumeAccelerator;
    private final InflightRequestCountInterceptor inflightRequestCountInterceptor;

    /**
     * Indicates the times of message reception.
     */
    private final AtomicLong receptionTimes;
    /**
     * Indicates the quantity of received messages.
     * consumer 接收到消息的总数
     */
    private final AtomicLong receivedMessagesQuantity;
    // 20 线程
    private final ThreadPoolExecutor consumptionExecutor;
    // 缓存订阅的 topic 下所有的有效 messageQueue(所有订阅 topic)
    // 这里缓存的是所有订阅 topic 的可读 message queue
    // 如果是 fifo 则收集所有副本集中的所有可读 queue
    // 非 fifo 则每个副本集只收集一个可读 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
    private final ConcurrentMap<MessageQueueImpl, ProcessQueue> processQueueTable;
    // FifoConsumeService or StandardConsumeService ?
    /**
     * 这里的 FIFO 标识是在 admin 创建消费者组 SubscriptionGroup 时指定的，保存在 broker 中
     * 消费者启动的时候会去 broker 拉取 SubscriptionGroup 相关配置
     * 只要在 admin 指定了 FIFO, 那么 broker 端的拉取以及这里的消息消费逻辑均是 FIFO, 无论你订阅的事 normalTopic 还是延时，事务 topic
     * admin 指定非 FIFO ,那么 broker 端的拉取以及这里的消息消费逻辑均是非 FIFO ，即使你订阅的是 FIFO topic
     * 因此一个 SubscriptionGroup 不能同时订阅 FIFO TOPIC 和其他类型 topic
     * 其实更加合理的设计是根据 topic 的类型来，而不是一开始由 admin 指定，topic 类型是 FIFO 的，那么消息拉取以及消费都是 FIFO
     * TOPIC 类型是非 FIFO 的，那么消息的拉取以及消费都应该是非 FIFO
     * 这样 SubscriptionGroup 就能随意订阅任何 topic 类型了
     *
     * */
    private ConsumeService consumeService;

    private volatile ScheduledFuture<?> scanAssignmentsFuture;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     */
    public PushConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup,
        Map<String, FilterExpression> subscriptionExpressions, MessageListener messageListener,
        int maxCacheMessageCount, int maxCacheMessageSizeInBytes, int consumptionThreadCount,
        boolean enableFifoConsumeAccelerator) {
        super(clientConfiguration, consumerGroup, subscriptionExpressions.keySet());
        this.clientConfiguration = clientConfiguration;
        Resource groupResource = new Resource(clientConfiguration.getNamespace(), consumerGroup);
        // consumer 一些信息
        this.pushSubscriptionSettings = new PushSubscriptionSettings(clientConfiguration.getNamespace(), clientId,
            endpoints, groupResource, clientConfiguration.getRequestTimeout(), subscriptionExpressions);
        this.consumerGroup = consumerGroup;
        // 消费者组订阅的所有 topic 以及对应的 FilterExpression
        this.subscriptionExpressions = subscriptionExpressions;
        this.cacheAssignments = new ConcurrentHashMap<>();
        // 注意这里是由 push consumer sdk 内部的 20 个 consume 线程并发调用 MessageListener 的
        // 消息 pop 下来之后会一个一个的提交给这 20 个线程并发执行
        // FIFO 消息是提交一个执行完之后，在向提交第二个，一个一个的提交执行
        this.messageListener = messageListener;
        // 1024
        this.maxCacheMessageCount = maxCacheMessageCount;
        // 64M
        this.maxCacheMessageSizeInBytes = maxCacheMessageSizeInBytes;
        // false
        this.enableFifoConsumeAccelerator = enableFifoConsumeAccelerator;

        this.receptionTimes = new AtomicLong(0);
        this.receivedMessagesQuantity = new AtomicLong(0);
        this.consumptionOkQuantity = new AtomicLong(0);
        this.consumptionErrorQuantity = new AtomicLong(0);

        this.processQueueTable = new ConcurrentHashMap<>();

        this.consumptionExecutor = new ThreadPoolExecutor(
            consumptionThreadCount, // 20
            consumptionThreadCount,
            60,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryImpl("MessageConsumption", this.getClientId().getIndex()));

        this.inflightRequestCountInterceptor = new InflightRequestCountInterceptor();
        this.addMessageInterceptor(inflightRequestCountInterceptor);
    }

    public PushConsumerImpl(ClientConfiguration clientConfiguration, String consumerGroup,
        Map<String, FilterExpression> subscriptionExpressions, MessageListener messageListener,
        int maxCacheMessageCount, int maxCacheMessageSizeInBytes, int consumptionThreadCount) {
        this(clientConfiguration, consumerGroup, subscriptionExpressions, messageListener, maxCacheMessageCount,
            maxCacheMessageSizeInBytes, consumptionThreadCount, true);
    }

    @Override
    protected void startUp() throws Exception {
        try {
            log.info("Begin to start the rocketmq push consumer, clientId={}", clientId);
            GaugeObserver gaugeObserver = new ProcessQueueGaugeObserver(processQueueTable, clientId, consumerGroup);
            this.clientMeterManager.setGaugeObserver(gaugeObserver);
            // 和生产者的逻辑一样，start clientImpl
            // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
            // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
            // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
            // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
            super.startUp();
            // availableProcessors
            final ScheduledExecutorService scheduler = this.getClientManager().getScheduler();
            // FifoConsumeService or StandardConsumeService ?
            /**
             * 这里的 FIFO 标识是在 admin 创建消费者组 SubscriptionGroup 时指定的，保存在 broker 中
             * 消费者启动的时候会去 broker 拉取 SubscriptionGroup 相关配置
             * 只要在 admin 指定了 FIFO, 那么 broker 端的拉取以及这里的消息消费逻辑均是 FIFO, 无论你订阅的事 normalTopic 还是延时，事务 topic
             * admin 指定非 FIFO ,那么 broker 端的拉取以及这里的消息消费逻辑均是非 FIFO ，即使你订阅的是 FIFO topic
             * 因此一个 SubscriptionGroup 不能同时订阅 FIFO TOPIC 和其他类型 topic
             * 其实更加合理的设计是根据 topic 的类型来，而不是一开始由 admin 指定，topic 类型是 FIFO 的，那么消息拉取以及消费都是 FIFO
             * TOPIC 类型是非 FIFO 的，那么消息的拉取以及消费都应该是非 FIFO
             * 这样 SubscriptionGroup 就能随意订阅任何 topic 类型了
             *
             * */
            this.consumeService = createConsumeService();
            // Scan assignments periodically.
            scanAssignmentsFuture = scheduler.scheduleWithFixedDelay(() -> {
                try {
                    // 每 5s send receive request
                    scanAssignments();
                } catch (Throwable t) {
                    log.error("Exception raised while scanning the load assignments, clientId={}", clientId, t);
                }
            }, 1, 5, TimeUnit.SECONDS);
            log.info("The rocketmq push consumer starts successfully, clientId={}", clientId);
        } catch (Throwable t) {
            log.error("Exception raised while starting the rocketmq push consumer, clientId={}", clientId, t);
            shutDown();
            throw t;
        }
    }

    /**
     * PushConsumerImpl shutdown order
     * 1. when begin shutdown, do not send any new receive request
     * 2. cancel scanAssignmentsFuture, do not create new processQueue
     * 3. waiting all inflight receive request finished or timeout
     * 4. shutdown consumptionExecutor and waiting all message consumption finished
     * 5. sleep 1s to ack message async
     * 6. shutdown clientImpl
     */
    @Override
    protected void shutDown() throws InterruptedException {
        log.info("Begin to shutdown the rocketmq push consumer, clientId={}", clientId);
        if (null != scanAssignmentsFuture) {
            scanAssignmentsFuture.cancel(false);
        }
        log.info("Waiting for the inflight receive requests to be finished, clientId={}", clientId);
        waitingReceiveRequestFinished();
        log.info("Begin to Shutdown consumption executor, clientId={}", clientId);
        this.consumptionExecutor.shutdown();
        ExecutorServices.awaitTerminated(consumptionExecutor);
        TimeUnit.SECONDS.sleep(1);
        super.shutDown();
        log.info("Shutdown the rocketmq push consumer successfully, clientId={}", clientId);
    }

    private void waitingReceiveRequestFinished() {
        Duration maxWaitingTime = clientConfiguration.getRequestTimeout()
            .plus(pushSubscriptionSettings.getLongPollingTimeout());
        long endTime = System.currentTimeMillis() + maxWaitingTime.toMillis();
        try {
            while (true) {
                long inflightReceiveRequestCount = inflightRequestCountInterceptor.getInflightReceiveRequestCount();
                if (inflightReceiveRequestCount <= 0) {
                    log.info("All inflight receive requests have been finished, clientId={}", clientId);
                    break;
                } else if (System.currentTimeMillis() > endTime) {
                    log.warn("Timeout waiting for all inflight receive requests to be finished, clientId={}, "
                        + "inflightReceiveRequestCount={}", clientId, inflightReceiveRequestCount);
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(100);
            }
        } catch (Exception e) {
            log.error("Unexpected exception while waiting for the inflight receive requests to be finished, "
                + "clientId={}", clientId, e);
        }
    }
/**
 * 这里的 FIFO 标识是在 admin 创建消费者组 SubscriptionGroup 时指定的，保存在 broker 中
 * 消费者启动的时候会去 broker 拉取 SubscriptionGroup 相关配置
 * 只要在 admin 指定了 FIFO, 那么 broker 端的拉取以及这里的消息消费逻辑均是 FIFO, 无论你订阅的事 normalTopic 还是延时，事务 topic
 * admin 指定非 FIFO ,那么 broker 端的拉取以及这里的消息消费逻辑均是非 FIFO ，即使你订阅的是 FIFO topic
 * 因此一个 SubscriptionGroup 不能同时订阅 FIFO TOPIC 和其他类型 topic
 * 其实更加合理的设计是根据 topic 的类型来，而不是一开始由 admin 指定，topic 类型是 FIFO 的，那么消息拉取以及消费都是 FIFO
 * TOPIC 类型是非 FIFO 的，那么消息的拉取以及消费都应该是非 FIFO
 * 这样 SubscriptionGroup 就能随意订阅任何 topic 类型了
 *
 * */
    private ConsumeService createConsumeService() {
        // availableProcessors
        final ScheduledExecutorService scheduler = this.getClientManager().getScheduler();
        // fifo 标识由 org.apache.rocketmq.client.java.impl.ClientManagerImpl.startUp 中启动的定时任务 syncSetting 进行设置
        // pushSubscriptionSettings 会在 syncSetting 中向远端 broker 同步 consumerGroup 的订阅配置（由 admin 创建消费者组时在指定broker填充）
        if (pushSubscriptionSettings.isFifo()) {
            log.info("Create FIFO consume service, consumerGroup={}, clientId={}, enableFifoConsumeAccelerator={}",
                consumerGroup, clientId, enableFifoConsumeAccelerator);
            return new FifoConsumeService(clientId, messageListener, consumptionExecutor, this,
                scheduler, enableFifoConsumeAccelerator);
        }
        log.info("Create standard consume service, consumerGroup={}, clientId={}", consumerGroup, clientId);
        return new StandardConsumeService(clientId, messageListener, consumptionExecutor, this, scheduler);
    }

    /**
     * @see PushConsumer#getConsumerGroup()
     */
    @Override
    public String getConsumerGroup() {
        return consumerGroup;
    }

    public PushSubscriptionSettings getPushConsumerSettings() {
        return pushSubscriptionSettings;
    }

    /**
     * @see PushConsumer#getSubscriptionExpressions()
     */
    @Override
    public Map<String, FilterExpression> getSubscriptionExpressions() {
        return new HashMap<>(subscriptionExpressions);
    }

    /**
     * @see PushConsumer#subscribe(String, FilterExpression)
     */
    @Override
    public PushConsumer subscribe(String topic, FilterExpression filterExpression) throws ClientException {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to add subscription because push consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Push consumer is not running now");
        }
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        handleClientFuture(future);
        subscriptionExpressions.put(topic, filterExpression);
        return this;
    }

    /**
     * @see PushConsumer#unsubscribe(String)
     */
    @Override
    public PushConsumer unsubscribe(String topic) {
        // Check consumer status.
        if (!this.isRunning()) {
            log.error("Unable to remove subscription because push consumer is not running, state={}, clientId={}",
                this.state(), clientId);
            throw new IllegalStateException("Push consumer is not running now");
        }
        subscriptionExpressions.remove(topic);
        return this;
    }

    private ListenableFuture<Endpoints> pickEndpointsToQueryAssignments(String topic) {
        final ListenableFuture<TopicRouteData> future = getRouteData(topic);
        return Futures.transformAsync(future, topicRouteData -> {
            if (topicRouteData.getTotalEndpoints().contains(this.getEndpoints())) {
                return Futures.immediateFuture(this.getEndpoints());
            }
            // 随便选一个 topic 所在副本集的 master
            Endpoints endpoints = topicRouteData.pickEndpointsToQueryAssignments();
            return Futures.immediateFuture(endpoints);
        }, MoreExecutors.directExecutor());
    }

    private QueryAssignmentRequest wrapQueryAssignmentRequest(String topic) {
        apache.rocketmq.v2.Resource topicResource = apache.rocketmq.v2.Resource.newBuilder()
            .setResourceNamespace(clientConfiguration.getNamespace())
            .setName(topic)
            .build();
        return QueryAssignmentRequest.newBuilder().setTopic(topicResource)
            .setEndpoints(endpoints.toProtobuf()).setGroup(getProtobufGroup()).build();
    }
    // 向 proxy 获取 topic 所在副本集中的所有可读 queue
    // 如果是 fifo 则收集所有副本集中的所有可读 queue
    // 非 fifo 则每个副本集只收集一个可读 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
    ListenableFuture<Assignments> queryAssignment(final String topic) {
        // 获取 proxy endpoints
        final ListenableFuture<Endpoints> future0 = pickEndpointsToQueryAssignments(topic);
        return Futures.transformAsync(future0, endpoints -> {
            final QueryAssignmentRequest request = wrapQueryAssignmentRequest(topic);
            final Duration requestTimeout = clientConfiguration.getRequestTimeout();
            // 向 proxy 获取 topic 所在副本集中的所有可读 queue
            // 如果是 fifo 则收集所有副本集中的所有可读 queue
            // 非 fifo 则每个副本集只收集一个可读 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
            final RpcFuture<QueryAssignmentRequest, QueryAssignmentResponse> future1 =
                this.getClientManager().queryAssignment(endpoints, request, requestTimeout);
            return Futures.transformAsync(future1, response -> {
                final Status status = response.getStatus();
                StatusChecker.check(status, future1);
                // 向 proxy 获取 topic 所在副本集中的所有可读 queue
                // 如果是 fifo 则收集所有副本集中的所有可读 queue
                // 非 fifo 则每个副本集只收集一个可读 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
                final List<Assignment> assignmentList = response.getAssignmentsList().stream().map(assignment ->
                    new Assignment(new MessageQueueImpl(assignment.getMessageQueue()))).collect(Collectors.toList());
                final Assignments assignments = new Assignments(assignmentList);
                return Futures.immediateFuture(assignments);
            }, MoreExecutors.directExecutor());
        }, MoreExecutors.directExecutor());
    }

    /**
     * Drop {@link ProcessQueue} by {@link MessageQueueImpl}, {@link ProcessQueue} must be removed before it is dropped.
     *
     * @param mq message queue.
     */
    void dropProcessQueue(MessageQueueImpl mq) {
        final ProcessQueue pq = processQueueTable.remove(mq);
        if (null != pq) {
            pq.drop();
        }
    }

    /**
     * Create process queue and add it into {@link #processQueueTable}, return {@link Optional#empty()} if mapped
     * process queue already exists.
     * <p>
     * This function and {@link #dropProcessQueue(MessageQueueImpl)} make sures that process queue is not dropped if
     * it is contained in {@link #processQueueTable}, once process queue is dropped, it must have been removed
     * from {@link #processQueueTable}.
     *
     * @param mq               message queue.
     * @param filterExpression filter expression of topic.
     * @return optional process queue.
     */
    protected Optional<ProcessQueue> createProcessQueue(MessageQueueImpl mq, final FilterExpression filterExpression) {
        final ProcessQueueImpl processQueue = new ProcessQueueImpl(this, mq, filterExpression);
        final ProcessQueue previous = processQueueTable.putIfAbsent(mq, processQueue);
        if (null != previous) {
            return Optional.empty();
        }
        return Optional.of(processQueue);
    }

    @Override
    public HeartbeatRequest wrapHeartbeatRequest() {
        return HeartbeatRequest.newBuilder().setGroup(getProtobufGroup())
            .setClientType(ClientType.PUSH_CONSUMER).build();
    }

    // 订阅的 topic
    // assignments : topic 所在副本集中所有可读的 messageQueue, 对于非 fifo 来说，这里每个副本集只收集一个 queue(id=-1),
    // 后续到了 broker 在随机选取 queue
    // filterExpression : 用户指定的消费 topic 对应的 filterExpression
    @VisibleForTesting
    void syncProcessQueue(String topic, Assignments assignments, FilterExpression filterExpression) {
        // 最新的订阅 topic 可读messagequeue
        Set<MessageQueueImpl> latest = new HashSet<>();
        // 获取订阅 topic 下所有可读 messageQueue(id 按照副本集的维度从 0 递增)
        final List<Assignment> assignmentList = assignments.getAssignmentList();
        for (Assignment assignment : assignmentList) {
            latest.add(assignment.getMessageQueue());
        }
        // 收集缓存中有效的 messageQueue (在最新的 latest 中，未过期)
        // 以 processQueueTable 为基础，剔除不在 latest 中的，以过期的
        // latest 中存在的 messageQueue 未必现在就在 processQueueTable 中
        // 下面会将不再 processQueueTable 中，但在 latest 中的 messageQueue 缓存到 processQueueTable
        Set<MessageQueueImpl> activeMqs = new HashSet<>();
        // 遍历所有订阅 topic messageQueue,过滤出本次 topic 下
        for (Map.Entry<MessageQueueImpl, ProcessQueue> entry : processQueueTable.entrySet()) {
            final MessageQueueImpl mq = entry.getKey();
            final ProcessQueue pq = entry.getValue();
            // 过滤指定 topic 下的 messageQueue
            if (!topic.equals(mq.getTopic())) {
                continue;
            }
            // 如果缓存的 messageQueue 不在最新的里边则从缓存中剔除
            if (!latest.contains(mq)) {
                log.info("Drop message queue according to the latest assignmentList, mq={}, clientId={}", mq,
                    clientId);
                dropProcessQueue(mq);
                continue;
            }
            // 缓存的 messageQueue 过期，剔除
            // no fetch message for a long time
            // 空闲时间不能超过 9s
            // activityNanoTime and cacheFullNanoTime 不能超过 9s
            if (pq.expired()) { // idle 检测
                log.warn("Drop message queue because it is expired, mq={}, clientId={}", mq, clientId);

                dropProcessQueue(mq);
                continue;
            }
            activeMqs.add(mq);
        }
        // 遍历 latest， 向 processQueueTable 中添加新的 messageQueue
        for (MessageQueueImpl mq : latest) {
            if (activeMqs.contains(mq)) {
                continue;
            }
            final Optional<ProcessQueue> optionalProcessQueue = createProcessQueue(mq, filterExpression);
            // 新加入的 messageQueue, 立即开始拉取消息
            if (optionalProcessQueue.isPresent()) {
                log.info("Start to fetch message from remote, mq={}, clientId={}", mq, clientId);
                optionalProcessQueue.get().fetchMessageImmediately();
            }
        }
    }
    // 向 proxy 获取每个订阅 topic 所在副本集中的所有可读 queue
    // 如果是 fifo 则收集所有副本集中的所有可读 queue
    // 非 fifo 则每个副本集只收集一个可读 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
    // 一个 Assignment 对应一个 MessageQueue
    @VisibleForTesting
    void scanAssignments() {
        try {
            log.debug("Start to scan assignments periodically, clientId={}", clientId);
            // 以消费者本地指定的 subscriptionExpressions 为主，远程 broker 端只定义消费者组的消费行为
            // 不会定义消费哪些 topic, 因为这个订阅关系是随时可变的，不适合 admin 一开始定义
            for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
                final String topic = entry.getKey();
                final FilterExpression filterExpression = entry.getValue();
                final Assignments existed = cacheAssignments.get(topic);
                // 向 proxy 获取 topic 所在副本集中的所有可读 queue
                // 如果是 fifo 则收集所有副本集中的所有可读 queue
                // 非 fifo 则每个副本集只收集一个可读 queue, 并且 queueId 是 -1 ， 到了 broker 会随机选择 queue
                // 一个 Assignment 对应一个 MessageQueue
                final ListenableFuture<Assignments> future = queryAssignment(topic);
                // 由 asyncWorker( availableProcessors, 50000 队列) 执行
                Futures.addCallback(future, new FutureCallback<Assignments>() {
                    @Override
                    public void onSuccess(Assignments latest) {
                        if (latest.getAssignmentList().isEmpty()) {
                            if (null == existed || existed.getAssignmentList().isEmpty()) {
                                log.info("Acquired empty assignments from remote, would scan later, topic={}, "
                                    + "clientId={}", topic, clientId);
                                return;
                            }
                            log.info("Attention!!! acquired empty assignments from remote, but existed assignments"
                                + " is not empty, topic={}, clientId={}", topic, clientId);
                        }

                        if (!latest.equals(existed)) {
                            log.info("Assignments of topic={} has changed, {} => {}, clientId={}", topic, existed,
                                latest, clientId);
                            // 订阅 topic 的所有可读队列（所有副本集中）
                            syncProcessQueue(topic, latest, filterExpression);
                            cacheAssignments.put(topic, latest);
                            return;
                        }
                        log.debug("Assignments of topic={} remains the same, assignments={}, clientId={}", topic,
                            existed, clientId);
                        // Process queue may be dropped, need to be synchronized anyway.
                        syncProcessQueue(topic, latest, filterExpression);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        log.error("Exception raised while scanning the assignments, topic={}, clientId={}", topic,
                            clientId, t);
                    }
                }, MoreExecutors.directExecutor());
            }
        } catch (Throwable t) {
            log.error("Exception raised while scanning the assignments for all topics, clientId={}", clientId, t);
        }
    }

    @Override
    public Settings getSettings() {
        return pushSubscriptionSettings;
    }

    /**
     * @see PushConsumer#close()
     */
    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    int getQueueSize() {
        return processQueueTable.size();
    }

    int cacheMessageBytesThresholdPerQueue() {
        final int size = this.getQueueSize();
        // ALl process queues are removed, no need to cache messages.
        if (size <= 0) {
            return 0;
        }
        // 64M
        return Math.max(1, maxCacheMessageSizeInBytes / size);
    }

    int cacheMessageCountThresholdPerQueue() {
        // consumer 订阅的所有 topic 下的所有 messageQueue 数量
        final int size = this.getQueueSize();
        // All process queues are removed, no need to cache messages.
        if (size <= 0) {
            return 0;
        }
        // maxCacheMessageCount = 1024 ，这个是缓存消息的总量（针对所有 messageQueue(所有订阅 topic)）
        // 1024 / size
        return Math.max(1, maxCacheMessageCount / size);
    }

    public AtomicLong getReceptionTimes() {
        return receptionTimes;
    }

    public AtomicLong getReceivedMessagesQuantity() {
        return receivedMessagesQuantity;
    }

    public ConsumeService getConsumeService() {
        return consumeService;
    }

    @Override
    public void onVerifyMessageCommand(Endpoints endpoints, VerifyMessageCommand verifyMessageCommand) {
        final String nonce = verifyMessageCommand.getNonce();
        final MessageViewImpl messageView = MessageViewImpl.fromProtobuf(verifyMessageCommand.getMessage());
        final MessageId messageId = messageView.getMessageId();
        final ListenableFuture<ConsumeResult> future = consumeService.consume(messageView);
        Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
            @Override
            public void onSuccess(ConsumeResult consumeResult) {
                Code code = ConsumeResult.SUCCESS.equals(consumeResult) ? Code.OK : Code.FAILED_TO_CONSUME_MESSAGE;
                Status status = Status.newBuilder().setCode(code).build();
                final VerifyMessageResult verifyMessageResult =
                    VerifyMessageResult.newBuilder().setNonce(nonce).build();
                TelemetryCommand command = TelemetryCommand.newBuilder()
                    .setVerifyMessageResult(verifyMessageResult)
                    .setStatus(status)
                    .build();
                try {
                    telemetry(endpoints, command);
                } catch (Throwable t) {
                    log.error("Failed to send message verification result command, endpoints={}, command={}, "
                        + "messageId={}, clientId={}", endpoints, command, messageId, clientId, t);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                // Should never reach here.
                log.error("[Bug] Failed to get message verification result, endpoints={}, messageId={}, "
                    + "clientId={}", endpoints, messageId, clientId, t);
            }
        }, MoreExecutors.directExecutor());
    }

    private ForwardMessageToDeadLetterQueueRequest wrapForwardMessageToDeadLetterQueueRequest(
        MessageViewImpl messageView) {
        final apache.rocketmq.v2.Resource topicResource =
            apache.rocketmq.v2.Resource.newBuilder()
                .setResourceNamespace(clientConfiguration.getNamespace())
                .setName(messageView.getTopic())
                .build();
        return ForwardMessageToDeadLetterQueueRequest.newBuilder().setGroup(getProtobufGroup()).setTopic(topicResource)
            .setReceiptHandle(messageView.getReceiptHandle())
            .setMessageId(messageView.getMessageId().toString())
            .setDeliveryAttempt(messageView.getDeliveryAttempt())
            .setMaxDeliveryAttempts(getRetryPolicy().getMaxAttempts()).build();
    }

    public RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse>
    forwardMessageToDeadLetterQueue(final MessageViewImpl messageView) {
        // Intercept before forwarding message to DLQ.
        final List<GeneralMessage> generalMessages = Collections.singletonList(new GeneralMessageImpl(messageView));
        MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.FORWARD_TO_DLQ);
        doBefore(context, generalMessages);

        final Endpoints endpoints = messageView.getEndpoints();
        RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future;
        final ForwardMessageToDeadLetterQueueRequest request =
            wrapForwardMessageToDeadLetterQueueRequest(messageView);
        // 一个 consumerGroup 对应一个死信队列 DLQTopic : %DLQ%consumerGroup
        future = this.getClientManager().forwardMessageToDeadLetterQueue(endpoints, request,
            clientConfiguration.getRequestTimeout());
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                // Intercept after forwarding message to DLQ.
                MessageHookPointsStatus status = Code.OK.equals(response.getStatus().getCode()) ?
                    MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
                final MessageInterceptorContext context0 = new MessageInterceptorContextImpl(context, status);
                doAfter(context0, generalMessages);
            }

            @Override
            public void onFailure(Throwable t) {
                // Intercept after forwarding message to DLQ.
                final MessageInterceptorContext context0 = new MessageInterceptorContextImpl(context,
                    MessageHookPointsStatus.ERROR);
                doAfter(context0, generalMessages);
            }
        }, MoreExecutors.directExecutor());
        return future;
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public void doStats() {
        final long receptionTimes = this.receptionTimes.getAndSet(0);
        final long receivedMessagesQuantity = this.receivedMessagesQuantity.getAndSet(0);

        final long consumptionOkQuantity = this.consumptionOkQuantity.getAndSet(0);
        final long consumptionErrorQuantity = this.consumptionErrorQuantity.getAndSet(0);

        log.info("clientId={}, consumerGroup={}, receptionTimes={}, receivedMessagesQuantity={}, "
                + "consumptionOkQuantity={}, consumptionErrorQuantity={}", clientId, consumerGroup, receptionTimes,
            receivedMessagesQuantity, consumptionOkQuantity, consumptionErrorQuantity);
        processQueueTable.values().forEach(ProcessQueue::doStats);
    }

    public RetryPolicy getRetryPolicy() {
        return pushSubscriptionSettings.getRetryPolicy();
    }

    public ThreadPoolExecutor getConsumptionExecutor() {
        return consumptionExecutor;
    }
}
