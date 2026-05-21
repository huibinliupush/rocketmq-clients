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

import apache.rocketmq.v2.AckMessageRequest;
import apache.rocketmq.v2.AckMessageResponse;
import apache.rocketmq.v2.ChangeInvisibleDurationRequest;
import apache.rocketmq.v2.ChangeInvisibleDurationResponse;
import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueRequest;
import apache.rocketmq.v2.ForwardMessageToDeadLetterQueueResponse;
import apache.rocketmq.v2.ReceiveMessageRequest;
import apache.rocketmq.v2.Status;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.apache.rocketmq.client.java.exception.BadRequestException;
import org.apache.rocketmq.client.java.exception.TooManyRequestsException;
import org.apache.rocketmq.client.java.hook.MessageHookPoints;
import org.apache.rocketmq.client.java.hook.MessageHookPointsStatus;
import org.apache.rocketmq.client.java.hook.MessageInterceptorContextImpl;
import org.apache.rocketmq.client.java.message.GeneralMessage;
import org.apache.rocketmq.client.java.message.GeneralMessageImpl;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.rpc.RpcFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ProcessQueue}.
 *
 * <p>Apart from the basic part mentioned in {@link ProcessQueue}, this implementation
 *
 * @see ProcessQueue
 */
@SuppressWarnings({"NullableProblems", "UnstableApiUsage"})
class ProcessQueueImpl implements ProcessQueue {
    static final Duration FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);
    static final Duration ACK_MESSAGE_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);
    static final Duration CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);

    private static final Logger log = LoggerFactory.getLogger(ProcessQueueImpl.class);

    private static final Duration RECEIVING_FLOW_CONTROL_BACKOFF_DELAY = Duration.ofMillis(20);
    private static final Duration RECEIVING_FAILURE_BACKOFF_DELAY = Duration.ofSeconds(1);
    private static final Duration RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL = Duration.ofSeconds(1);

    private final PushConsumerImpl consumer;

    /**
     * Dropped means {@link ProcessQueue} is deprecated, which means no message would be fetched from remote anymore.
     */
    private volatile boolean dropped;
    private final MessageQueueImpl mq;
    private final FilterExpression filterExpression;

    /**
     * Messages which is pending means have been cached, but are not taken by consumer dispatcher yet.
     * 从 broker pop 下来的消息将会缓存到这里，消息不一定是 mq 属性指定的队列，broker 是根据 mq 中的 brokerName 随机选取一个队列 pop 消息
     * 这里缓存的是同一副本集（brokerName）下的所有队列的消息
     *
     * MessageViewImpl 的 GC ROOT，没被消费之前一直 hold 在这里，防止被 GC
     * 其实消费者在消费 MessageViewImpl 的时候也不是从 cache 中拿的，而是从 broker pop 出来直接消费
     * 但需要缓存一下让 GC ROOT 引用
     */
    @GuardedBy("cachedMessageLock")
    private final List<MessageViewImpl> cachedMessages;
    private final ReadWriteLock cachedMessageLock;
    // 缓存的消息 body size 总量
    private final AtomicLong cachedMessagesBytes;
    // 重新消费次数
    private final AtomicLong receptionTimes;
    // 接收到消息的总数
    private final AtomicLong receivedMessagesQuantity;

    private volatile long activityNanoTime = System.nanoTime();
    // cachedMessages 满了时时候的时间戳
    // cacheMessageCountThresholdPerQueue <= actualMessagesQuantity
    private volatile long cacheFullNanoTime = Long.MIN_VALUE;

    public ProcessQueueImpl(PushConsumerImpl consumer, MessageQueueImpl mq, FilterExpression filterExpression) {
        this.consumer = consumer;
        this.dropped = false;
        // 要消费的 messageQueue
        this.mq = mq;
        // consumer 指定的 filterExpression（消息过滤）
        this.filterExpression = filterExpression;
        this.cachedMessages = new ArrayList<>();
        this.cachedMessageLock = new ReentrantReadWriteLock();
        this.cachedMessagesBytes = new AtomicLong();
        this.receptionTimes = new AtomicLong(0);
        this.receivedMessagesQuantity = new AtomicLong(0);
    }

    @Override
    public MessageQueueImpl getMessageQueue() {
        return mq;
    }

    @Override
    public void drop() {
        this.dropped = true;
    }

    @Override
    public boolean expired() {
        final Duration longPollingTimeout = consumer.getPushConsumerSettings().getLongPollingTimeout();
        final Duration requestTimeout = consumer.getClientConfiguration().getRequestTimeout();
        final Duration maxIdleDuration = longPollingTimeout.plus(requestTimeout).multipliedBy(3);
        final Duration idleDuration = Duration.ofNanos(System.nanoTime() - activityNanoTime);
        if (idleDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        final Duration afterCacheFullDuration = Duration.ofNanos(System.nanoTime() - cacheFullNanoTime);
        if (afterCacheFullDuration.compareTo(maxIdleDuration) < 0) {
            return false;
        }
        log.warn("Process queue is idle, idleDuration={}, maxIdleDuration={}, afterCacheFullDuration={}, mq={}, "
            + "clientId={}", idleDuration, maxIdleDuration, afterCacheFullDuration, mq, consumer.getClientId());
        return true;
    }

    void cacheMessages(List<MessageViewImpl> messageList) {
        cachedMessageLock.writeLock().lock();
        try {
            for (MessageViewImpl messageView : messageList) {
                cachedMessages.add(messageView);
                cachedMessagesBytes.addAndGet(messageView.getBody().remaining());
            }
        } finally {
            cachedMessageLock.writeLock().unlock();
        }
    }

    private int getReceptionBatchSize() {
        // maxCacheMessageCount = 1024 ，这个是缓存消息的总量（针对所有 messageQueue(所有订阅 topic)）
        // 1024 / size
        int bufferSize = consumer.cacheMessageCountThresholdPerQueue() - this.cachedMessagesCount();
        bufferSize = Math.max(bufferSize, 1);
        // receiveBatchSize = 32(从 proxy 获取)
        return Math.min(bufferSize, consumer.getPushConsumerSettings().getReceiveBatchSize());
    }

    @Override
    public void fetchMessageImmediately() {
        receiveMessageImmediately();
    }

    /**
     * Receive message later by message queue.
     *
     * <p> Make sure that no exception will be thrown.
     */
    public void onReceiveMessageException(Throwable t, String attemptId) {
        Duration delay = t instanceof TooManyRequestsException ? RECEIVING_FLOW_CONTROL_BACKOFF_DELAY :
            RECEIVING_FAILURE_BACKOFF_DELAY;
        receiveMessageLater(delay, attemptId);
    }
    // asyncWorker 执行
    private void receiveMessageLater(Duration delay, String attemptId) {
        final ClientId clientId = consumer.getClientId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            log.info("Try to receive message later, mq={}, delay={}, clientId={}", mq, delay, clientId);
            scheduler.schedule(() -> receiveMessage(attemptId), delay.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message receiving request, mq={}, clientId={}", mq, clientId, t);
            onReceiveMessageException(t, attemptId);
        }
    }

    private String generateAttemptId() {
        return UUID.randomUUID().toString();
    }
    // asyncWorker 执行
    public void receiveMessage() {
        // 开启新的一轮 pop
        receiveMessage(this.generateAttemptId());
    }
    // asyncWorker 执行
    // cache 满了，则由 scheduler 延时 1s 在 调用
    public void receiveMessage(String attemptId) {
        final ClientId clientId = consumer.getClientId();
        if (dropped) {
            log.info("Process queue has been dropped, no longer receive message, mq={}, clientId={}", mq, clientId);
            return;
        }
        // cache 满了就等 1s后再 pop
        if (this.isCacheFull()) {
            log.warn("Process queue cache is full, would receive message later, mq={}, clientId={}", mq, clientId);
            receiveMessageLater(RECEIVING_BACKOFF_DELAY_WHEN_CACHE_IS_FULL, attemptId);
            return;
        }
        // cache 没有拉满就立即 pop
        receiveMessageImmediately(attemptId);
    }

    private void receiveMessageImmediately() {
        receiveMessageImmediately(this.generateAttemptId());
    }
    // asyncWorker 执行
    private void receiveMessageImmediately(String attemptId) {
        final ClientId clientId = consumer.getClientId();
        if (!consumer.isRunning()) {
            log.info("Stop to receive message because consumer is not running, mq={}, clientId={}", mq, clientId);
            return;
        }
        try {
            // 这里逻辑上应该是 messageQueue 所在副本集 master broker 的地址，但其实是 proxy 地址
            final Endpoints endpoints = mq.getBroker().getEndpoints();
            // 单个 messageQueue 能够缓存的消息数量，最大为 32
            final int batchSize = this.getReceptionBatchSize();
            // 20s(proxy端配置)
            final Duration longPollingTimeout = consumer.getPushConsumerSettings().getLongPollingTimeout();
            // 创建 gRPC 请求
            final ReceiveMessageRequest request = consumer.wrapReceiveMessageRequest(batchSize, mq, filterExpression,
                longPollingTimeout, attemptId);
            // 更新 activeTime , 用于 idle 检测
            activityNanoTime = System.nanoTime();

            // Intercept before message reception.
            final MessageInterceptorContextImpl context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
            // inflightRequestCountInterceptor
            consumer.doBefore(context, Collections.emptyList());
            // 向 proxy 拉取消息
            final ListenableFuture<ReceiveMessageResult> future = consumer.receiveMessage(request, mq,
                longPollingTimeout);
            // asyncWorker
            Futures.addCallback(future, new FutureCallback<ReceiveMessageResult>() {
                    @Override
                    public void onSuccess(ReceiveMessageResult result) {
                        // Intercept after message reception.
                        final List<GeneralMessage> generalMessages = result.getMessageViewImpls().stream()
                            .map((Function<MessageView, GeneralMessage>) GeneralMessageImpl::new)
                            .collect(Collectors.toList());
                        final MessageInterceptorContextImpl context0 =
                            new MessageInterceptorContextImpl(context, MessageHookPointsStatus.OK);
                        consumer.doAfter(context0, generalMessages);

                        try {
                            // asyncWorker
                            onReceiveMessageResult(result);
                        } catch (Throwable t) {
                            // Should never reach here.
                            log.error("[Bug] Exception raised while handling receive result, mq={}, endpoints={}, "
                                + "clientId={}", mq, endpoints, clientId, t);
                            onReceiveMessageException(t, attemptId);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        String nextAttemptId = null;
                        if (t instanceof StatusRuntimeException) {
                            StatusRuntimeException exception = (StatusRuntimeException) t;
                            if (io.grpc.Status.DEADLINE_EXCEEDED.getCode() == exception.getStatus().getCode()) {
                                nextAttemptId = request.getAttemptId();
                            }
                        }
                        // Intercept after message reception.
                        final MessageInterceptorContextImpl context0 =
                            new MessageInterceptorContextImpl(context, MessageHookPointsStatus.ERROR);
                        consumer.doAfter(context0, Collections.emptyList());

                        log.error("Exception raised during message reception, mq={}, endpoints={}, attemptId={}, " +
                                "nextAttemptId={}, clientId={}", mq, endpoints, request.getAttemptId(), nextAttemptId,
                            clientId, t);
                        onReceiveMessageException(t, nextAttemptId);
                    }
                }, MoreExecutors.directExecutor());
            receptionTimes.getAndIncrement();
            consumer.getReceptionTimes().getAndIncrement();
        } catch (Throwable t) {
            log.error("Exception raised during message reception, mq={}, clientId={}", mq, clientId, t);
            onReceiveMessageException(t, attemptId);
        }
    }

    public boolean isCacheFull() {
        // 每个 queue 可以缓存的消息个数
        final int cacheMessageCountThresholdPerQueue = consumer.cacheMessageCountThresholdPerQueue();
        // 当前缓存的消息个数
        final long actualMessagesQuantity = this.cachedMessagesCount();
        final ClientId clientId = consumer.getClientId();
        if (cacheMessageCountThresholdPerQueue <= actualMessagesQuantity) {
            log.warn("Process queue total cached messages quantity exceeds the threshold, threshold={}, actual={}," +
                " mq={}, clientId={}", cacheMessageCountThresholdPerQueue, actualMessagesQuantity, mq, clientId);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        final int cacheMessageBytesThresholdPerQueue = consumer.cacheMessageBytesThresholdPerQueue();
        final long actualCachedMessagesBytes = this.cachedMessageBytes();
        if (cacheMessageBytesThresholdPerQueue <= actualCachedMessagesBytes) {
            log.warn("Process queue total cached messages memory exceeds the threshold, threshold={} bytes," +
                    " actual={} bytes, mq={}, clientId={}", cacheMessageBytesThresholdPerQueue,
                actualCachedMessagesBytes, mq, clientId);
            cacheFullNanoTime = System.nanoTime();
            return true;
        }
        return false;
    }

    @Override
    public void discardMessage(MessageViewImpl messageView) {
        log.info("Discard message, mq={}, messageId={}, clientId={}", mq, messageView.getMessageId(),
            consumer.getClientId());
        // changeInvisibleDuration
        final ListenableFuture<Void> future = nackMessage(messageView);
        future.addListener(() -> evictCache(messageView), MoreExecutors.directExecutor());
    }

    @Override
    public void discardFifoMessage(MessageViewImpl messageView) {
        log.info("Discard fifo message, mq={}, messageId={}, clientId={}", mq, messageView.getMessageId(),
            consumer.getClientId());
        final ListenableFuture<Void> future = forwardToDeadLetterQueue(messageView);
        future.addListener(() -> evictCache(messageView), MoreExecutors.directExecutor());
    }

    public int cachedMessagesCount() {
        cachedMessageLock.readLock().lock();
        try {
            return cachedMessages.size();
        } finally {
            cachedMessageLock.readLock().unlock();
        }
    }

    public long cachedMessageBytes() {
        return cachedMessagesBytes.get();
    }
    // asyncWorker 执行
    private void onReceiveMessageResult(ReceiveMessageResult result) {
        final List<MessageViewImpl> messages = result.getMessageViewImpls();
        if (!messages.isEmpty()) {
            // 缓存 pop 下来的消息
            cacheMessages(messages);
            receivedMessagesQuantity.getAndAdd(messages.size());
            consumer.getReceivedMessagesQuantity().getAndAdd(messages.size());
            // 消费消息
            // 消息提交到 consumptionExecutor（20线程）中消费,回调客户端指定的 messageListener
            // 消费成功则 ackMessage,消费失败则 nackMessage,从 cache 中剔除 message

            // 在 FIFO 消费场景下，必须等到前一个消息消费成功之后，才能开始下一个消息的消费
            // 如果前一个消息没有消费成功，那么就进行重试，重试成功之后再开始下一个消息消费
            // 如果重试一直失败，达到最大重试次数，则直接发送到死信队列，发送成功之后（不成功则一直重试直到成功）再开始下一个消息的消费
            // 消费成功也是一样，必须等到 ackMessage 成功之后才能消费下一个消息
            consumer.getConsumeService().consume(this, messages);
        }
        // 继续 pop
        receiveMessage();
    }

    private void evictCache(MessageViewImpl messageView) {
        cachedMessageLock.writeLock().lock();
        try {
            if (cachedMessages.remove(messageView)) {
                cachedMessagesBytes.addAndGet(-messageView.getBody().remaining());
            }
        } finally {
            cachedMessageLock.writeLock().unlock();
        }
    }

    private void statsConsumptionResult(ConsumeResult consumeResult) {
        if (ConsumeResult.SUCCESS.equals(consumeResult)) {
            consumer.consumptionOkQuantity.incrementAndGet();
            return;
        }
        consumer.consumptionErrorQuantity.incrementAndGet();
    }

    @Override
    public void eraseMessage(MessageViewImpl messageView, ConsumeResult consumeResult) {
        // stats 计数
        statsConsumptionResult(consumeResult);
        ListenableFuture<Void> future = ConsumeResult.SUCCESS.equals(consumeResult) ? ackMessage(messageView) :
            nackMessage(messageView);
        future.addListener(() -> evictCache(messageView), MoreExecutors.directExecutor());
    }

    private ListenableFuture<Void> nackMessage(final MessageViewImpl messageView) {
        final int deliveryAttempt = messageView.getDeliveryAttempt();
        // admin 创建消费者组时指定
        // org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy
        // org.apache.rocketmq.remoting.protocol.subscription.CustomizedRetryPolicy
        // CustomizedRetryPolicy : 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        // ExponentialRetryPolicy: initial=5s , multiplier = 2, max = 2h
        // 获取 deliveryAttempt 对应的重试间隔 duration
        final Duration duration = consumer.getRetryPolicy().getNextAttemptDelay(deliveryAttempt);
        final SettableFuture<Void> future0 = SettableFuture.create();
        // 从现在开始算起，消息将在 duration 之后可见，也就是说 pushConsumer 的重试间隔时间不包括消息消费时间
        // 从返回 ConsumeResult.FAILURE 这一时刻开始算起
        // 向 reviveTopic 添加一个新的 checkpoint , 修改它的 InvisibleTime , 新的 popTime 为当前时间戳
        // ack 原来的消息，防止原来的消息被重新投递，从而达到修改消息 InvisibleTime 的逻辑
        changeInvisibleDuration(messageView, duration, 1, future0);
        return future0;
    }
    // 向 reviveTopic 添加一个新的 checkpoint , 修改它的 InvisibleTim
    // ack 原来的消息，防止原来的消息被重新投递，从而达到修改消息 InvisibleTim 的逻辑
    private void changeInvisibleDuration(final MessageViewImpl messageView, final Duration duration,
        final int attempt, final SettableFuture<Void> future0) {
        final ClientId clientId = consumer.getClientId();
        final String consumerGroup = consumer.getConsumerGroup();
        final MessageId messageId = messageView.getMessageId();
        // 逻辑上消息所在 broker 的地址，其实这里是 proxy 地址（由 proxy 转发）
        final Endpoints endpoints = messageView.getEndpoints();
        // 向 reviveTopic 添加一个新的 checkpoint , 修改它的 InvisibleTim
        // ack 原来的消息，防止原来的消息被重新投递，从而达到修改消息 InvisibleTim 的逻辑
        final RpcFuture<ChangeInvisibleDurationRequest, ChangeInvisibleDurationResponse> future =
            consumer.changeInvisibleDuration(messageView, duration);
        Futures.addCallback(future, new FutureCallback<ChangeInvisibleDurationResponse>() {
            @Override
            public void onSuccess(ChangeInvisibleDurationResponse response) {
                final String requestId = future.getContext().getRequestId();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.INVALID_RECEIPT_HANDLE.equals(code)) {
                    log.error("Failed to change invisible duration due to the invalid receipt handle, forgive to "
                            + "retry, clientId={}, consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, "
                            + "requestId={}, status message=[{}]", clientId, consumerGroup, messageId, attempt, mq,
                        endpoints, requestId, status.getMessage());
                    future0.setException(new BadRequestException(code.getNumber(), requestId, status.getMessage()));
                    return;
                }
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    log.error("Failed to change invisible duration, would retry later, clientId={}, "
                            + "consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, requestId={}, "
                            + "status message=[{}]", clientId, consumerGroup, messageId, attempt, mq, endpoints,
                        requestId, status.getMessage());
                    changeInvisibleDurationLater(messageView, duration, 1 + attempt, future0);
                    return;
                }
                // Set result if succeed in changing invisible time.
                future0.setFuture(Futures.immediateVoidFuture());
                // Log retries.
                if (1 < attempt) {
                    log.info("Finally, change invisible duration successfully, clientId={}, consumerGroup={} "
                            + "messageId={}, attempt={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
                        messageId, attempt, mq, endpoints, requestId);
                    return;
                }
                log.debug("Change invisible duration successfully, clientId={}, consumerGroup={}, messageId={}, "
                        + "mq={}, endpoints={}, requestId={}", clientId, consumerGroup, messageId, mq, endpoints,
                    requestId);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                log.error("Exception raised while changing invisible duration, would retry later, clientId={}, "
                        + "consumerGroup={}, messageId={}, mq={}, endpoints={}", clientId, consumerGroup,
                    messageId, mq, endpoints, t);
                changeInvisibleDurationLater(messageView, duration, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void changeInvisibleDurationLater(final MessageViewImpl messageView, final Duration duration,
        final int attempt, SettableFuture<Void> future) {
        final MessageId messageId = messageView.getMessageId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> changeInvisibleDuration(messageView, duration, attempt, future),
                CHANGE_INVISIBLE_DURATION_FAILURE_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message change invisible duration request, mq={}, messageId={}, "
                + "clientId={}", mq, messageId, consumer.getClientId());
            changeInvisibleDurationLater(messageView, duration, 1 + attempt, future);
        }
    }

    @Override
    public ListenableFuture<Void> eraseFifoMessage(MessageViewImpl messageView, ConsumeResult consumeResult) {
        statsConsumptionResult(consumeResult);
        // CustomizedRetryPolicy : 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
        // ExponentialRetryPolicy: initial=5s , multiplier = 2, max = 2h
        final RetryPolicy retryPolicy = consumer.getRetryPolicy();
        final int maxAttempts = retryPolicy.getMaxAttempts();
        int attempt = messageView.getDeliveryAttempt();
        final MessageId messageId = messageView.getMessageId();
        // FifoConsumeService
        final ConsumeService service = consumer.getConsumeService();
        final ClientId clientId = consumer.getClientId();
        // 未到最大重试次数
        if (ConsumeResult.FAILURE.equals(consumeResult) && attempt < maxAttempts) {
            // 重试间隔
            final Duration nextAttemptDelay = retryPolicy.getNextAttemptDelay(attempt);
            attempt = messageView.incrementAndGetDeliveryAttempt();
            log.debug("Prepare to redeliver the fifo message because of the consumption failure, maxAttempt={}," +
                    " attempt={}, mq={}, messageId={}, nextAttemptDelay={}, clientId={}", maxAttempts, attempt, mq,
                messageId, nextAttemptDelay, clientId);
            // 本地延时重试
            final ListenableFuture<ConsumeResult> future = service.consume(messageView, nextAttemptDelay);
            // 处理重试结果
            return Futures.transformAsync(future, result -> eraseFifoMessage(messageView, result),
                MoreExecutors.directExecutor());
        }
        boolean ok = ConsumeResult.SUCCESS.equals(consumeResult);
        if (!ok) {
            log.info("Failed to consume fifo message finally, run out of attempt times, maxAttempts={}, "
                + "attempt={}, mq={}, messageId={}, clientId={}", maxAttempts, attempt, mq, messageId, clientId);
        }
        // Ack message or forward it to DLQ depends on consumption result.
        // 一直重试失败，则发往死信队列， 一个 consumerGroup 对应一个死信队列 DLQTopic : %DLQ%consumerGroup
        ListenableFuture<Void> future = ok ? ackMessage(messageView) : forwardToDeadLetterQueue(messageView);
        // ackMessage 或者发送到死信队列成功之后剔除缓存
        future.addListener(() -> evictCache(messageView), consumer.getConsumptionExecutor());
        return future;
    }


    private ListenableFuture<Void> forwardToDeadLetterQueue(final MessageViewImpl messageView) {
        final SettableFuture<Void> future = SettableFuture.create();
        forwardToDeadLetterQueue(messageView, 1, future);
        return future;
    }

    private void forwardToDeadLetterQueue(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        // 一个 consumerGroup 对应一个死信队列 DLQTopic : %DLQ%consumerGroup
        final RpcFuture<ForwardMessageToDeadLetterQueueRequest, ForwardMessageToDeadLetterQueueResponse> future =
            consumer.forwardMessageToDeadLetterQueue(messageView);
        final ClientId clientId = consumer.getClientId();
        final String consumerGroup = consumer.getConsumerGroup();
        final MessageId messageId = messageView.getMessageId();
        final Endpoints endpoints = messageView.getEndpoints();
        Futures.addCallback(future, new FutureCallback<ForwardMessageToDeadLetterQueueResponse>() {
            @Override
            public void onSuccess(ForwardMessageToDeadLetterQueueResponse response) {
                final String requestId = future.getContext().getRequestId();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    log.error("Failed to forward message to dead letter queue, would attempt to re-forward later," +
                            " clientId={}, consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, "
                            + "requestId={}, code={}, status message={}", clientId, consumerGroup, messageId, attempt,
                        mq, endpoints, requestId, code, status.getMessage());
                    forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
                    return;
                }
                // Set result if message is forwarded successfully.
                future0.setFuture(Futures.immediateVoidFuture());
                // Log retries.
                if (1 < attempt) {
                    log.info("Re-forward message to dead letter queue successfully, clientId={}, consumerGroup={}, "
                            + "attempt={}, messageId={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup,
                        attempt, messageId, mq, endpoints, requestId);
                    return;
                }
                log.info("Forward message to dead letter queue successfully, clientId={}, consumerGroup={}, "
                        + "messageId={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup, messageId, mq,
                    endpoints, requestId);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                log.error("Exception raised while forward message to DLQ, would attempt to re-forward later, " +
                        "clientId={}, consumerGroup={}, attempt={}, messageId={}, mq={}", clientId, consumerGroup,
                    attempt, messageId, mq, t);
                forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void forwardToDeadLetterQueueLater(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future0) {
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            scheduler.schedule(() -> forwardToDeadLetterQueue(messageView, attempt, future0),
                FORWARD_FIFO_MESSAGE_TO_DLQ_FAILURE_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule DLQ message request, mq={}, messageId={}, clientId={}", mq,
                messageView.getMessageId(), consumer.getClientId());
            forwardToDeadLetterQueueLater(messageView, 1 + attempt, future0);
        }
    }

    private ListenableFuture<Void> ackMessage(final MessageViewImpl messageView) {
        SettableFuture<Void> future = SettableFuture.create();
        ackMessage(messageView, 1, future);
        return future;
    }

    private void ackMessage(final MessageViewImpl messageView, final int attempt, final SettableFuture<Void> future0) {
        final ClientId clientId = consumer.getClientId();
        final String consumerGroup = consumer.getConsumerGroup();
        // storehost + commitlogOffset
        final MessageId messageId = messageView.getMessageId();
        // 消息所在的 broker, 但其实这里还是 proxy 地址，由 proxy 转发
        final Endpoints endpoints = messageView.getEndpoints();
        final RpcFuture<AckMessageRequest, AckMessageResponse> future =
            consumer.ackMessage(messageView);
        // 如果 ack 失败则 ackMessageLater（1 + attempt）
        Futures.addCallback(future, new FutureCallback<AckMessageResponse>() {
            @Override
            public void onSuccess(AckMessageResponse response) {
                final String requestId = future.getContext().getRequestId();
                final Status status = response.getStatus();
                final Code code = status.getCode();
                if (Code.INVALID_RECEIPT_HANDLE.equals(code)) {
                    log.error("Failed to ack message due to the invalid receipt handle, forgive to retry, "
                            + "clientId={}, consumerGroup={}, messageId={}, attempt={}, mq={}, endpoints={}, "
                            + "requestId={}, status message=[{}]", clientId, consumerGroup, messageId, attempt, mq,
                        endpoints, requestId, status.getMessage());
                    future0.setException(new BadRequestException(code.getNumber(), requestId, status.getMessage()));
                    return;
                }
                // Log failure and retry later.
                if (!Code.OK.equals(code)) {
                    log.error("Failed to ack message, would attempt to re-ack later, clientId={}, "
                            + "consumerGroup={}, attempt={}, messageId={}, mq={}, code={}, requestId={}, endpoints={}, "
                            + "status message=[{}]", clientId, consumerGroup, attempt, messageId, mq, code, requestId,
                        endpoints, status.getMessage());
                    ackMessageLater(messageView, 1 + attempt, future0);
                    return;
                }
                // Set result if FIFO message is acknowledged successfully.
                future0.setFuture(Futures.immediateVoidFuture());
                // Log retries.
                if (1 < attempt) {
                    log.info("Finally, ack message successfully, clientId={}, consumerGroup={}, attempt={}, "
                            + "messageId={}, mq={}, endpoints={}, requestId={}", clientId, consumerGroup, attempt,
                        messageId, mq, endpoints, requestId);
                    return;
                }
                log.debug("Ack message successfully, clientId={}, consumerGroup={}, messageId={}, mq={}, "
                    + "endpoints={}, requestId={}", clientId, consumerGroup, messageId, mq, endpoints, requestId);
            }

            @Override
            public void onFailure(Throwable t) {
                // Log failure and retry later.
                log.error("Exception raised while acknowledging message, clientId={}, consumerGroup={}, "
                        + "would attempt to re-ack later, attempt={}, messageId={}, mq={}, endpoints={}", clientId,
                    consumerGroup, attempt, messageId, mq, endpoints, t);
                ackMessageLater(messageView, 1 + attempt, future0);
            }
        }, MoreExecutors.directExecutor());
    }

    private void ackMessageLater(final MessageViewImpl messageView, final int attempt,
        final SettableFuture<Void> future) {
        final MessageId messageId = messageView.getMessageId();
        final ScheduledExecutorService scheduler = consumer.getScheduler();
        try {
            // 延时 1s ack
            scheduler.schedule(() -> ackMessage(messageView, attempt, future),
                ACK_MESSAGE_FAILURE_BACKOFF_DELAY.toNanos(), TimeUnit.NANOSECONDS);
        } catch (Throwable t) {
            if (scheduler.isShutdown()) {
                return;
            }
            // Should never reach here.
            log.error("[Bug] Failed to schedule message ack request, mq={}, messageId={}, clientId={}",
                mq, messageId, consumer.getClientId());
            ackMessageLater(messageView, 1 + attempt, future);
        }
    }

    @Override
    public long getCachedMessageCount() {
        cachedMessageLock.readLock().lock();
        try {
            return cachedMessages.size();
        } finally {
            cachedMessageLock.readLock().unlock();
        }
    }

    @Override
    public long getCachedMessageBytes() {
        return cachedMessagesBytes.get();
    }

    @ExcludeFromJacocoGeneratedReport
    public void doStats() {
        final long receptionTimes = this.receptionTimes.getAndSet(0);
        final long receivedMessagesQuantity = this.receivedMessagesQuantity.getAndSet(0);
        log.info("Process queue stats: clientId={}, mq={}, receptionTimes={}, receivedMessageQuantity={}, "
            + "cachedMessageCount={}, cachedMessageBytes={}", consumer.getClientId(), mq, receptionTimes,
            receivedMessagesQuantity, this.getCachedMessageCount(), this.getCachedMessageBytes());
    }
}
