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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("UnstableApiUsage")
class FifoConsumeService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(FifoConsumeService.class);
    // false
    private final boolean enableFifoConsumeAccelerator;

    public FifoConsumeService(ClientId clientId, MessageListener messageListener,
        ThreadPoolExecutor consumptionExecutor, MessageInterceptor messageInterceptor,
        ScheduledExecutorService scheduler, boolean enableFifoConsumeAccelerator) {
        super(clientId, messageListener, consumptionExecutor, messageInterceptor, scheduler);
        this.enableFifoConsumeAccelerator = enableFifoConsumeAccelerator;
    }
    // 在 FIFO 消费场景下，必须等到前一个消息消费成功之后，才能开始下一个消息的消费
    // 如果前一个消息没有消费成功，那么就进行重试，重试成功之后再开始下一个消息消费
    // 如果重试一直失败，达到最大重试次数，则直接发送到死信队列，发送完毕之后再开始下一个消息的消费
    @Override
    public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
        // false
        if (!enableFifoConsumeAccelerator || messageViews.size() <= 1) {
            consumeIteratively(pq, messageViews.iterator());
            return;
        }
        Map<String, List<MessageViewImpl>> messageViewsGroupByMessageGroup = new HashMap<>();
        List<MessageViewImpl> messageViewsWithoutMessageGroup = new ArrayList<>();
        for (MessageViewImpl messageView : messageViews) {
            Optional<String> messageGroup = messageView.getMessageGroup();
            if (messageGroup.isPresent()) {
                messageViewsGroupByMessageGroup.computeIfAbsent(messageGroup.get(), k -> new ArrayList<>())
                    .add(messageView);
            } else {
                messageViewsWithoutMessageGroup.add(messageView);
            }
        }

        log.debug("FifoConsumeService parallel consume, messageViewsNum={}, groupNum={}", messageViews.size(),
            messageViewsGroupByMessageGroup.size() + (messageViewsWithoutMessageGroup.isEmpty() ? 0 : 1));

        messageViewsGroupByMessageGroup.values().forEach(list -> consumeIteratively(pq, list.iterator()));
        consumeIteratively(pq, messageViewsWithoutMessageGroup.iterator());
    }
    // 在 FIFO 消费场景下，必须等到前一个消息消费成功之后，才能开始下一个消息的消费
    // 如果前一个消息没有消费成功，那么就进行重试，重试成功之后再开始下一个消息消费
    // 如果重试一直失败，达到最大重试次数，则直接发送到死信队列，发送成功之后（不成功则一直重试直到成功）再开始下一个消息的消费
    // 消费成功也是一样，必须等到 ackMessage 成功之后才能消费下一个消息
    public void consumeIteratively(ProcessQueue pq, Iterator<MessageViewImpl> iterator) {
        if (!iterator.hasNext()) {
            return;
        }
        final MessageViewImpl messageView = iterator.next();
        if (messageView.isCorrupted()) {
            // Discard corrupted message.
            log.error("Message is corrupted for FIFO consumption, prepare to discard it, mq={}, messageId={}, "
                + "clientId={}", pq.getMessageQueue(), messageView.getMessageId(), clientId);
            // 直接发送到死信队列，从缓存中剔除
            pq.discardFifoMessage(messageView);
            // 继续往后消费 FIFO 消息
            consumeIteratively(pq, iterator);
            return;
        }
        // 按照顺序消费第一个 FIFO 消息
        final ListenableFuture<ConsumeResult> future0 = consume(messageView);
        // 处理消费结果
        ListenableFuture<Void> future = Futures.transformAsync(future0, result -> pq.eraseFifoMessage(messageView,
            result), MoreExecutors.directExecutor());
        // 第一个消费完之后，在接着消费下一个 FIFO 消息
        // 在 FIFO 消费场景下，必须等到前一个消息消费成功之后，才能开始下一个消息的消费
        // 如果前一个消息没有消费成功，那么就进行重试，重试成功之后再开始下一个消息消费
        // 如果重试一直失败，达到最大重试次数，则直接发送到死信队列，发送成功之后（不成功则一直重试直到成功）再开始下一个消息的消费
        // 消费成功也是一样，必须等到 ackMessage 成功之后才能消费下一个消息
        future.addListener(() -> consumeIteratively(pq, iterator), MoreExecutors.directExecutor());
    }
}
