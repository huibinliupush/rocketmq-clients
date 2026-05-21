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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.MessageListener;
import org.apache.rocketmq.client.java.hook.MessageInterceptor;
import org.apache.rocketmq.client.java.message.MessageViewImpl;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("NullableProblems")
public class StandardConsumeService extends ConsumeService {
    private static final Logger log = LoggerFactory.getLogger(StandardConsumeService.class);

    public StandardConsumeService(ClientId clientId, MessageListener messageListener,
        ThreadPoolExecutor consumptionExecutor, MessageInterceptor messageInterceptor,
        ScheduledExecutorService scheduler) {
        super(clientId, messageListener, consumptionExecutor, messageInterceptor, scheduler);
    }
    // asyncWorker 执行
    // 消息提交到 consumptionExecutor（20线程）中消费,回调客户端指定的 messageListener
    // 消费成功则 ackMessage,消费失败则 nackMessage,从 cache 中剔除 message
    @Override
    public void consume(ProcessQueue pq, List<MessageViewImpl> messageViews) {
        for (MessageViewImpl messageView : messageViews) {
            // Discard corrupted message.
            if (messageView.isCorrupted()) { // CRC32 校验失败
                log.error("Message is corrupted for standard consumption, prepare to discard it, mq={}, "
                    + "messageId={}, clientId={}", pq.getMessageQueue(), messageView.getMessageId(), clientId);
                // changeInvisibleDuration
                pq.discardMessage(messageView);
                continue;
            }
            // 消息提交到 consumptionExecutor（20线程）中消费
            // 回调客户端指定的 messageListener
            final ListenableFuture<ConsumeResult> future = consume(messageView);
            Futures.addCallback(future, new FutureCallback<ConsumeResult>() {
                // consumptionExecutor（20线程）执行
                @Override
                public void onSuccess(ConsumeResult consumeResult) {
                    // 消费成功则 ackMessage
                    // 消费失败则 nackMessage
                    // 从 cache 中剔除 message
                    pq.eraseMessage(messageView, consumeResult);
                }

                @Override
                public void onFailure(Throwable t) {
                    // Should never reach here.
                    log.error("[Bug] Exception raised in consumption callback, clientId={}", clientId, t);
                }
            }, MoreExecutors.directExecutor());
        }
    }
}
