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

import apache.rocketmq.v2.FilterType;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.impl.Settings;
import org.apache.rocketmq.client.java.impl.UserAgent;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.retry.CustomizedBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PushSubscriptionSettings extends Settings {
    private static final Logger log = LoggerFactory.getLogger(PushSubscriptionSettings.class);
    // consumer group
    private final Resource group;
    // 消费者组订阅的所有 topic 以及对应的 FilterExpression
    private final Map<String, FilterExpression> subscriptionExpressions;
    // 由 admin 创建消费者时指定
    private volatile Boolean fifo = false;
    // 32(proxy端配置)
    private volatile int receiveBatchSize = 32;
    // 20s(proxy端配置)
    private volatile Duration longPollingTimeout = Duration.ofSeconds(30);

    public PushSubscriptionSettings(String namespace, ClientId clientId, Endpoints endpoints, Resource group,
        Duration requestTimeout, Map<String, FilterExpression> subscriptionExpression) {
        super(namespace, clientId, ClientType.PUSH_CONSUMER, endpoints, requestTimeout);
        // consumer group
        this.group = group;
        // 消费者组订阅的所有 topic 以及对应的 FilterExpression
        this.subscriptionExpressions = subscriptionExpression;
    }

    public boolean isFifo() {
        return fifo;
    }

    public int getReceiveBatchSize() {
        return receiveBatchSize;
    }

    public Duration getLongPollingTimeout() {
        return longPollingTimeout;
    }

    @Override
    public apache.rocketmq.v2.Settings toProtobuf() {
        List<SubscriptionEntry> subscriptionEntries = new ArrayList<>();
        // 订阅关系
        for (Map.Entry<String, FilterExpression> entry : subscriptionExpressions.entrySet()) {
            final FilterExpression filterExpression = entry.getValue();
            apache.rocketmq.v2.Resource topic =
                apache.rocketmq.v2.Resource.newBuilder()
                    .setResourceNamespace(namespace)
                    .setName(entry.getKey())
                    .build();
            final apache.rocketmq.v2.FilterExpression.Builder expressionBuilder =
                apache.rocketmq.v2.FilterExpression.newBuilder().setExpression(filterExpression.getExpression());
            final FilterExpressionType type = filterExpression.getFilterExpressionType();
            switch (type) {
                case TAG:
                    expressionBuilder.setType(FilterType.TAG);
                    break;
                case SQL92:
                    expressionBuilder.setType(FilterType.SQL);
                    break;
                default:
                    log.warn("[Bug] Unrecognized filter type, type={}", type);
            }
            SubscriptionEntry subscriptionEntry =
                SubscriptionEntry.newBuilder().setTopic(topic).setExpression(expressionBuilder.build()).build();
            subscriptionEntries.add(subscriptionEntry);
        }
        Subscription subscription =
            Subscription.newBuilder().setGroup(group.toProtobuf()).addAllSubscriptions(subscriptionEntries).build();
        return apache.rocketmq.v2.Settings.newBuilder().setAccessPoint(accessPoint.toProtobuf())
            .setClientType(clientType.toProtobuf()).setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos()))
            .setSubscription(subscription).setUserAgent(UserAgent.INSTANCE.toProtoBuf()).build();
    }
    // 由 org.apache.rocketmq.client.java.impl.ClientImpl.onSettingsCommand 回调
    // 在 telementry rpc 返回时回调

    // 用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
    // 剩下的订阅配置由本地 setting 配置决定，admin 创建的 SubscriptionGroupConfig 主要用来规定消费行为
    // 具体订阅消费哪些数据是可变的，所以由客户端的 setting 决定，比如订阅那些 topic 都是随时可变的只能由消费者灵活制定
    // admin 在创建消费者组的时候无法判定要订阅哪些 topic, 无法灵活改变，所以这部分订阅配置由消费者指定
    @Override
    public void sync(apache.rocketmq.v2.Settings settings) {
        // 从远程 broker 获取到的 consumerGroup 订阅关系配置（由 admin 创建消费者组的时候在指定 broker 填充）
        // org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager#mergeSubscriptionData(apache.rocketmq.v2.Settings, org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig)
        final apache.rocketmq.v2.Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!apache.rocketmq.v2.Settings.PubSubCase.SUBSCRIPTION.equals(pubSubCase)) {
            log.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
                + "clientType={}", clientId, pubSubCase, clientType);
            return;
        }
        // broker 端存储的 consumerGroup 订阅关系配置，用远程配置中的 isConsumeMessageOrderly，RetryMaxTimes，GroupRetryPolicy 覆盖本地配置
        final Subscription subscription = settings.getSubscription();
        // 由 admin 创建消费者时指定
        this.fifo = subscription.getFifo();
        // 32(proxy端配置)
        this.receiveBatchSize = subscription.getReceiveBatchSize();
        // 20s(proxy端配置)
        this.longPollingTimeout = Duration.ofNanos(Durations.toNanos(subscription.getLongPollingTimeout()));
        final RetryPolicy backoffPolicy = settings.getBackoffPolicy();
        switch (backoffPolicy.getStrategyCase()) {
            case EXPONENTIAL_BACKOFF:
                // 由 admin 创建消费者时指定
                retryPolicy = ExponentialBackoffRetryPolicy.fromProtobuf(backoffPolicy);
                break;
            case CUSTOMIZED_BACKOFF:
                // 由 admin 创建消费者时指定
                retryPolicy = CustomizedBackoffRetryPolicy.fromProtobuf(backoffPolicy);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized backoff policy strategy.");
        }
    }

    @ExcludeFromJacocoGeneratedReport
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .add("group", group)
            .add("subscriptionExpressions", subscriptionExpressions)
            .add("fifo", fifo)
            .add("receiveBatchSize", receiveBatchSize)
            .add("longPollingTimeout", longPollingTimeout)
            .toString();
    }
}
