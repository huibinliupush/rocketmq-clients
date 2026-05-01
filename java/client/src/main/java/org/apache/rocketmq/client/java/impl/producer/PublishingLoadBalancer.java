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

package org.apache.rocketmq.client.java.impl.producer;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.RandomUtils;
import org.apache.rocketmq.client.java.misc.Utilities;
import org.apache.rocketmq.client.java.route.Broker;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.MessageQueueImpl;
import org.apache.rocketmq.client.java.route.TopicRouteData;

@Immutable
public class PublishingLoadBalancer {
    /**
     * Index for round-robin.
     */
    private final AtomicInteger index;
    /**
     * Message queues to send message.
     * 从 topic 下所有副本集中过滤出 master broker,并且 messageQueue 是 writable
     */
    private final ImmutableList<MessageQueueImpl> messageQueues;

    public PublishingLoadBalancer(TopicRouteData topicRouteData) {
        this(new AtomicInteger(RandomUtils.nextInt(0, Integer.MAX_VALUE)), topicRouteData);
    }

    private PublishingLoadBalancer(AtomicInteger index, TopicRouteData topicRouteData) {
        this.index = index;
        // 从 topic 下所有副本集中过滤出 master broker,并且 messageQueue 是 writable
        final List<MessageQueueImpl> mqs = topicRouteData.getMessageQueues().stream()
            .filter((Predicate<MessageQueueImpl>) mq -> mq.getPermission().isWritable() &&
                Utilities.MASTER_BROKER_ID == mq.getBroker().getId())
            .collect(Collectors.toList());
        if (mqs.isEmpty()) {
            throw new IllegalArgumentException("No writable message queue found, topiRouteData=" + topicRouteData);
        }
        this.messageQueues = ImmutableList.<MessageQueueImpl>builder().addAll(mqs).build();
    }

    PublishingLoadBalancer update(TopicRouteData topicRouteData) {
        return new PublishingLoadBalancer(index, topicRouteData);
    }
/**
 *
 * 相同的 messageGroup 字符串总是被分配到相同的队列（因为哈希值不变，取模结果也不变）。
 *
 * 不同的 messageGroup 会以近似均匀的概率分布到各个队列上，从而实现负载的平衡。
 *
 * 使用 SipHash24 而不是普通哈希（如 String.hashCode()）可以有效防止攻击者刻意构造大量 messageGroup 值，使它们全部落到同一个队列，导致某个队列过载（哈希洪水攻击）。
 *
 * 如果后续需要增加或减少队列数量，取模结果会大面积改变，导致同一个 messageGroup 被路由到不同的队列。如果需要支持动态扩缩容，应考虑一致性哈希。
 * */
    public MessageQueueImpl takeMessageQueueByMessageGroup(String messageGroup) {
        // 稳定的、均匀分布的哈希值,特别适合对短输入（如字符串键）进行哈希，并能够有效防御“哈希洪水攻击
        // 相同的 messageGroup 输入，每次运行都会得到完全相同的 hashCode（前提是不改变默认密钥）。
        final long hashCode = Hashing.sipHash24().hashBytes(messageGroup.getBytes(StandardCharsets.UTF_8)).asLong();
        // 之所以不直接用 % 运算符，是因为 Java 的 % 对于负数会返回负余数（例如 -5 % 3 = -2），而 LongMath.mod 会将负数调整到 [0, m-1] 区间内，保证索引一定是有效的
        final int index = LongMath.mod(hashCode, messageQueues.size());
        return messageQueues.get(index);
    }

    public List<MessageQueueImpl> takeMessageQueues(Set<Endpoints> excluded, int count) {
        int next = index.getAndIncrement();
        List<MessageQueueImpl> candidates = new ArrayList<>();
        Set<String> candidateBrokerNames = new HashSet<>();
        // 从 topic 下所有副本集中过滤出 master broker,并且 messageQueue 是 writable
        for (int i = 0; i < messageQueues.size(); i++) {
            final MessageQueueImpl messageQueueImpl = messageQueues.get(IntMath.mod(next++, messageQueues.size()));
            final Broker broker = messageQueueImpl.getBroker();
            final String brokerName = broker.getName();
            if (!excluded.contains(broker.getEndpoints()) && !candidateBrokerNames.contains(brokerName)) {
                candidateBrokerNames.add(brokerName);
                candidates.add(messageQueueImpl);
            }
            // count 为重试次数，选取 count 个 candidates
            if (candidates.size() >= count) {
                return candidates;
            }
        }
        // If all endpoints are isolated.
        if (candidates.isEmpty()) {
            for (int i = 0; i < messageQueues.size(); i++) {
                final MessageQueueImpl messageQueueImpl = messageQueues.get(IntMath.mod(next++, messageQueues.size()));
                final Broker broker = messageQueueImpl.getBroker();
                final String brokerName = broker.getName();
                if (!candidateBrokerNames.contains(brokerName)) {
                    candidateBrokerNames.add(brokerName);
                    candidates.add(messageQueueImpl);
                }
                if (candidates.size() >= count) {
                    break;
                }
            }
        }
        return candidates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PublishingLoadBalancer that = (PublishingLoadBalancer) o;
        return Objects.equal(messageQueues, that.messageQueues);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(messageQueues);
    }
}
