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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;
import org.apache.rocketmq.client.apis.producer.TransactionChecker;
import org.apache.rocketmq.client.java.message.MessageBuilderImpl;

/**
 * Implementation of {@link ProducerBuilder}
 */
public class ProducerBuilderImpl implements ProducerBuilder {
    private ClientConfiguration clientConfiguration = null;
    private final Set<String> topics = new HashSet<>();
    private int maxAttempts = 3;
    private TransactionChecker checker = null;

    public ProducerBuilderImpl() {
    }

    /**
     * @see ProducerBuilder#setClientConfiguration(ClientConfiguration)
     */
    @Override
    public ProducerBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = checkNotNull(clientConfiguration, "clientConfiguration should not be null");
        return this;
    }

    /**
     * @see ProducerBuilder#setTopics(String...)
     */
    @Override
    public ProducerBuilder setTopics(String... topics) {
        // peek 通常用于在不影响最终结果的情况下观察或调试流的处理过程,不改变元素本身，所以只接受一个 Consumer action
        // peek 是一个惰性中间操作，只有在流的终端操作被触发时才会真正执行。并且，如果终端操作只关心部分元素（如 findFirst、limit），peek 可能只会对部分元素执行
        // 例如：count() 可能直接获取流的大小而不遍历元素，因此 peek 可能不被执行。永远不要依赖 peek 执行必要的业务逻辑，仅用于调试。
        // peek 的存在主要是为了支持调试，在流管道执行过程中查看元素。但在诸如 count()、forEach() 等优化下，peek 可能不会被调用
        final Set<String> set = Arrays.stream(topics).peek(topic -> checkNotNull(topic, "topic should not be null"))
            .peek(topic -> checkArgument(MessageBuilderImpl.TOPIC_PATTERN.matcher(topic).matches(), "topic does not "
                + "match the regex [regex=%s]", MessageBuilderImpl.TOPIC_PATTERN.pattern()))
            .collect(Collectors.toSet());
        this.topics.addAll(set);
        return this;
    }

    /**
     * @see ProducerBuilder#setMaxAttempts(int)
     */
    @Override
    public ProducerBuilder setMaxAttempts(int maxAttempts) {
        checkArgument(maxAttempts > 0, "maxAttempts should be positive");
        this.maxAttempts = maxAttempts;
        return this;
    }

    /**
     * @see ProducerBuilder#setTransactionChecker(TransactionChecker)
     */
    @Override
    public ProducerBuilder setTransactionChecker(TransactionChecker checker) {
        this.checker = checkNotNull(checker, "checker should not set null");
        return this;
    }

    /**
     * @see ProducerBuilder#build()
     */
    @Override
    public Producer build() {
        checkNotNull(clientConfiguration, "clientConfiguration has not been set yet");
        final ProducerImpl producer = new ProducerImpl(clientConfiguration, topics, maxAttempts, checker);
        // 回调 org.apache.rocketmq.client.java.impl.ClientImpl.startUp
        // 启动 clientManager ， 预取 topic 路由
        producer.startAsync().awaitRunning();
        return producer;
    }
}
