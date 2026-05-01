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

package org.apache.rocketmq.client.java.retry;

import static apache.rocketmq.v2.RetryPolicy.StrategyCase.EXPONENTIAL_BACKOFF;
import static com.google.common.base.Preconditions.checkArgument;

import apache.rocketmq.v2.ExponentialBackoff;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Durations;
import java.time.Duration;

/**
 * The {@link ExponentialBackoffRetryPolicy} defines a policy to do more attempts when failure is encountered.
 * retry backoff strategy
 * 除服务端返回系统流控错误场景，其他触发条件触发重试后，均会立即进行重试，无等待间隔。
 *
 * 若由于服务端返回流控错误触发重试，系统会按照指数退避策略进行延迟重试
 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
    // 最大重试次数
    private final int maxAttempts;
    // 第一次失败重试前后需等待多久，默认值：1秒
    private final Duration initialBackoff;
    // 等待间隔时间上限，默认值：120秒
    private final Duration maxBackoff;
    // 指数退避因子，即退避倍率，默认值：1.6
    private final double backoffMultiplier;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exception or
     * logging warnings already, so we avoid repeating args check here.
     *
     * see : https://rocketmq.apache.org/zh/docs/featureBehavior/05sendretrypolicy
     * 除服务端返回系统流控错误场景，其他触发条件触发重试后，均会立即进行重试，无等待间隔。
     *
     * 若由于服务端返回流控错误触发重试，系统会按照指数退避策略进行延迟重试
     */
    public ExponentialBackoffRetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff,
        double backoffMultiplier) {
        // 3
        this.maxAttempts = maxAttempts;
        // 0
        this.initialBackoff = initialBackoff;
        // 0
        this.maxBackoff = maxBackoff;
        // 1
        this.backoffMultiplier = backoffMultiplier;
    }

    public static ExponentialBackoffRetryPolicy immediatelyRetryPolicy(int maxAttempts) {
        return new ExponentialBackoffRetryPolicy(maxAttempts, Duration.ZERO, Duration.ZERO, 1);
    }

    @Override
    public int getMaxAttempts() {
        return maxAttempts;
    }

    Duration getInitialBackoff() {
        return initialBackoff;
    }

    Duration getMaxBackoff() {
        return maxBackoff;
    }

    double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    @Override
    public Duration getNextAttemptDelay(int attempt) {
        checkArgument(attempt > 0, "attempt must be positive");
        // Math.pow(a,b) : 返回 a 的 b 次幂
        // Math.pow(2, 3) = 8 ,Math.pow(5, 2)=25
        // initialBackoff * backoffMultiplier^(attempt - 1) 但不能超过 maxBackoff
        double delayNanos = Math.min(initialBackoff.toNanos() * Math.pow(backoffMultiplier,
            1.0 * (attempt - 1)), maxBackoff.toNanos());
        if (delayNanos <= 0) {
            return Duration.ZERO;
        }
        return Duration.ofNanos((long) delayNanos);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("maxAttempts", maxAttempts)
            .add("initialBackoff", initialBackoff)
            .add("maxBackoff", maxBackoff)
            .add("backoffMultiplier", backoffMultiplier)
            .toString();
    }

    public static ExponentialBackoffRetryPolicy fromProtobuf(apache.rocketmq.v2.RetryPolicy retryPolicy) {
        if (!EXPONENTIAL_BACKOFF.equals(retryPolicy.getStrategyCase())) {
            throw new IllegalArgumentException();
        }
        final ExponentialBackoff exponentialBackoff = retryPolicy.getExponentialBackoff();
        return new ExponentialBackoffRetryPolicy(retryPolicy.getMaxAttempts(),
            Duration.ofNanos(Durations.toNanos(exponentialBackoff.getInitial())),
            Duration.ofNanos(Durations.toNanos(exponentialBackoff.getMax())),
            exponentialBackoff.getMultiplier());
    }

    @Override
    public RetryPolicy inheritBackoff(apache.rocketmq.v2.RetryPolicy retryPolicy) {
        if (!EXPONENTIAL_BACKOFF.equals(retryPolicy.getStrategyCase())) {
            throw new IllegalArgumentException("strategy must be exponential backoff");
        }
        return inheritBackoff(retryPolicy.getExponentialBackoff());
    }

    private RetryPolicy inheritBackoff(ExponentialBackoff backoff) {
        return new ExponentialBackoffRetryPolicy(maxAttempts,
            Duration.ofNanos(Durations.toNanos(backoff.getInitial())),
            Duration.ofNanos(Durations.toNanos(backoff.getMax())),
            backoff.getMultiplier());
    }

    @Override
    public apache.rocketmq.v2.RetryPolicy toProtobuf() {
        ExponentialBackoff exponentialBackoff = ExponentialBackoff.newBuilder()
            .setMultiplier((float) backoffMultiplier)
            .setMax(Durations.fromNanos(maxBackoff.toNanos()))
            .setInitial(Durations.fromNanos(initialBackoff.toNanos()))
            .build();
        return apache.rocketmq.v2.RetryPolicy.newBuilder()
            .setMaxAttempts(maxAttempts)
            .setExponentialBackoff(exponentialBackoff)
            .build();
    }
}
