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
package com.zhang.cache.core.metadata.hotkey;

import com.alibaba.fastjson.JSON;
import com.zhang.cache.core.constant.HotKeyConstants;
import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.event.entity.HotKeyMetadataRefreshEvent;
import com.zhang.cache.core.metadata.hotkey.entity.HotKeyMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class HotKeyMetadataManager {
    @Getter
    private volatile Map<String, HotKeyMetadata> hotKeyMetadata = new HashMap<>();

    @Value("${hot-key.cooling-down-time}")
    private Long coolingDownTime;

    @Autowired
    private EventPublisher eventPublisher;

    private volatile Map<String, LongAdder> currentBucket;
    /**
     * Use a circular array for rolling index update, achieve a sliding window in O(1) space.
     */
    @SuppressWarnings("unchecked")
    private final Map<String, LongAdder>[] hotKeyBuckets = new ConcurrentHashMap[HotKeyConstants.HOT_KEY_WINDOW_SIZE];
    /**
     * Mark the current index of HOT_KEY_BUCKETS array.
     */
    private int currentIndex = 0;
    /**
     * For statistics. Based on the HOT_KEY_BUCKETS array, used for determining whether a key is a hot key.
     */
    private final Map<String, AtomicLong> AGGREGATION_COUNTER = new ConcurrentHashMap<>();
    /**
     * Whether the current bucket list is traversed for the first time.
     * After counting data of the current bucket:
     * If it is the first round traversal, do not delete current bucket data from aggregated data;
     * Otherwise, perform the deletion.
     */
    private boolean initial = true;
    private boolean firstBucketVisited = false;
    private static final AtomicLong EMPTY_COUNTER = new AtomicLong(0);

    @PostConstruct
    private void init() {
        for (int i = 0; i < HotKeyConstants.HOT_KEY_WINDOW_SIZE; i++) {
            hotKeyBuckets[i] = new ConcurrentHashMap<>();
        }
        currentBucket = hotKeyBuckets[0];
    }

    @Value("${hot-key.absolute-qps-threshold}")
    private int absoluteQpsThreshold;

    public boolean isHotKey(String key) {
        HotKeyMetadata metadata = hotKeyMetadata.get(key);
        if (metadata == null) {
            return false;
        }
        return HotKeyStatus.ACTIVE.equals(metadata.getStatus());
    }

    public void recordKey(String key) {
        Map<String, LongAdder> bucket = currentBucket;
        bucket.computeIfAbsent(key, k -> new LongAdder()).increment();
        // update traffic information
        AGGREGATION_COUNTER.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
    }

    public long getHotKeyAverageQps(String key) {
        if (!isHotKey(key)) {
            return 0L;
        }
        long totalQps = 0L;
        for (Map<String, LongAdder> hotKeyBucket : hotKeyBuckets) {
            long qps = hotKeyBucket.getOrDefault(key, new LongAdder()).sum();
            totalQps += qps;
        }
        return totalQps / getEffectWindowSize();
    }

    /**
     * Rotates the sliding window bucket and performs hot key analysis.<br>
     * To avoid global synchronization overhead and potential writer starvation under high QPS scenarios, bucket
     * switching is intentionally implemented without strict locking guarantees. As a result, a small number of
     * requests near the bucket rotation boundary may drift into adjacent buckets.<br>
     * This behavior is an acceptable trade-off for achieving higher throughput and lower contention in approximate
     * hot key statistics.
     */
    public void analyzeLocalHotKey() {
        moveOffset();
        hotKeyMetadata = calculateMetadata();
        eventPublisher.publishEvent(new HotKeyMetadataRefreshEvent());
    }

    private void moveOffset() {
        // update index for the circular array
        currentIndex = (currentIndex + 1) % HotKeyConstants.HOT_KEY_WINDOW_SIZE;
        updateVisitInformation(currentIndex);

        Map<String, LongAdder> expiredBucket = hotKeyBuckets[currentIndex];
        /* Replace the current bucket with a new map, and analyze the old expired bucket.
         * A small amount of request drift between adjacent buckets is tolerated
         * to avoid global synchronization overhead and writer starvation.
         */
        currentBucket = new ConcurrentHashMap<>();
        hotKeyBuckets[currentIndex] = currentBucket;

        if (!initial) {
            for (Map.Entry<String, LongAdder> bucketInfo : expiredBucket.entrySet()) {
                String key = bucketInfo.getKey();
                long traffic = bucketInfo.getValue().sum();

                AGGREGATION_COUNTER.compute(key, (k, counter) -> {
                    if (counter == null) {
                        return null;
                    }
                    long remaining = counter.addAndGet(-traffic);
                    // clean up cold key
                    return remaining <= 0 ? null : counter;
                });
            }
        }
    }

    private Map<String, HotKeyMetadata> calculateMetadata() {
        long now = System.currentTimeMillis();
        Map<String, HotKeyMetadata> result = new HashMap<>();
        /* AGGREGATION_COUNTER saves all data in the past several seconds(depends on configuration), but it missed
         * keys which are not visited in the past several seconds. Because INACTIVE keys also need to be calculated,
         * so the collection to calculate the whole data should be:
         * AGGREGATION_COUNTER + hotKeyMetadata.
         */
        Set<String> needComputeKeys = new HashSet<>();
        needComputeKeys.addAll(AGGREGATION_COUNTER.keySet());
        needComputeKeys.addAll(hotKeyMetadata.keySet());

        for (String key : needComputeKeys) {
            // avoid creating empty objects
            long count = AGGREGATION_COUNTER.getOrDefault(key, EMPTY_COUNTER).longValue();
            if (achieveHotKeyThreshold(count)) {
                // 1. If the current key is a hot key, update timestamp to now
                result.put(key, buildHotKeyMetadata(key, HotKeyStatus.ACTIVE, now));
            } else {
                // 2. If the current key is not a hot key now, there are three cases.
                if (!hotKeyMetadata.containsKey(key)) {
                    // 2.1. This key has not been a hot key yet, skip.
                    continue;
                }
                HotKeyMetadata metadata = hotKeyMetadata.get(key);
                long lastOperationTimestamp = metadata.getLastOperationTimestamp();
                boolean exceedCoolingDuration = now - lastOperationTimestamp >= coolingDownTime;

                if (HotKeyStatus.ACTIVE.equals(metadata.getStatus())) {
                    // 2.2. This key was a hot key last second, but now it is not. Update timestamp to now.
                    result.put(key, buildHotKeyMetadata(key, HotKeyStatus.COOLING_DOWN, now));
                    log.info("Hot key is cooling down: {}", key);
                } else {
                    if (HotKeyStatus.COOLING_DOWN.equals(metadata.getStatus())) {
                        // 2.3. This key is already a cooling-down key.
                        if (exceedCoolingDuration) {
                            // 2.3.1. This key has not been a hot key for longer than cooling-down time, marking it as INVALID.
                            result.put(key, buildHotKeyMetadata(key, HotKeyStatus.INVALID, now));
                            log.info("Key has cooled down: {}", key);
                        } else {
                            // 2.3.2 This key is still cooling down
                            result.put(key, metadata);
                        }
                    } else {
                        // 2.4. This key has been an invalid key for a while, delete it from the map, avoiding memory leak.
                        if (exceedCoolingDuration) {
                            result.remove(key);
                        }
                    }
                }
            }
        }

        log.info("Hot key analysis finished. Hot keys distribution: {}", JSON.toJSONString(result));

        return result;
    }

    private void updateVisitInformation(int index) {
        if (index == 0 && firstBucketVisited) {
            // If index equals 0 and first bucket has been visited, this is not the first round of iteration.
            initial = false;
        }
        firstBucketVisited = true;
    }

    private boolean achieveHotKeyThreshold(long currentKeyTrafficCount) {
        int windowSize = getEffectWindowSize();
        int absoluteThreshold = absoluteQpsThreshold * windowSize;

        return currentKeyTrafficCount >= absoluteThreshold;
    }

    private int getEffectWindowSize() {
        return initial ? Math.max(currentIndex, 1) : HotKeyConstants.HOT_KEY_WINDOW_SIZE;
    }

    private HotKeyMetadata buildHotKeyMetadata(String key, HotKeyStatus status, long timestamp) {
        HotKeyMetadata hotKeyMetadata = new HotKeyMetadata();
        hotKeyMetadata.setKey(key);
        hotKeyMetadata.setStatus(status);
        hotKeyMetadata.setLastOperationTimestamp(timestamp);
        return hotKeyMetadata;
    }
}
