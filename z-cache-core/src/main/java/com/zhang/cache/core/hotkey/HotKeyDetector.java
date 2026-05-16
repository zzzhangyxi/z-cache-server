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
package com.zhang.cache.core.hotkey;

import com.zhang.cache.core.constant.HotKeyConstants;
import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.event.entity.HotKeyDetectionEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author zzzhangyxi
 * @since 2026/5/15
 */
@Component
@Slf4j
public class HotKeyDetector {
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
     * Calculate total traffic
     */
    private final LongAdder TOTAL_TRAFFIC = new LongAdder();
    /**
     * For statistics. Based on the HOT_KEY_BUCKETS array, used for determining whether a key is a hot key.
     */
    private final Map<String, AtomicLong> AGGREGATION_COUNTER = new ConcurrentHashMap<>();
    /**
     * Hot key collection after calculating.
     */
    private volatile Set<String> hotKeys = Collections.emptySet();
    /**
     * Whether the current bucket list is traversed for the first time.
     * After counting data of the current bucket:
     * If it is the first round traversal, do not delete current bucket data from aggregated data;
     * Otherwise, perform the deletion.
     */
    private boolean initial = true;
    private boolean firstBucketVisited = false;

    @PostConstruct
    private void init() {
        for (int i = 0; i < HotKeyConstants.HOT_KEY_WINDOW_SIZE; i++) {
            hotKeyBuckets[i] = new ConcurrentHashMap<>();
        }
        currentBucket = hotKeyBuckets[0];
    }

    @Autowired
    private EventPublisher eventPublisher;

    @Value("${hot-key.absolute-qps-threshold}")
    private int absoluteQpsThreshold;
    @Value("${hot-key.relative-qps-threshold}")
    private int relativeQpsThreshold;
    @Value("${hot-key.relative-qps-ratio}")
    private double relativeQpsRatio;

    public void recordKey(String key) {
        Map<String, LongAdder> bucket = currentBucket;
        bucket.computeIfAbsent(key, k -> new LongAdder()).increment();
        // update traffic information
        AGGREGATION_COUNTER.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
        TOTAL_TRAFFIC.increment();
    }

    public boolean isHotKey(String key) {
        return hotKeys.contains(key);
    }

    /**
     * Rotates the sliding window bucket and performs hot key analysis.<br>
     * To avoid global synchronization overhead and potential writer starvation under high QPS scenarios, bucket
     * switching is intentionally implemented without strict locking guarantees. As a result, a small number of
     * requests near the bucket rotation boundary may drift into adjacent buckets.<br>
     * This behavior is an acceptable trade-off for achieving higher throughput and lower contention in approximate
     * hot key statistics.
     */
    @Scheduled(fixedRate = 1000)
    public void analyzeHotKey() {
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
                TOTAL_TRAFFIC.add(-traffic);
            }
        }

        Set<String> latestHotKeys = new HashSet<>();
        long totalTraffic = Math.max(TOTAL_TRAFFIC.sum(), 0L);
        for (Map.Entry<String, AtomicLong> aggregationBucketInfo : AGGREGATION_COUNTER.entrySet()) {
            String key = aggregationBucketInfo.getKey();
            long count = aggregationBucketInfo.getValue().get();
            // determine hot key
            if (achieveHotKeyThreshold(count, totalTraffic)) {
                latestHotKeys.add(key);
            }
        }

        hotKeys = latestHotKeys;
        eventPublisher.publishEvent(new HotKeyDetectionEvent(hotKeys));

        log.debug("Hot key analysis finished. Current hot key count: {}", hotKeys.size());
    }

    private void updateVisitInformation(int index) {
        if (index == 0 && firstBucketVisited) {
            // If index equals 0 and first bucket has been visited, this is not the first round of iteration.
            initial = false;
        }
        firstBucketVisited = true;
    }

    private boolean achieveHotKeyThreshold(long currentKeyTrafficCount, long totalTrafficCount) {
        int windowSize = initial ? Math.max(currentIndex, 1) : HotKeyConstants.HOT_KEY_WINDOW_SIZE;
        int absoluteThreshold = absoluteQpsThreshold * windowSize;
        int relativeThreshold = relativeQpsThreshold * windowSize;

        if (currentKeyTrafficCount >= absoluteThreshold) {
            return true;
        }
        if (currentKeyTrafficCount >= relativeThreshold) {
            if (totalTrafficCount > 0) {
                double ratio = (double) currentKeyTrafficCount / (double) totalTrafficCount;
                return ratio >= relativeQpsRatio;
            }
        }
        return false;
    }
}
