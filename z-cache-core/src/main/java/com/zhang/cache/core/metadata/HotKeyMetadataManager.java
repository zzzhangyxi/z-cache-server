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
package com.zhang.cache.core.metadata;

import com.zhang.cache.core.event.EventPublisher;
import com.zhang.cache.core.event.entity.HotKeyMetadataRefreshEvent;
import com.zhang.cache.core.metadata.hotkey.HotKeyMetadata;
import com.zhang.cache.core.metadata.hotkey.HotKeyStatus;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zzzhangyxi
 * @since 2026/5/16
 */
@Component
@Slf4j
public class HotKeyMetadataManager {
    @Getter
    private volatile Map<String, HotKeyMetadata> hotKeyMetadataMap = new HashMap<>();
    private static final Map<String, Long> COOLING_DOWN_HOT_KEYS = new ConcurrentHashMap<>();

    @Value("${hot-key.cooling-down-time}")
    private Long coolingDownTime;

    @Autowired
    private EventPublisher eventPublisher;

    public void updateLocalHotKeyMetadata(Set<String> hotKeys) {
        Set<String> lastHotKeys = new HashSet<>(hotKeyMetadataMap.keySet());
        Set<String> newHotKeys = new HashSet<>(hotKeys);
        newHotKeys.removeAll(lastHotKeys);
        Set<String> coolingDownKeys = new HashSet<>(lastHotKeys);
        coolingDownKeys.removeAll(hotKeys);

        long now = System.currentTimeMillis();
        Map<String, HotKeyMetadata> newHotKeyMetadata = new HashMap<>(hotKeys.size());
        for (String latestHotKey : hotKeys) {
            if (newHotKeys.contains(latestHotKey)) {
                HotKeyMetadata hotKeyMetadata = HotKeyMetadata.builder()
                        .key(latestHotKey)
                        .status(HotKeyStatus.DETECTED)
                        .lastOperationTimestamp(now)
                        .build();
                newHotKeyMetadata.put(latestHotKey, hotKeyMetadata);
                log.info("Hot key detected: {}", latestHotKey);
            } else {
                HotKeyMetadata hotKeyMetadata = hotKeyMetadataMap.get(latestHotKey);
                if (hotKeyMetadata != null) {
                    // must not be null
                    HotKeyMetadata updatedHotKeyMetadata = HotKeyMetadata.builder()
                            .key(hotKeyMetadata.getKey())
                            .status(hotKeyMetadata.getStatus())
                            .lastOperationTimestamp(now)
                            .build();
                    newHotKeyMetadata.put(latestHotKey, updatedHotKeyMetadata);
                }
            }
        }

        hotKeyMetadataMap = newHotKeyMetadata;

        // TODO 这部分逻辑删掉，放到定时任务里面统一管理
        Map<String, HotKeyMetadata> needToUpdateHotKeys = new HashMap<>(hotKeyMetadataMap);
        for (String coolingDownKey : coolingDownKeys) {
            HotKeyMetadata hotKeyMetadata = HotKeyMetadata.builder()
                    .key(coolingDownKey)
                    .status(HotKeyStatus.COOLING_DOWN)
                    .lastOperationTimestamp(now)
                    .build();
            needToUpdateHotKeys.put(coolingDownKey, hotKeyMetadata);
            COOLING_DOWN_HOT_KEYS.put(coolingDownKey, now);
            log.info("Hot key is cooling down: {}", coolingDownKey);
        }

        eventPublisher.publishEvent(new HotKeyMetadataRefreshEvent(needToUpdateHotKeys));
    }

    @Scheduled(fixedRate = 1000)
    public void updateLocalHotKeyMetadata() {
        // TODO 读远程Metadata并合并更新本地

        Map<String, HotKeyMetadata> expiredCoolDownHotKeys = new HashMap<>();

        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> coolingDownKey : COOLING_DOWN_HOT_KEYS.entrySet()) {
            String key = coolingDownKey.getKey();
            Long lastDetectTimestamp = coolingDownKey.getValue();
            if (now - lastDetectTimestamp > coolingDownTime) {
                HotKeyMetadata expiredCoolDownKeyMetadata = HotKeyMetadata.builder()
                        .key(key)
                        .status(HotKeyStatus.INVALID)
                        .lastOperationTimestamp(now)
                        .build();
                expiredCoolDownHotKeys.put(key, expiredCoolDownKeyMetadata);
                COOLING_DOWN_HOT_KEYS.remove(key);
                log.info("Key has cooled down: {}", key);
                // TODO 删除副本
            }
        }
        eventPublisher.publishEvent(new HotKeyMetadataRefreshEvent(expiredCoolDownHotKeys));
    }

    // TODO 数据复制逻辑 & COOLING_DOWN清除逻辑
}
